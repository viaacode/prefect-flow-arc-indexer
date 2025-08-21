from elasticsearch.helpers import streaming_bulk
from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.utilities.annotations import quote
from prefect.testing.utilities import prefect_test_harness
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_meemoo.elasticsearch.credentials import ElasticsearchCredentials
from prefect_sqlalchemy.credentials import DatabaseCredentials
from functools import partial
from datetime import datetime
import os
import json
from psycopg2 import sql, connect

os.environ.update(PREFECT_LOGGING_LEVEL="DEBUG")


# Helper function to create a connection to database
def get_postgres_connection(postgres_credentials: DatabaseCredentials):
    """
    Function to get a postgres connection.
    """
    logger = get_run_logger()
    logger.info("(Re)connecting to postgres")
    db_conn = connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
    )
    return db_conn


# Task to get the indexes from database
@task
def get_indexes_list(
    db_credentials: DatabaseCredentials,
    db_table: str,
    db_column_es_index: str = "index",
):
    logger = get_run_logger()
    # Connect to Postgres
    db_conn = get_postgres_connection(db_credentials)

    # Create cursor
    cursor = db_conn.cursor()

    # Run query
    query = sql.SQL(
        "SELECT DISTINCT {db_column_es_index} FROM {db_table} WHERE {db_column_es_index} is not null;"
    ).format(
        db_column_es_index=sql.Identifier(db_column_es_index),
        db_table=sql.Identifier(*db_table.split(".")),
    )
    logger.debug(query.as_string(db_conn))
    cursor.execute(query)
    # unpack result
    indexes = [row[0] for row in cursor.fetchall()]
    logger.info(
        "Retrieved the following Elasticsearch indexes from database: %s.", indexes
    )

    return indexes


# Task to get the indexes ordered by the number of documents
@task
def get_index_order(
    indexes: list[str],
    db_credentials: DatabaseCredentials,
    db_table: str,
    db_column_es_index: str = "index",
):
    logger = get_run_logger()
    # Connect to Postgres
    db_conn = get_postgres_connection(db_credentials)

    # Create cursor
    cursor = db_conn.cursor()

    # Run query
    query = sql.SQL(
        """
        SELECT {db_column_es_index}, count(id)
        FROM {db_table} 
        WHERE {db_column_es_index} IN %(indexes_list)s
        GROUP BY {db_column_es_index} 
        ORDER BY count(id) ASC;
        """
    ).format(
        db_column_es_index=sql.Identifier(db_column_es_index),
        db_table=sql.Identifier(*db_table.split(".")),
    )
    logger.debug(query.as_string(db_conn))
    cursor.execute(query, {"indexes_list": tuple(indexes)})
    logger.info("Retrieving order for Elasticsearch indexes %s from database.", indexes)

    return cursor.fetchall()


# Task to create a list of indexes
@task
def create_indexes(
    indexes: list[str],
    es_credentials: ElasticsearchCredentials,
    timestamp: str,
):
    logger = get_run_logger()
    es = es_credentials.get_client()
    for index in indexes:
        index_name = f"{index}_{timestamp}"

        # create the index and disable the refresh for load
        result = es.indices.create(
            index=index_name,
            settings={"refresh_interval": "-1", "number_of_replicas": 0},
        )
        logger.info(f"Created of Elasticsearch index {result}.")

    return logger.info("Creation of Elasticsearch indexes completed.")


# Hook to delete all created indexes when stream task fails
def delete_indexes(
    task,
    task_run,
    state,
    indexes: list[str],
    es_credentials: ElasticsearchCredentials,
    timestamp: str,
):
    logger = get_run_logger()
    es = es_credentials.get_client()
    for index in indexes:
        index_name = f"{index}_{timestamp}"
        result = es.indices.delete(index=index_name)
        logger.info("Deletion of Elasticsearch index %s: %s.", index_name, result)

    return logger.info("Cleanup of Elasticsearch indexes completed.")


# Task to get records from PostgreSQL using a cursor and stream to Elasticsearch
@task(
    retries=1,
    retry_delay_seconds=30,
)
def stream_records_to_es(
    indexes: list[str],
    es_credentials: ElasticsearchCredentials,
    db_credentials: DatabaseCredentials,
    db_table: str,
    db_column_es_id: str = "id",
    db_column_es_index: str = "index",
    db_batch_size: int = 1000,
    es_chunk_size: int = 500,
    es_request_timeout: int = 30,
    es_max_retries: int = 10,
    es_retry_on_timeout: bool = True,
    timestamp: str = None,  # timestamp to uniquely identify indexes
    last_modified: DateTime = None,
    record_count: int = None,
):
    logger = get_run_logger()

    # Connect to ES and Postgres
    db_conn = get_postgres_connection(db_credentials)
    es = es_credentials.get_client().options(
        request_timeout=es_request_timeout,
        max_retries=es_max_retries,
        retry_on_timeout=es_retry_on_timeout,
    )

    # Create server-side cursor
    cursor = db_conn.cursor(name="large_query_cursor")
    cursor.itersize = db_batch_size

    # Compose and run query
    if last_modified:
        sql_query = sql.SQL(
            """
            SELECT {db_column_es_index}, {db_column_es_id}, document, is_deleted
            FROM {db_table}
            WHERE {db_column_es_index} IN %(indexes_list)s
            AND updated_at >= %(last_modified)s
            """
        ).format(
            db_column_es_index=sql.Identifier(db_column_es_index),
            db_table=sql.Identifier(*db_table.split(".")),
            db_column_es_id=sql.Identifier(db_column_es_id),
        )
        logger.info("Creating cursor from query %s.", sql_query.as_string(db_conn))
        cursor.execute(
            sql_query,
            {"indexes_list": tuple(indexes), "last_modified": str(last_modified)},
        )
    else:
        sql_query = sql.SQL(
            """
            SELECT {db_column_es_index}, {db_column_es_id}, document, is_deleted
            FROM {db_table}
            WHERE {db_column_es_index} IN %(indexes_list)s 
            AND NOT is_deleted
            """
        ).format(
            db_column_es_index=sql.Identifier(db_column_es_index),
            db_table=sql.Identifier(*db_table.split(".")),
            db_column_es_id=sql.Identifier(db_column_es_id),
        )
        logger.info("Creating cursor from query %s.", sql_query.as_string(db_conn))
        cursor.execute(sql_query, {"indexes_list": tuple(indexes)})

    # Stats
    records = 0
    errors = 0

    n = (
        round(record_count / 10)
        if record_count is not None
        and record_count > 0
        and round(record_count / 10) > 0
        else 50
    )

    # Fill new indexes
    def generate_actions():
        for index, id, document, is_deleted in cursor:
            index_name = f"{index}_{timestamp}" if last_modified is None else index
            logger.debug(
                "Attempt indexing %s (charlength: %s)",
                (id if db_column_es_id else None),
                len(
                    json.dumps(
                        document,
                    )
                ),
            )
            yield {
                "_index": index_name,
                "_id": (id if db_column_es_id else None),
                "_source": document if not is_deleted else None,
                "_op_type": "delete" if is_deleted else "index",
            }

    logger.info(
        "Starting indexing stream of %s documents with timestamp %s and last modified %s",
        record_count,
        timestamp,
        last_modified,
    )

    for ok, item in streaming_bulk(
        es,
        generate_actions(),
        chunk_size=es_chunk_size,
        raise_on_error=False,
        raise_on_exception=False,
        max_retries=es_max_retries
    ):
        records += 1
        if not ok:
            errors += 1
            logger.error(item)

        if records % n == 0:
            logger.info(
                "Indexed %s of %s records",
                records,
                record_count if record_count is not None else "?",
            )

    cursor.close()
    db_conn.close()

    return logger.info(
        "Streaming records into Elasticsearch indexes %s completed. %s of %s records failed.",
        indexes,
        errors,
        records,
    )


# Task to delete the indexes that dissapeared since since last full sync
@task
def delete_untouched_indexes(
    indexes: list[str], es_credentials: ElasticsearchCredentials
):
    logger = get_run_logger()
    es = es_credentials.get_client()

    # get a list of all indexes in cluster
    all_indexes = list(es.indices.get_alias(name="*").keys())
    # find all untouched indexes by filtering the touched indexes from the list
    untouched_indexes = [
        index for index in all_indexes if not any(alias in index for alias in indexes)
    ]
    # delete all indexes that won't be touched
    if len(untouched_indexes) > 0:
        untouched_indexes_seq = ",".join(untouched_indexes)
        logger.info("Deleting untouched indexes %s", {untouched_indexes_seq})
        es.indices.delete(index=untouched_indexes_seq)
    else:
        logger.info("No untouched indexes.")


# Task to do changeover from old to new indexes when full sync
@task(retries=1, retry_delay_seconds=30)
def swap_indexes(
    indexes: list[str],
    es_credentials: ElasticsearchCredentials,
    timestamp: str,
    es_timeout: int = 30,
):
    logger = get_run_logger()
    es = es_credentials.get_client()

    for index in indexes:
        # get index connected to alias
        old_indexes = (
            list(es.indices.get_alias(name=index).keys())
            if es.indices.exists_alias(name=index)
            else []
        )
        # switch alias
        alias_name = f"{index}_{timestamp}"
        logger.info("Switching alias %s to new index %s.", index, alias_name)
        es.indices.put_settings(
            index=alias_name,
            settings={"refresh_interval": "30s", "number_of_replicas": 1},
            timeout=f"{es_timeout}s",
        )
        es.indices.put_alias(name=index, index=alias_name, timeout=f"{es_timeout}s")

        # delete old indexes if there are any
        if len(old_indexes) > 0:
            old_indexes_seq = ",".join(old_indexes)
            logger.info("Deleting old indexes %s for alias %s", old_indexes_seq, index)
            es.indices.delete(index=old_indexes_seq)

    return logger.info("Elasticsearch indexes swapped succesfully.")


# Define the Prefect flow
@flow(name="prefect-flow-arc-indexer", on_completion=[save_last_run_config])
def main_flow(
    db_block_name: str,
    es_block_name: str,
    db_table: str = "graph.index_documents",
    db_column_es_id: str = "id",
    db_column_es_index: str = "index",
    or_ids: list[str] = None,
    last_modified: DateTime = None,
    full_sync: bool = False,
    db_batch_size: int = 1000,
    es_chunk_size: int = 500,
    es_request_timeout: int = 30,
    es_max_retries: int = 10,
    es_retry_on_timeout: bool = True,
):
    """
    Flow to index all of the Hasura Postgres records.
    """
    # Load logger
    logger = get_run_logger()

    # Load credentials
    es_credentials = ElasticsearchCredentials.load(es_block_name)
    db_credentials = DatabaseCredentials.load(db_block_name)

    # Init task
    logger.info("Start indexing process (full sync = %s)", full_sync)

    # Get all indexes from database if none provided
    use_all_indexes = or_ids is None or len(or_ids) < 1
    if use_all_indexes:
        or_ids = get_indexes_list.submit(
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_index=db_column_es_index,
        ).result()

    # Get timestamp to uniquely identify indexes
    timestamp = datetime.now().strftime("%Y-%m-%dt%H.%M.%S")

    if full_sync:
        # When there are indexes that are no longer part of the full sync, delete them
        if use_all_indexes:
            delete_untouched_indexes.submit(
                indexes=quote(or_ids),
                es_credentials=es_credentials,
            )

        # Determine order in which to load indexes
        indexes_order = get_index_order.submit(
            indexes=quote(or_ids),
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_index=db_column_es_index,
        )

        indexes = indexes_order.result()
        if len(indexes) != len(or_ids):
            raise ValueError(
                "The number of indexes does not match the number of ordered indexes."
            )

        for i, (index, record_count) in enumerate(indexes):
            t1 = create_indexes.with_options(
                name=f"creating-{index}",
                tags=[
                    "pg-indexer-create",
                ],
            ).submit(
                indexes=quote([index]),
                es_credentials=es_credentials,
                timestamp=timestamp,
                wait_for=indexes_order,
            )
            t2 = stream_records_to_es.with_options(
                name=f"indexing-{index}",
                on_failure=[
                    partial(
                        delete_indexes,
                        indexes=[index],
                        es_credentials=es_credentials,
                        timestamp=timestamp,
                    )
                ],
                tags=[
                    "pg-indexer-large" if i > len(indexes) - 3 else "pg-indexer",
                ],
                retries=3
            ).submit(
                indexes=quote([index]),
                es_credentials=es_credentials,
                db_credentials=db_credentials,
                db_table=db_table,
                db_column_es_id=db_column_es_id,
                db_column_es_index=db_column_es_index,
                db_batch_size=db_batch_size,
                es_chunk_size=es_chunk_size,
                es_request_timeout=es_request_timeout,
                es_max_retries=es_max_retries,
                es_retry_on_timeout=es_retry_on_timeout,
                timestamp=timestamp,
                record_count=record_count,
                wait_for=t1,
            )
            swap_indexes.with_options(
                name=f"swap-index-{index}",
            ).submit(
                indexes=quote([index]),
                es_credentials=es_credentials,
                timestamp=timestamp,
                es_timeout=es_request_timeout,
                wait_for=t2,
            )
    else:
        stream_records_to_es.with_options(
            on_failure=[
                partial(
                    delete_indexes,
                    indexes=or_ids,
                    es_credentials=es_credentials,
                    timestamp=timestamp,
                )
            ]
        ).submit(
            indexes=quote(or_ids),
            es_credentials=es_credentials,
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_id=db_column_es_id,
            db_column_es_index=db_column_es_index,
            db_batch_size=db_batch_size,
            es_chunk_size=es_chunk_size,
            last_modified=last_modified if not full_sync else None,
        )


# Execute the flow
if __name__ == "__main__":
    with prefect_test_harness():
        es = ElasticsearchCredentials(
            url="https://localhost:9200", username="elastic", password="elk-password"
        )
        es.save("elastic-arc")
        db = DatabaseCredentials(
            host="0.0.0.0",
            port=5432,
            username="postgres",
            password="mysecretpassword",
            database="postgres",
            driver="postgresql+asyncpg",
        )
        db.save("hasura-arc")
        main_flow(
            db_block_name="hasura-arc",
            es_block_name="elastic-arc",
            db_table="index",
            es_index="arcv3",
            db_column_es_id="id",
        )
