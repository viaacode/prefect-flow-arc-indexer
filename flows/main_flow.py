from elasticsearch.helpers import streaming_bulk
from elasticsearch.exceptions import ConnectionTimeout
from pendulum.datetime import DateTime
from prefect import flow, get_run_logger, task
from prefect.utilities.annotations import quote
from prefect.testing.utilities import prefect_test_harness
from prefect_meemoo.config.last_run import save_last_run_config
from prefect_meemoo.elasticsearch.credentials import ElasticsearchCredentials
from prefect_sqlalchemy.credentials import DatabaseCredentials
from functools import partial
from datetime import datetime
from time import sleep
import os
import json
from psycopg2 import sql, connect, OperationalError

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


@task
def check_if_org_name_changed(
    index: list[str],
    es_credentials: ElasticsearchCredentials,
    db_credentials: DatabaseCredentials,
    db_table: str,
    db_column_es_index: str = "index",
) -> bool:
    logger = get_run_logger()
    es = es_credentials.get_client()
    db_conn = get_postgres_connection(db_credentials)
    cursor = db_conn.cursor()

    # check if index exists
    if not es.indices.exists(index=index):
        logger.info(f"Elasticsearch index {index} does not exist. Assuming org_name changed.")
        return True

    # get one 'schema_maintainer'->>'schema_name'  from index
    resp = es.search(
        index=index,
        size=1,  # only one result
        _source=["schema_maintainer.schema_name"],  # only fetch this field
        query={"match_all": {}}  # no filtering, just grab any document
    )
    if resp['hits']['total']['value'] > 0:
        es_schema_name = resp['hits']['hits'][0]['_source']['schema_maintainer']['schema_name']
        logger.info(f"Elasticsearch index {index} has schema_name {es_schema_name}.")

    # get one 'schema_maintainer'->>'schema_name'  from Postgres
    query = sql.SQL(
        """
        SELECT document->'schema_maintainer'->>'schema_name'
        FROM {db_table} 
        WHERE {db_column_es_index} = %(index)s
        lIMIT 1;
        """).format(
            db_column_es_index=sql.Identifier(db_column_es_index),
            db_table=sql.Identifier(*db_table.split(".")),
        )
    logger.debug(query.as_string(db_conn))
    cursor.execute(query, {"index": index})
    pg_schema_name = cursor.fetchone()[0]
    logger.info(f"Postgres table {db_table} has schema_name {pg_schema_name} for index {index}.")
    if es_schema_name != pg_schema_name:
        logger.warning(f"Schema name changed for index {index}: Elasticsearch has {es_schema_name}, Postgres has {pg_schema_name}.")
        return True
    else:
        logger.info(f"Schema names match for index {index}: both have {es_schema_name}.")
        return False

    # if es.indices.exists(index=index):
    #     # query to get one schema_name from index
    #     es_query = {
    #         "size": 1,
    #         "_source": ["schema_maintainer"],
    #         "query": {"match_all": {}},
    #     }
    #     es_result = es.search(index=index, body=es_query)
    #     if es_result["hits"]["total"]["value"] > 0:
    #         es_schema_name = es_result["hits"]["hits"][0]["_source"][
    #             "schema_maintainer"
    #         ]["schema_name"]
    #         logger.info(
    #             f"Elasticsearch index {index} has schema_name {es_schema_name}."
    #         )

    
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
    run : bool = True,
):
    logger = get_run_logger()
    es = es_credentials.get_client()
    if not run:
        return logger.info("Skipping deletion of Elasticsearch indexes.")
    
    for index in indexes:
        index_name = f"{index}_{timestamp}"
        result = es.indices.delete(index=index_name)
        logger.info("Deletion of Elasticsearch index %s: %s.", index_name, result)

    return logger.info("Cleanup of Elasticsearch indexes completed.")

@task
def compare_postgres_es_count(
    indexes: list[str],
    es_credentials: ElasticsearchCredentials,
    db_credentials: DatabaseCredentials,
    db_table: str,
    db_column_es_index: str = "index",
):
    logger = get_run_logger()
    es = es_credentials.get_client()
    db_conn = get_postgres_connection(db_credentials)
    cursor = db_conn.cursor()

    for index in indexes:
        # Get count from Elasticsearch
        es_count = es.count(index=index)["count"]
        logger.info(f"Elasticsearch index {index} has {es_count} documents.")

        # Get count from Postgres
        query = sql.SQL(
            """
            SELECT COUNT(id)
            FROM {db_table} 
            WHERE {db_column_es_index} = %(index)s
            AND NOT is_deleted;
            """
        ).format(
            db_column_es_index=sql.Identifier(db_column_es_index),
            db_table=sql.Identifier(*db_table.split(".")),
        )
        logger.debug(query.as_string(db_conn))
        cursor.execute(query, {"index": index})
        pg_count = cursor.fetchone()[0]
        logger.info(f"Postgres table {db_table} has {pg_count} documents for index {index}.")

        if es_count != pg_count:
            logger.warning(f"Count mismatch for index {index}: Elasticsearch has {es_count}, Postgres has {pg_count}.")
        else:
            logger.info(f"Counts match for index {index}: both have {es_count} documents.")

    cursor.close()
    db_conn.close()

# Task to get all current indexes in Elasticsearch
@task
def get_current_es_indexes(es_credentials: ElasticsearchCredentials):
    logger = get_run_logger()
    es = es_credentials.get_client()
    # get a list of all indexes in cluster
    all_indexes = list(es.indices.get_alias(name="*").keys())
    logger.info("Current Elasticsearch indexes in cluster: %s", all_indexes)
    return all_indexes

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

    logger.info(
        "Starting streaming of records to Elasticsearch indexes %s with timestamp %s and last modified %s",
        indexes, timestamp if timestamp else "None", last_modified if last_modified else "None"
    )

    def connect_es():
        return es_credentials.get_client().options(
            request_timeout=es_request_timeout,
            max_retries=es_max_retries,
            retry_on_timeout=es_retry_on_timeout,
        )
    es = connect_es()

    def connect_db():
        # Connect to ES and Postgres
        db_conn = get_postgres_connection(db_credentials)
        # Create server-side cursor
        cursor = db_conn.cursor(name="large_query_cursor", scrollable=True)
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
        return db_conn, cursor
        
    db_conn, cursor = connect_db()

    # Stats
    records = 0
    errors = 0

    n = (
        round(record_count / 10)
        if record_count is not None
        and record_count > 0
        and round(record_count / 10) > 0
        else 1000
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
            if is_deleted:
                yield {
                    "_index": index_name,
                    "_id": (id if db_column_es_id else None),
                    "_op_type": "delete"
                }
            else:
                yield {
                    "_index": index_name,
                    "_id": (id if db_column_es_id else None),
                    "_source": document,
                    "_op_type": "index",
                }

    logger.info(
        "Starting indexing stream of %s documents with timestamp %s and last modified %s",
        record_count,
        timestamp,
        last_modified,
    )

    retries = 0
    last_row = 0  
    current_batch_start = 0
    while True:
        if retries > es_max_retries:
            raise Exception(
                f"Maximum number of indexing retries {es_max_retries} reached."
            )
        try:
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
                if records % es_chunk_size == 0:
                    last_row = current_batch_start
                    current_batch_start = records
            break
        # OperationalError and ConnectionTimeout are caught in one except to reconnect and resume the streaming_bulk
        except (OperationalError, ConnectionTimeout) as e:
            sleep(120)
            logger.error("Error during streaming_bulk: %s", e)
            logger.info("Reconnecting to Postgres and resuming streaming_bulk...")
            try:
                cursor.close()
                db_conn.close()
            except Exception as e:
                pass
            try:
                es.transport.close()
            except Exception as e:
                pass
            try:
                db_conn, cursor = connect_db()
                cursor.scroll(value=last_row, mode="absolute")
                logger.info("Reconnecting to Elasticsearch and resuming streaming_bulk...")
                es = connect_es()
                records = last_row
            except Exception as e:
                logger.error("Error reconnecting to Postgres or Elasticsearch: %s", e)
                break
        else:
            break
        retries += 1
        continue


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

@task
def count_total_updated(
    db_credentials: DatabaseCredentials,
    db_table: str,
    db_column_es_index: str = "index",
    last_modified: DateTime = None,
):
    logger = get_run_logger()
    # Connect to Postgres
    db_conn = get_postgres_connection(db_credentials)

    # Create cursor
    cursor = db_conn.cursor()

    # Run query
    query = sql.SQL(
        """
        SELECT COUNT(id)
        FROM {db_table} 
        WHERE {db_column_es_index} is not null
        AND updated_at >= %(last_modified)s;
        """
    ).format(
        db_column_es_index=sql.Identifier(db_column_es_index),
        db_table=sql.Identifier(*db_table.split(".")),
    )
    logger.debug(query.as_string(db_conn))
    cursor.execute(query, {"last_modified": str(last_modified) if last_modified else "0001-01-01T00:00:00"})
    count = cursor.fetchone()[0]
    logger.info(
        "Found %s records updated since %s in database table %s.",
        count,
        last_modified,
        db_table,
    )

    return count

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

    if not or_ids:
        indexes_from_db = get_indexes_list.submit(
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_index=db_column_es_index,
        ).result()
    else:
        indexes_from_db = [or_id.lower() for or_id in or_ids]

    logger.info("Indexing the following Elasticsearch indexes: %s.", indexes_from_db)
    current_indexes = get_current_es_indexes.submit(es_credentials=es_credentials)


    # Get timestamp to uniquely identify indexes
    timestamp = datetime.now().strftime("%Y-%m-%dt%H.%M.%S")
    if not indexes_from_db:
        logger.info('No indexes with changed records.')
        return
    
    if not or_ids:
        delete_untouched_indexes.submit(
            indexes=quote(indexes_from_db),
            es_credentials=es_credentials,
        )

    # Determine order in which to load indexes
    indexes_order = get_index_order.submit(
        indexes=quote(indexes_from_db),
        db_credentials=db_credentials,
        db_table=db_table,
        db_column_es_index=db_column_es_index,
    )

    indexes_from_db = indexes_order.result()
    if len(indexes_from_db) != len(indexes_from_db):
        raise ValueError(
            "The number of indexes does not match the number of ordered indexes."
        )
    

    for i, (index, record_count) in enumerate(indexes_from_db):
        # Check if org_name changed
        org_name_changed = check_if_org_name_changed.submit(
            index=quote(index),
            es_credentials=es_credentials,
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_index=db_column_es_index,
        ).result()
        # Create timestamped index if full sync
        index_creation_result = create_indexes.with_options(
            name=f"creating-{index}",
            tags=[
                "pg-indexer-create",
            ],
        ).submit(
            indexes=quote([index]),
            es_credentials=es_credentials,
            timestamp=timestamp,
            wait_for=[indexes_order, org_name_changed],
        ) if full_sync or org_name_changed else None

        # Count total records to update if not full sync
        record_count = count_total_updated.submit(
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_index=db_column_es_index,
            last_modified=last_modified,
            wait_for=[org_name_changed],
        ).result() if not full_sync and not org_name_changed else record_count

        if record_count == 0:
            logger.info(f"No records to update for index {index}. Skipping.")
            continue
        
        # Stream records to ES
        stream_records_result = stream_records_to_es.with_options(
            name=f"indexing-{index}",
            on_failure=[
                partial(
                    delete_indexes,
                    indexes=[index],
                    es_credentials=es_credentials,
                    timestamp=timestamp,
                    run=full_sync or org_name_changed,
                )
            ],
            tags= ["pg-indexer-large", "pg-indexer"] if i > len(indexes_from_db) - 3 else ["pg-indexer"],
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
            # don't pass timestamp when incremental update
            timestamp=timestamp if full_sync or org_name_changed else None,
            record_count=record_count,
            # don't pass last modified when full sync
            last_modified=last_modified if not full_sync and not org_name_changed else None,
            wait_for=[index_creation_result],
        )
        # Swap time-stamped index to normal index if full sync
        index_swapping = swap_indexes.with_options(
            name=f"swap-index-{index}",
        ).submit(
            indexes=quote([index]),
            es_credentials=es_credentials,
            timestamp=timestamp,
            es_timeout=es_request_timeout,
            wait_for=stream_records_result,
        ) if full_sync or org_name_changed else None

        # Compare counts between Postgres and ES
        compare_postgres_es_count.submit(
            indexes=quote([index]),
            es_credentials=es_credentials,
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_index=db_column_es_index,
            wait_for=[index_swapping, stream_records_result],
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
