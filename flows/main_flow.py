import psycopg2
from elasticsearch.helpers import streaming_bulk
from prefect import flow, get_run_logger, task
from prefect.testing.utilities import prefect_test_harness
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config
from prefect_meemoo.elasticsearch.credentials import ElasticsearchCredentials
from prefect_sqlalchemy.credentials import DatabaseCredentials
from psycopg2.extras import RealDictCursor
from datetime import datetime
import os

os.environ.update(PREFECT_LOGGING_LEVEL="DEBUG")


def get_postgres_connection(postgres_credentials: DatabaseCredentials):
    """
    Function to get a postgres connection.
    """
    logger = get_run_logger()
    logger.info("(Re)connecting to postgres")
    db_conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        cursor_factory=RealDictCursor,
    )
    return db_conn


@task
def get_indexes_list(db_credentials: DatabaseCredentials):
    logger = get_run_logger()
    # Connect to Postgres
    db_conn = get_postgres_connection(db_credentials)

    # Create cursor
    cursor = db_conn.cursor()

    # Run query
    cursor.execute("SELECT DISTINCT(index) FROM graph._index_intellectual_entity;")

    return [row["index"] for row in list(cursor.fetchall())]


@task
def create_indexes(
    indexes: list[str], es_credentials: ElasticsearchCredentials, timestamp: str
):
    logger = get_run_logger()
    es = es_credentials.get_client()
    for index in indexes:
        index_name = f"{index}_{timestamp}"
        result = es.indices.create(index=index_name)
        logger.info(f"Created of Elasticsearch index {result}.")

    return logger.info("Creation of Elasticsearch indexes completed.")


# Function to get records from PostgreSQL using a cursor and stream to Elasticsearch
@task
def stream_records_to_es(
    indexes: list[str],
    es_credentials: ElasticsearchCredentials,
    db_credentials: DatabaseCredentials,
    db_table: str,
    db_column_es_id: str = "id",
    db_column_es_index: str = "index",
    db_batch_size: int = 1000,
    es_chunk_size: int = 500,
    timestamp: str = None,  # timestamp to uniquely identify indexes
    last_modified=None,
):
    logger = get_run_logger()

    logger.info(
        f"Starting indexing stream with timestamp {timestamp} and last modified {last_modified}."
    )

    # Compose SQL query

    # Integrate last_modified when not None
    suffix = f"AND updated_at >= {last_modified}" if last_modified else ""
    db_column_es_id_param = f", {db_column_es_id}" if db_column_es_id else ""
    indexes_list = ",".join(map(lambda index: f"'{index}'", indexes))
    sql_query = f"""
    SELECT document,{db_column_es_index}{db_column_es_id_param}
    FROM {db_table}
    WHERE {db_column_es_index} IN ({indexes_list}) {suffix}
    """

    logger.info(f"Creating cursor from query {sql_query}.")

    # Connect to ES and Postgres
    db_conn = get_postgres_connection(db_credentials)
    es = es_credentials.get_client().options(
        timeout=30, max_retries=10, retry_on_timeout=True
    )

    # Create server-side cursor
    cursor = db_conn.cursor(name="large_query_cursor")
    cursor.itersize = db_batch_size

    # Run query
    cursor.execute(sql_query)

    # Fill new indexes
    def generate_actions():
        for record in cursor:
            dict_record = dict(record)
            index_name = (
                f"{dict_record[db_column_es_index]}_{timestamp}"
                if last_modified is None
                else dict_record[db_column_es_index]
            )
            yield {
                "_index": index_name,
                "_id": (dict_record[db_column_es_id] if db_column_es_id else None),
                "_source": dict_record["document"],
            }

    records = 0
    errors = 0
    for ok, item in streaming_bulk(es, generate_actions(), chunk_size=es_chunk_size):
        records += 1
        if not ok:
            errors += 1
            logger.error(item)

    cursor.close()
    db_conn.close()

    return logger.info(
        f"Streaming records into Elasticsearch indexes {indexes_list} completed. {errors} of {records} records failed."
    )


# Task to do changeover from old to new indexes when full sync
@task
def swap_indexes(
    indexes: list[str], es_credentials: ElasticsearchCredentials, timestamp: str
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
        logger.info(f"Switching alias {index} to new index {alias_name}.")
        es.indices.put_alias(name=index, index=alias_name)

        # delete old indexes if there are any
        if len(old_indexes) > 0:
            old_indexes_seq = ",".join(old_indexes)
            logger.info(f"Deleting old indexes {old_indexes_seq} for alias {index}")
            es.indices.delete(index=old_indexes_seq)

    return logger.info("Elasticsearch indexes swapped succesfully.")


# Define the Prefect flow
@flow(name="prefect-flow-arc-indexer", on_completion=[save_last_run_config])
def main_flow(
    db_block_name: str,
    db_table: str,
    es_block_name: str,
    db_column_es_id: str = "id",
    db_column_es_index: str = "index",
    or_ids_to_run: list[str] = None,
    full_sync: bool = False,
    db_batch_size: int = 1000,
    es_chunk_size: int = 500,
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
    logger.info(f"Start indexing process (full sync = {full_sync})")

    # Get all indexes from database if none provided
    if or_ids_to_run is None or len(or_ids_to_run) < 1:
        or_ids_to_run = get_indexes_list.submit(db_credentials=db_credentials).result()

    if full_sync:
        # Get timestamp to uniquely identify indexes
        timestamp = datetime.now().strftime("%Y-%m-%dt%H.%M.%S")

        t1 = create_indexes.submit(
            indexes=or_ids_to_run, es_credentials=es_credentials, timestamp=timestamp
        )
        t2 = stream_records_to_es.submit(
            indexes=or_ids_to_run,
            es_credentials=es_credentials,
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_id=db_column_es_id,
            db_column_es_index=db_column_es_index,
            db_batch_size=db_batch_size,
            es_chunk_size=es_chunk_size,
            timestamp=timestamp,
            wait_for=t1,
        )
        swap_indexes.submit(
            indexes=or_ids_to_run,
            es_credentials=es_credentials,
            timestamp=timestamp,
            wait_for=t2,
        )
    else:
        stream_records_to_es.submit(
            indexes=or_ids_to_run,
            es_credentials=es_credentials,
            db_credentials=db_credentials,
            db_table=db_table,
            db_column_es_id=db_column_es_id,
            db_column_es_index=db_column_es_index,
            db_batch_size=db_batch_size,
            es_chunk_size=es_chunk_size,
            last_modified=get_last_run_config("%Y-%m-%d"),
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
