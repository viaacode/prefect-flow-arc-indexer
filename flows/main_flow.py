import psycopg2
from elasticsearch.helpers import streaming_bulk
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from prefect.testing.utilities import prefect_test_harness
from prefect_meemoo.config.last_run import get_last_run_config, save_last_run_config
from prefect_meemoo.elasticsearch.credentials import ElasticsearchCredentials
from prefect_sqlalchemy.credentials import DatabaseCredentials
from psycopg2.extras import RealDictCursor

BATCH_SIZE = 1000


def get_postgres_connection():
    """
    Function to get a postgres connection.
    """
    postgres_credentials: DatabaseCredentials = DatabaseCredentials.load(
        flow_run.get_parameters()["db_block_name"]
    )
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


# Function to get records from PostgreSQL using a cursor and stream to Elasticsearch
@task
def stream_records_to_es(
    indexes: list[str],
    es_credentials: ElasticsearchCredentials,
    db_table: str,
    db_column_es_id: str = "id",
    db_column_es_index: str = "index",
    last_modified=None,
):
    logger = get_run_logger()

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
    db_conn = get_postgres_connection()
    es = es_credentials.get_client()

    # Create server-side cursor
    cursor = db_conn.cursor(name="large_query_cursor")
    cursor.itersize = BATCH_SIZE

    # Run query
    cursor.execute(sql_query)

    def generate_actions():
        for record in cursor:
            yield {
                "_index": record[1],
                "_id": record[2] if len(record) > 2 else None,
                "_source": record[0],
            }

    records = 0
    errors = 0
    for ok, item in streaming_bulk(es, generate_actions()):
        records += 1
        if not ok:
            errors += 1
            logger.error(item)

    cursor.close()
    db_conn.close()
    return f"Streaming records into Elasticsearch indexes: {indexes_list} completed. {errors} of {records} records failed."


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
):
    """
    Flow to index all of the Hasura Postgres records.
    """
    # Load logger
    logger = get_run_logger()

    # Implement delta sync when available
    last_modified = get_last_run_config("%Y-%m-%d") if not full_sync else None

    # Load credentials
    es_credentials = ElasticsearchCredentials.load(es_block_name)

    # Init task
    # for or_id in or_ids_to_run:
    stream_task = stream_records_to_es.submit(
        or_ids_to_run,
        es_credentials,
        db_table,
        db_column_es_id,
        db_column_es_index,
        last_modified,
    ).result()
    logger.info(stream_task)


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
