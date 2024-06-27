import psycopg2
from elasticsearch.helpers import streaming_bulk
from prefect import flow, get_run_logger, task
from prefect.runtime import flow_run
from prefect.testing.utilities import prefect_test_harness
from prefect_meemoo.config.last_run import (get_last_run_config,
                                            save_last_run_config)
from prefect_meemoo.elasticsearch.credentials import ElasticsearchCredentials
from prefect_sqlalchemy.credentials import DatabaseCredentials
from psycopg2.extras import RealDictCursor

BATCH_SIZE = 1000

def get_postgres_connection():
    """
        Function to get a postgres connection.
    """
    postgres_credentials : DatabaseCredentials = DatabaseCredentials.load(flow_run.get_parameters()["db_block_name"])
    logger = get_run_logger()
    logger.info("(Re)connecting to postgres")
    db_conn = psycopg2.connect(
        user=postgres_credentials.username,
        password=postgres_credentials.password.get_secret_value(),
        host=postgres_credentials.host,
        port=postgres_credentials.port,
        database=postgres_credentials.database,
        cursor_factory=RealDictCursor
    )
    return db_conn
# Function to get records from PostgreSQL using a cursor and stream to Elasticsearch
@task
def stream_records_to_es(es_credentials: ElasticsearchCredentials, es_index: str, db_table: str, db_column_es_id: str, last_modified = None):
    logger = get_run_logger()

    db_conn = get_postgres_connection()
    cursor = db_conn.cursor(name='large_query_cursor')
    cursor.itersize = BATCH_SIZE
    cursor.execute(f"SELECT * FROM {db_table} {f'WHERE updated_at >= {last_modified}' if last_modified is not None else ''}")

    es = es_credentials.get_client()

    def generate_actions():
        for record in cursor.fetchall():
            yield {
                "_index": es_index,
                "_id": dict(record)[db_column_es_id] if db_column_es_id else None,
                "_source": dict(record)
            }

    errors = 0
    for ok, item in streaming_bulk(es, generate_actions()):
        if not ok:
            errors += 1
            logger.error(item)

    cursor.close()
    db_conn.close()
    return f"Streaming records into Elasticsearch index: '{es_index}' completed. {errors} records failed."

# Define the Prefect flow
@flow(
    name= "prefect-flow-arc-indexer",
    on_completion=[save_last_run_config]
)
def main_flow(
    db_block_name: str,
    db_table: str,
    es_block_name: str, 
    es_index: str,
    db_column_es_id: str= None,
    full_sync: bool = False
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
    stream_task = stream_records_to_es.submit(es_credentials,es_index, db_table, db_column_es_id, last_modified).result()
    logger.info(stream_task)

# Execute the flow
if __name__ == "__main__":
    with prefect_test_harness():
        es = ElasticsearchCredentials(
            url="https://localhost:9200",
            username="elastic",
            password="elk-password"
        )
        es.save("elastic-arc")
        db = DatabaseCredentials(
            host= '0.0.0.0',
            port= 5432,
            username= "postgres",
            password="mysecretpassword",
            database="postgres",
            driver="postgresql+asyncpg"
        )
        db.save('hasura-arc')
        main_flow(
            db_block_name="hasura-arc",
            es_block_name="elastic-arc",
            db_table="index",
            es_index="arcv3",
            db_column_es_id="id"
        )
