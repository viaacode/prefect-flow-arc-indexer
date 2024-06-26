from prefect import task, flow, get_run_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from prefect_sqlalchemy.credentials import DatabaseCredentials
from prefect_meemoo.elasticsearch.credentials import ElasticsearchCredentials
from prefect_meemoo.config.last_run import (get_last_run_config,
                                            save_last_run_config)

BATCH_SIZE = 1000

# Function to get records from PostgreSQL using a cursor and stream to Elasticsearch
@task
def stream_records_to_es(db_credentials, es_credentials, es_index, db_table, last_modified = None):
    connection_str = db_credentials.get_connection_string()
    engine = create_engine(connection_str)
    Session = sessionmaker(bind=engine)
    session = Session()

    connection = session.connection().connection
    cursor = connection.cursor(name='large_query_cursor')
    cursor.itersize = BATCH_SIZE 
    # Integrate last_modified when not None
    cursor.execute(f"SELECT * FROM {db_table} {f"WHERE updated_at >= {last_modified}" if last_modified is not None else ""}")

    es = Elasticsearch(
        host=es_credentials.host,
        basic_auth=(es_credentials.username, es_credentials.password.get_secret_value())
    )
    
    def generate_actions():
        for record in cursor:
            yield {
                "_index": es_index,
                "_source": dict(record)
            }

    streaming_bulk(es, generate_actions())
    cursor.close()
    session.close()
    return "Streaming records into Elasticsearch completed."

# Define the Prefect flow
@flow(
    name= "prefect-flow-arc-indexer",
    on_completion=[save_last_run_config]
)
def main_flow(
    db_block_name: str,
    db_table: str,
    elasticsearch_block_name: str, 
    elasticsearch_index: str,
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
    es_credentials = ElasticsearchCredentials.load(elasticsearch_block_name)
    db_credentials = DatabaseCredentials.load(db_block_name)

    # Init task
    stream_task = stream_records_to_es(db_credentials,es_credentials,elasticsearch_index, db_table, last_modified)
    logger.info(stream_task)

# Execute the flow
if __name__ == "__main__":
    main_flow(
        db_block_name="hasura-arc",
        elasticsearch_block_name="elastic-arc",
        db_table="index",
        elasticsearch_index="arcv3"
    )
