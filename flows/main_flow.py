from prefect import task, flow, get_run_logger
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from prefect_sqlalchemy.credentials import DatabaseCredentials
from prefect_meemoo.config.last_run import (get_last_run_config,
                                            save_last_run_config)

# TODO: Elasticsearch connection settings; move to ElasticsearchCredentials block in prefect_meemoo
ES_HOST = 'your_elasticsearch_host'
ES_PORT = 'your_elasticsearch_port'
ES_INDEX = 'your_elasticsearch_index'

BATCH_SIZE = 1000

# Function to get records from PostgreSQL using a cursor and stream to Elasticsearch
@task
def stream_records_to_es(db_credentials):
    connection_str = db_credentials.get_connection_string()
    engine = create_engine(connection_str)
    Session = sessionmaker(bind=engine)
    session = Session()

    connection = session.connection().connection
    cursor = connection.cursor(name='large_query_cursor')
    cursor.itersize = BATCH_SIZE 
    cursor.execute("SELECT * FROM your_table")

    es = Elasticsearch([{'host': ES_HOST, 'port': ES_PORT}])
    
    def generate_actions():
        for record in cursor:
            yield {
                "_index": ES_INDEX,
                "_source": dict(record)
            }

    streaming_bulk(es, generate_actions())
    cursor.close()
    session.close()
    return "Streaming completed"

# Define the Prefect flow
@flow(
    name= "prefect-flow-arc-indexer",
    on_completion=[save_last_run_config]
)
def main_flow(postgres_block_name: str,elasticsearch_block_name: str):
    """
    Flow to index all of the Hasura Postgres records.
    """
    # Load logger
    logger = get_run_logger()

    # TODO: implement delta sync when available
    # last_modified = get_last_run_config("%Y-%m-%d")

    # Load credentials
    # es_credentials = ElasticsearchCredentials.load("elastic-credentials")
    db_credentials = DatabaseCredentials.load("postgres-credentials")

    # Init task
    stream_task = stream_records_to_es(db_credentials)

# Execute the flow
if __name__ == "__main__":
    main_flow(
        postgres_block_name="hasura-arc",
        elasticsearch_block_name="elastic-arc"
    )
