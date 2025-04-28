# Hetarchief Elasticsearch indexer

Prefect flow to index documents from a postgres database in Elasticsearch for hetarchief.be. 

## Synopsis

This flow runs the following tasks:
1. Retrieve a list of all indexes from the postgres view
2. If there is a full sync:
   1. delete all indexes that are no longer present in the view
   2. retrieve the indexes ordered by size
   3. For each index (smallest first)
      1. create new index 
      2. stream documents to the elasticsearch API
      3. replace old index by the new index by switching the alias and deleting the old index
3. If it is not a full sync, stream documents to the elasticsearch API to be deleted or indexed

If an error occurs during streaming, the created indexes are rolled back.

The Flow's diagram:

![Diagram of Prefect flow](diagram.png)

## Prerequisites

The following Prefect Blocks need to be configured:
- location and credentials of the postgres database (type: `prefect_sqlalchemy.DatabaseCredentials`)
- location and credentials of the elasticsearch cluster (type: `prefect_meemoo.credentials.ElasticsearchCredentials`)


## Usage

The Prefect Flow requires setting the following parameters:
- `db_block_name`: name of the database block
- `db_table`: name of the table or view where the index documents are stored
- `es_block_name`: name of the elasticsearch block
- `db_column_es_id`: name of the column that contains the document identifiers(default: `"id"`)
- `db_column_es_index`: name of the column that contains the index alias (default: `"index"`)
- `or_ids_to_run`: list of indexes that need to be included. Set to `None` for all indexes. (default: `None`)
- `full_sync`: set the sync to a full reload (default: `False`)
- `db_batch_size`: size of the database cursor (default: `1000`)
- `es_chunk_size`: elasticsearch chunk size (default: `500`)
- `es_request_timeout`: elasticsearch request timeout (default: `30`)
- `es_max_retries`: elasticsearch retries when a document failed to index (default: `10`)
- `es_retry_on_timeout`: retry indexing a document when at timeout (default: `True`)