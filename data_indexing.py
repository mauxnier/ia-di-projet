from elasticsearch import Elasticsearch
from data_loading import all_flow_data

# Specifier les nœuds Elasticsearch
hosts = ["http://localhost:9200"]  # Exemple pour un nœud local

# Initialize Elasticsearch client
es = Elasticsearch(hosts=hosts)

# Specify the Elasticsearch index and document type
index_name = 'flow_data_index'
doc_type = 'flow'

# Index the flow data into Elasticsearch
for flow in all_flow_data:
    es.index(index=index_name, doc_type=doc_type, body=flow, id=None, pipeline=None, refresh=True, routing=None, timeout=None, version=None, version_type=None, request_timeout=None)

# You can now search and retrieve data from Elasticsearch as needed
# For example, search for all flows with a source port of 80
# Note that the port field is stored as a string, so we need to use a string query
# See https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

# Define the query
query = {
    "query": {
        "query_string": {
            "query": "port:80"
        }
    }
}

# Execute the query
results = es.search(index=index_name, body=query)

# Print the results
print("Nombre de résultats: ", results['hits']['total']['value'])
print("Premier résultat: ", results['hits']['hits'][0]['_source'])