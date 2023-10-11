from elasticsearch import Elasticsearch, TransportError
from data_loading import all_flow_data

# Spécifiez les URL des nœuds Elasticsearch (peut être un seul ou une liste de nœuds)
hosts = ["http://localhost:9200"]  # Exemple pour un nœud local

# Initialisez Elasticsearch en spécifiant les hôtes
es = Elasticsearch(hosts=hosts)

# Appliquez les options de transport à l'objet Elasticsearch
es.options(request_timeout=30, max_retries=3)

# Spécifiez l'index Elasticsearch et le type de document
index_name = 'flow_data_index'

# Indexez les données de flux dans Elasticsearch
for flow in all_flow_data:
    es.index(index=index_name, document=flow, id=None)

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