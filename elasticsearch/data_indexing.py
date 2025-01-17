"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: 1 - Flow Classification
Stage: 2 - Data indexing in Elasticsearch
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 10/2023
"""

from elasticsearch import Elasticsearch, helpers
from data_loading import all_flow_data

# Spécifiez les URL des nœuds Elasticsearch (peut être un seul ou une liste de nœuds)
hosts = ["http://localhost:9200"]  # Exemple pour un nœud local

# Initialisez Elasticsearch en spécifiant les hôtes
es = Elasticsearch(hosts=hosts)

# Appliquez les options de transport à l'objet Elasticsearch
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Spécifiez l'index Elasticsearch et le type de document
index_name = "flow_data_index"

# Supprimez l'index existant (attention, cela supprime toutes les données de l'index)
es.indices.delete(
    index=index_name, ignore=[400, 404]
)  # Ignore les erreurs 400 et 404 si l'index n'existe pas

# Indexez les données de flux dans Elasticsearch
actions = []

for flow in all_flow_data:
    action = {
        "_op_type": "index",  # Opération d'indexation
        "_index": index_name,  # Remplacez par le nom de votre index
        "_source": flow,  # Les données de votre flux
    }
    actions.append(action)

print(f"Nombre d'actions : {len(actions)}")

# Utilisez helpers.bulk() pour indexer les actions en une seule opération
success, failed = helpers.bulk(
    es, actions, index=index_name
)  # Remplacez par le nom de votre index

print(f"Documents indexes avec succes : {success}")
print(f"Echecs d'indexation : {failed}")

# You can now search and retrieve data from Elasticsearch as needed
# For example, search for all flows with a source port of 80
# Note that the port field is stored as a string, so we need to use a string query
# See https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

# Define the query
# query = {
#     "query": {
#         "query_string": {
#             "query": "port:80"
#         }
#     }
# }

# Execute the query
# results = es.search(index=index_name, body=query)

# Print the results
# print("Nombre de résultats: ", results['hits']['total']['value'])
# print("Premier résultat: ", results['hits']['hits'][0]['_source'])
