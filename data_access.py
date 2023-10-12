from elasticsearch import Elasticsearch
from flask import Flask, jsonify

index_name = 'flow_data_index'

# Spécifiez les URL des nœuds Elasticsearch (peut être un seul ou une liste de nœuds)
hosts = ["http://localhost:9200"]  # Exemple pour un nœud local

# Initialisez Elasticsearch en spécifiant les hôtes
es = Elasticsearch(hosts=hosts)

app = Flask("API d'acces aux donnees")

@app.route('/distinct_protocols', methods=['GET'])
def get_distinct_protocols():
    """
    Endpoint pour obtenir une liste de tous les protocoles distincts dans l'index.
    """
    
    try:
        result = es.search(index=index_name, aggs={'distinct_protocols': {'terms': {'field': 'protocolName.keyword'}}})
        distinct_protocols = [bucket['key'] for bucket in result['aggregations']['distinct_protocols']['buckets']]
        return jsonify(distinct_protocols)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)