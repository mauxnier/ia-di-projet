from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request

index_name = 'flow_data_index'

# Spécifiez les URL des nœuds Elasticsearch (peut être un seul ou une liste de nœuds)
hosts = ["http://localhost:9200"]  # Exemple pour un nœud local

# Initialisez Elasticsearch en spécifiant les hôtes
es = Elasticsearch(hosts=hosts)

app = Flask("API d'acces aux donnees")

def get_numerical_field(field_name):
    return f"Integer.parseInt(doc['{field_name}'].value)"

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
    
@app.route('/flows_by_protocol', methods=['GET'])
def get_flows_by_protocol():
    """
    Endpoint to get a list of flows for a given protocol.
    """
    protocol = request.args.get('protocol')
    if not protocol:
        return jsonify({"error": "Protocol parameter is missing"}), 400
    
    try:
        result = es.search(index=index_name, query={"match": {"protocolName.keyword": protocol}})
        flows = [hit['_source'] for hit in result['hits']['hits']]
        return jsonify(flows)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/flows_count_by_protocol', methods=['GET'])
def get_flows_count_by_protocol():
    """
    Endpoint to get the number of flows for each protocol.
    """
    
    try:
        result = es.search(index=index_name, aggs={'protocols_count': {'terms': {'field': 'protocolName.keyword'}}})
        protocol_counts = {bucket['key']: bucket['doc_count'] for bucket in result['aggregations']['protocols_count']['buckets']}
        return jsonify(protocol_counts)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/payload_sizes_by_protocol', methods=['GET'])
def get_payload_sizes_by_protocol():
    """
    Endpoint to get the source and destination payload size for each protocol.
    """
    mapping = {
        "properties": {
            "sourcePayloadAsBase64": {
                "type": "text",
                "fielddata": True
            },
            "destinationPayloadAsBase64": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    search_query = {
        "aggs": {
            "protocols": {
                "terms": {
                    "field": "protocolName.keyword",
                },
                "aggs": {
                    "total_source_payload_size": {
                        "sum": {
                            "script": {
                                "source": "doc['sourcePayloadAsBase64'].size()"
                            }
                        }
                    },
                    "total_destination_payload_size": {
                        "sum": {
                            "script": {
                                "source": "doc['destinationPayloadAsBase64'].size()"
                            }
                        }
                    }
                }
            }
        }
    }

    try:
        result = es.search(index=index_name, body=search_query)

        # Extracting the aggregation results
        protocol_info = {}
        for bucket in result['aggregations']['protocols']['buckets']:
            protocol_name = bucket['key']
            total_source_payload_size = bucket['total_source_payload_size']['value']
            total_destination_payload_size = bucket['total_destination_payload_size']['value']
            protocol_info[protocol_name] = {
                "total_source_payload_size": total_source_payload_size,
                "total_destination_payload_size": total_destination_payload_size
            }

        return jsonify(protocol_info)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/bytes_sizes_by_protocol', methods=['GET'])
def get_bytes_sizes_by_protocol():
    """
    Endpoint to get the source and destination bytes size for each protocol.
    """

    mapping = {
        "properties": {
            "totalSourcebytes": {
                "type": "text",
                "fielddata": True
            },
            "totalDestinationbytes": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    search_query = {
        "aggs": {
            "protocols": {
                "terms": {
                    "field": "protocolName.keyword",
                },
                "aggs": {
                    "source_bytes_size": {
                        "sum": {
                            "script": {
                                "source": get_numerical_field("totalSourceBytes")
                            }
                        }
                    },
                    "destination_bytes_size": {
                        "sum": {
                            "script": {
                                "source": get_numerical_field("totalDestinationBytes")
                            }
                        }
                    }
                }
            }
        }
    }

    try:
        result = es.search(index=index_name, aggs=search_query['aggs'])

        # Extracting the aggregation results
        protocol_info = {}
        for bucket in result['aggregations']['protocols']['buckets']:
            protocol_name = bucket['key']
            source_bytes_size = bucket['source_bytes_size']['value']
            destination_bytes_size = bucket['destination_bytes_size']['value']
            protocol_info[protocol_name] = {
                "source_bytes_size": source_bytes_size,
                "destination_bytes_size": destination_bytes_size
            }

        return jsonify(protocol_info)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/packets_by_protocol', methods=['GET'])
def get_packets_by_protocol():
    """
    Endpoint to get the total source/destination packets for each protocol.
    """

    mapping = {
        "properties": {
            "totalSourcePackets": {
                "type": "text",
                "fielddata": True
            },
            "totalDestinationPackets": {
                "type": "text",
                "fielddata": True
            }
        }
    }
    es.indices.put_mapping(index=index_name, body=mapping)

    search_query = {
        "aggs": {
            "protocols": {
                "terms": {
                    "field": "protocolName.keyword",
                },
                "aggs": {
                    "total_source_packets": {
                        "sum": {
                            "script": {
                                "source": get_numerical_field("totalSourcePackets")
                            }
                        }
                    },
                    "total_destination_packets": {
                        "sum": {
                            "script": {
                                "source": get_numerical_field("totalDestinationPackets")
                            }
                        }
                    }
                }
            }
        }
    }

    try:
        result = es.search(index="flow_data_index", aggs=search_query["aggs"])

        # Extracting the aggregation results
        protocol_info = {}
        for bucket in result['aggregations']['protocols']['buckets']:
            protocol_name = bucket['key']
            total_source_packets = bucket['total_source_packets']['value']
            total_destination_packets = bucket['total_destination_packets']['value']
            protocol_info[protocol_name] = {
                "total_source_packets": total_source_packets,
                "total_destination_packets": total_destination_packets
            }

        return jsonify(protocol_info)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)