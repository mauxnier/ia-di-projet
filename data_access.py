"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: 1 - Flow Classification
Stage: 3 - Data access via REST API
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 10/2023
"""

from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request
import matplotlib.pyplot as plt
import numpy as np
import base64

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
    
@app.route('/distinct_applications', methods=['GET'])
def get_distinct_applications():
    """
    Endpoint to get the list of distinct applications.
    """
    search_query = {
        "aggs": {
            "distinct_applications": {
                "terms": {
                    "field": "appName.keyword",
                }
            }
        }
    }

    try:
        result = es.search(index=index_name, aggs=search_query["aggs"])

        # Extracting the aggregation results
        distinct_applications = [bucket['key'] for bucket in result['aggregations']['distinct_applications']['buckets']]

        return jsonify(distinct_applications)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/flows_by_application', methods=['GET'])
def get_flows_by_application():
    """
    Endpoint to get the list of flows for a given application.
    """
    application_name = request.args.get('application')

    if not application_name:
        return jsonify({"error": "Please provide an 'application_name' parameter in the query."}), 400

    search_query = {
        "query": {
            "term": {
                "appName.keyword": application_name
            }
        }
    }

    try:
        result = es.search(index=index_name, query=search_query["query"])
        flows = result.get('hits', {}).get('hits', [])

        # Extract relevant information from the flows
        flow_data = [{'_id': flow['_id'], '_source': flow['_source']} for flow in flows]

        return jsonify(flow_data)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/flows_count_by_application', methods=['GET'])
def get_flows_count_by_application():
    """
    Endpoint to get the number of flows for each application.
    """
    
    try:
        result = es.search(index=index_name, aggs={'applications_count': {'terms': {'field': 'appName.keyword'}}})
        application_counts = {bucket['key']: bucket['doc_count'] for bucket in result['aggregations']['applications_count']['buckets']}
        return jsonify(application_counts)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/payload_sizes_by_application', methods=['GET'])
def get_payload_sizes_by_application():
    """
    Endpoint to get the source and destination payload size for each application.
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
            "applications": {
                "terms": {
                    "field": "appName.keyword",
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
        application_info = {}
        for bucket in result['aggregations']['applications']['buckets']:
            application_name = bucket['key']
            total_source_payload_size = bucket['total_source_payload_size']['value']
            total_destination_payload_size = bucket['total_destination_payload_size']['value']
            application_info[application_name] = {
                "total_source_payload_size": total_source_payload_size,
                "total_destination_payload_size": total_destination_payload_size
            }

        return jsonify(application_info)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/bytes_sizes_by_application', methods=['GET'])
def get_bytes_sizes_by_application():
    """
    Endpoint to get the source and destination bytes size for each application.
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
            "applications": {
                "terms": {
                    "field": "appName.keyword",
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
        result = es.search(index=index_name, body=search_query)

        # Extracting the aggregation results
        application_info = {}
        for bucket in result['aggregations']['applications']['buckets']:
            application_name = bucket['key']
            source_bytes_size = bucket['source_bytes_size']['value']
            destination_bytes_size = bucket['destination_bytes_size']['value']
            application_info[application_name] = {
                "source_bytes_size": source_bytes_size,
                "destination_bytes_size": destination_bytes_size
            }

        return jsonify(application_info)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/packets_by_application', methods=['GET'])
def get_packets_by_application():
    """
    Endpoint to get the number of packets for each application.
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
            "applications": {
                "terms": {
                    "field": "appName.keyword",
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
        result = es.search(index=index_name, aggs=search_query["aggs"])

        # Extracting the aggregation results
        application_info = {}
        for bucket in result['aggregations']['applications']['buckets']:
            application_name = bucket['key']
            total_source_packets = bucket['total_source_packets']['value']
            total_destination_packets = bucket['total_destination_packets']['value']
            application_info[application_name] = {
                "total_source_packets": total_source_packets,
                "total_destination_packets": total_destination_packets
            }

        return jsonify(application_info)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/get_flows_and_packets', methods=['GET'])
def get_flows_and_packets():
    # Effectuez une requête Elasticsearch pour obtenir les données
    search_query = {
        "aggs": {
            "total_flows": {
                "sum": {
                    "field": "flows_field"  # Remplacez par le nom de votre champ de flux
                }
            },
            "total_packets": {
                "sum": {
                    "field": "packets_field"  # Remplacez par le nom de votre champ de paquets
                }
            }
        }
    }

    try:
        result = es.search(index=index_name, aggs=search_query["aggs"])

        # Extrayez les agrégations de données
        total_flows = result['aggregations']['total_flows']['value']
        total_packets = result['aggregations']['total_packets']['value']

        # Retournez les données en tant que réponse JSON
        return jsonify({
            "total_flows": total_flows,
            "total_packets": total_packets
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
    
@app.route('/ranked_distribution', methods=['GET'])
def get_ranked_distribution():
    query = {
        "size": 1000,  # You can adjust the size based on your data
        "_source": ["totalSourcePackets", "totalDestinationPackets"]
    }
    response = es.search(index=index_name, body=query)
    flows = []
    packets = []
    for hit in response['hits']['hits']:
        flow = hit['_source']
        flows.append(flow['totalSourcePackets'] + flow['totalDestinationPackets'])
        packets.append(flow['totalSourcePackets'])

    # Sort data in descending order
    flows = sorted(flows, reverse=True)
    packets = sorted(packets, reverse=True)

    # Calculate the ranks
    ranks = np.arange(1, len(flows) + 1)

    # Create a log-log plot
    plt.loglog(packets, flows, marker='o', linestyle='-', color='b')

    # Set labels and title
    plt.ylabel('#Packets (Log Scale)')
    plt.xlabel('#Flows (Log Scale)')
    plt.title('Ranked Distribution of Flows vs. Packets')

    # Save the plot as an image (optional)
    # plt.savefig('ranked_distribution.png')

    # Convert the plot to a base64 encoded string (optional)
    import io
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    image_data = base64.b64encode(buf.getvalue()).decode('utf-8')

    # You can return the image data as a response if needed
    # return jsonify({'image_data': image_data})

    # Show the plot (this line should be used in development, not in production)
    plt.show()

    return jsonify({'message': 'Ranked distribution plot generated'})


if __name__ == '__main__':
    app.run(debug=True)