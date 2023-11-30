"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: 1 - Flow Classification
Stage: 4 - Data preprocessing (one-hot encoding)
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 10/2023
"""

from elasticsearch import Elasticsearch, helpers
import pandas as pd
import ipaddress
import os
import sys

# Display a warning message
print(
    "WARNING: This script will perform operations that may take a significant amount of time."
)

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def process_df(df):
    # Step 2 : Perform conversions on the data

    columns_to_convert = [
        "sourcePort",
        "destinationPort",
        "totalSourceBytes",
        "totalDestinationBytes",
        "totalSourcePackets",
        "totalDestinationPackets",
    ]

    # Loop through each column and convert to numeric
    for column in columns_to_convert:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Convertir les colonnes startDateTime et stopDateTime en objets datetime
    df["startDateTime"] = pd.to_datetime(df["startDateTime"])
    df["stopDateTime"] = pd.to_datetime(df["stopDateTime"])

    # Calculer la durée entre startDateTime et stopDateTime
    df["duration"] = df["stopDateTime"] - df["startDateTime"]

    # Step 3: Perform encoding on Categorical Data

    # Créer des intervalles pour les adresses IP
    def map_ip_to_interval(ip):
        ip = ipaddress.IPv4Address(ip)
        if ip <= ipaddress.IPv4Address("128.0.0.0"):
            return "PrivateNetwork"
        elif ip <= ipaddress.IPv4Address("192.0.0.0"):
            return "PublicNetwork"
        elif ip <= ipaddress.IPv4Address("224.0.0.0"):
            return "MulticastNetwork"
        else:
            return "UnknownNetwork"

    # Créer des intervalles de ports
    port_bins = [0, 1023, 49151, 65535]
    port_labels = ["WellKnownPorts", "RegisteredPorts", "DynamicPrivatePorts"]

    # Créer des intervalles pour totalSourceBytes et totalDestinationBytes
    bytes_bins = [
        0,
        10000,
        50000,
        float("inf"),
    ]
    bytes_labels = ["Small", "Medium", "Large"]

    # Créer des intervalles pour totalSourcePackets et totalDestinationPackets
    packets_bins = [
        0,
        100,
        500,
        float("inf"),
    ]
    packets_labels = ["Low", "Medium", "High"]

    # Créer des intervalles pour la durée
    def map_duration_to_category(duration):
        hours = duration.total_seconds() / 3600  # Convertir la durée en heures

        if 0 <= hours < 2:
            return "Short"
        elif 2 <= hours < 8:
            return "Medium"
        else:
            return "Long"

    # Liste des noms de colonnes catégorielles
    categorical_columns = [
        "sourceCategory",
        "destinationCategory",
        "sourcePortCategory",
        "destinationPortCategory",
        "totalSourceBytesCategory",
        "totalDestinationBytesCategory",
        "totalSourcePacketsCategory",
        "totalDestinationPacketsCategory",
        "durationCategory",
    ]

    for column in categorical_columns:
        field_name = column.replace("Category", "")
        if column == "sourceCategory" or column == "destinationCategory":
            df[column] = df[field_name].apply(map_ip_to_interval)
        if column == "sourcePortCategory" or column == "destinationPortCategory":
            df[column] = pd.cut(
                df[field_name], bins=port_bins, labels=port_labels, include_lowest=True
            )
        if (
            column == "totalSourceBytesCategory"
            or column == "totalDestinationBytesCategory"
        ):
            df[column] = pd.cut(
                df[field_name],
                bins=bytes_bins,
                labels=bytes_labels,
                include_lowest=True,
            )
        if (
            column == "totalSourcePacketsCategory"
            or column == "totalDestinationPacketsCategory"
        ):
            df[column] = pd.cut(
                df[field_name],
                bins=packets_bins,
                labels=packets_labels,
                include_lowest=True,
            )
        if column == "durationCategory":
            df[column] = df[field_name].apply(map_duration_to_category)

    # Supprimer les colonnes catégorielles d'origine
    categorical_columns_without_category = [
        col.replace("Category", "") for col in categorical_columns
    ]
    # print(categorical_columns_without_category)

    columns_to_drop = categorical_columns_without_category + [
        "startDateTime",
        "stopDateTime",
        "sourcePayloadAsBase64",
        "sourcePayloadAsUTF",
        "destinationPayloadAsBase64",
        "destinationPayloadAsUTF",
    ]

    # Check if the columns exist before dropping them
    existing_columns = set(df.columns)
    columns_to_drop = [col for col in columns_to_drop if col in existing_columns]
    df.drop(columns=columns_to_drop, inplace=True)

    # Step 4: Perform One-Hot Encoding on Categorical Data
    df_encoded = pd.get_dummies(
        df,
        columns=[
            "appName",
            "direction",
            "protocolName",
        ].extend(categorical_columns),
    )
    return df_encoded


def indexing_enc(df_encoded, index_name):
    # Convert DataFrame to a list of dictionaries with boolean values
    flows = df_encoded.astype(bool).to_dict(orient="records")

    # Create actions for helpers.bulk()
    actions = [
        {
            "_op_type": "index",
            "_index": index_name,  # Replace with the name of your new index
            "_source": flow,
        }
        for flow in flows
    ]

    # Use helpers.bulk for efficient indexing
    success, failed = helpers.bulk(es, actions, raise_on_error=False)

    # If there are failures, print details for each failed document
    for failure in failed:
        print(f"Failed document: {failure['index']}")
        print(f"Error details: {failure['error']}")


def indexing_csv(df, csv_name):
    # Check if the CSV file already exists
    if os.path.exists(csv_name):
        # If it exists, append the DataFrame to the existing CSV
        df.to_csv(csv_name, mode="a", header=False, index=False)
    else:
        # If it doesn't exist, create a new CSV with the DataFrame
        df.to_csv(csv_name, index=False)
        print(f"Data saved to {csv_name}")


# Specify the Elasticsearch node URLs (can be a single or list of nodes)
hosts = ["http://localhost:9200"]

# Connect to Elasticsearch
es = Elasticsearch(hosts=hosts)

# Appliquez les options de transport à l'objet Elasticsearch
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Define your Elasticsearch index
data_index = "flow_data_index"

# Name of the encoded index
enc_index = "flow_data_enc"

# Check if the index already exists
index_exists = es.indices.exists(index=enc_index)

# If the index exists, find an available name with a suffix
if index_exists:
    suffix = 1
    new_enc_index = f"{enc_index}_{suffix}"

    while es.indices.exists(index=new_enc_index):
        suffix += 1
        new_enc_index = f"{enc_index}_{suffix}"
else:
    new_enc_index = enc_index

# Use the new index name for your operations
print(f"Using index: {new_enc_index}")

# Supprimez l'index existant (attention, cela supprime toutes les données de l'index)
# es.indices.delete(index=enc_index_name, ignore=[400, 404])

# Initial search request
result = es.search(index=data_index, query={"match_all": {}}, scroll="2m", size=5000)

# Continue scrolling until no more results
while len(result["hits"]["hits"]) > 0:
    # Process the current batch of results and create the DataFrame
    df = pd.DataFrame(hit["_source"] for hit in result["hits"]["hits"])

    # Process the DataFrame (replace this with your actual processing logic)
    df_enc = process_df(df)

    # Index the processed DataFrame (replace this with your actual indexing logic)
    indexing_enc(df_enc, new_enc_index)
    # indexing_csv(df_enc, "test.csv")

    # Use the scroll ID to retrieve the next batch
    result = es.scroll(scroll_id=result["_scroll_id"], scroll="2m")

# Close the scroll
es.clear_scroll(scroll_id=result["_scroll_id"])

print("Data encoding completed.")
