"""
Project: AD4IDS - Anomaly Detection for Intrusion Detection Systems
Subproject: 1 - Flow Classification
Stage: 4 - Data preprocessing (one-hot encoding)
Authors: MONNIER Killian & BAKKARI Ikrame
Date: 10/2023
"""

from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.preprocessing import OneHotEncoder
import ipaddress

# Specify the Elasticsearch node URLs (can be a single or list of nodes)
hosts = ["http://localhost:9200"]

# Connect to Elasticsearch
es = Elasticsearch(hosts=hosts)

# Define your Elasticsearch index
index_name = "flow_data_index"

# Step 1: Retrieve Data from Elasticsearch
# query = {"query": {"match_all": {}}}  # You can customize this query based on your needs
query = {
    "query": {"match_all": {}},
    "size": 5,  # Set the size to the number of flows you want to retrieve
}

result = es.search(
    index=index_name,
    query={"match_all": {}},
    size=5,
)
hits = result["hits"]["hits"]

# Convert Elasticsearch results to a pandas DataFrame
df = pd.DataFrame([hit["_source"] for hit in hits])

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

# Step 2 : Perform conversions on the data

columns_to_convert = [
    # "source",
    # "destination",
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
            df[field_name], bins=bytes_bins, labels=bytes_labels, include_lowest=True
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

# Créer un dictionnaire pour stocker les résultats encodés
encoded_data = {}

# Créer et appliquer l'encodeur pour chaque colonne catégorielle
for column in categorical_columns:
    encoder = OneHotEncoder(sparse=False, drop="first")
    encoded_data[column] = encoder.fit_transform(df[[column]])

# Convertir les données encodées en DataFrames pandas
encoded_dfs = {
    column: pd.DataFrame(
        encoded_values,
        columns=[f"{column}_{i}" for i in range(encoded_values.shape[1])],
    )
    for column, encoded_values in encoded_data.items()
}

# Concaténer les DataFrames encodés avec le DataFrame d'origine
df = pd.concat([df, *encoded_dfs.values()], axis=1)

# Supprimer les colonnes catégorielles d'origine
categorical_columns_without_category = [
    col.replace("Category", "") for col in categorical_columns
]
print(categorical_columns_without_category)
df.drop(
    columns=[
        "startDateTime",
        "stopDateTime",
    ].extend(categorical_columns_without_category),
    inplace=True,
)

# Step 4: Perform One-Hot Encoding on Categorical Data
df_encoded = pd.get_dummies(
    df,
    columns=[
        "appName",
        "direction",
        "protocolName",
    ].extend(categorical_columns),
    drop_first=True,
)

# Last step: Store Encoded Data Back to Elasticsearch
# for _, row in df_encoded.iterrows():
#     doc = row.to_dict()
#     es.index(index=index_name+"_enc", body=doc)

print("Data encoding and reindexing completed.")

print(df_encoded)
