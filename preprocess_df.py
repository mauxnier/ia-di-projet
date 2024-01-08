import xml.etree.ElementTree as ET
import pandas as pd
import ipaddress
import pickle

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)


def preprocess_data(df):
    # Perform conversions on the data
    columns_to_convert = [
        "sourcePort",
        "destinationPort",
        "totalSourceBytes",
        "totalDestinationBytes",
        "totalSourcePackets",
        "totalDestinationPackets",
    ]
    for column in columns_to_convert:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # Convert startDateTime and stopDateTime into datetime objects and calculate duration
    df["startDateTime"] = pd.to_datetime(df["startDateTime"])
    df["stopDateTime"] = pd.to_datetime(df["stopDateTime"])
    df["duration"] = df["stopDateTime"] - df["startDateTime"]
    df["duration"] = df["duration"].apply(lambda x: int(x.total_seconds()))

    # Perform encoding on Categorical Data
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

    # Extract year, month, day from startDateTime and stopDateTime
    # df["start_year"] = df["startDateTime"].dt.year
    # df["stop_year"] = df["stopDateTime"].dt.year
    # df["start_month"] = df["startDateTime"].dt.month
    # df["stop_month"] = df["stopDateTime"].dt.month
    # df["start_day"] = df["startDateTime"].dt.day
    # df["stop_day"] = df["stopDateTime"].dt.day

    # Perform One-Hot Encoding on Categorical Data
    columns_to_encode = ["direction", "protocolName"]
    columns_to_encode.extend(categorical_columns)
    df = pd.get_dummies(df, columns=columns_to_encode)

    # Modify the tag
    df["tag_Attack"] = df["Tag"].apply(lambda x: 1 if x == "Attack" else 0)

    # Modify sourceTCPFlagsDescription and destinationTCPFlagsDescription
    unique_flags = ["F", "S", "R", "P", "A", "N/A"]
    for flag in unique_flags:
        df[f"sourceTCPFlag_{flag}"] = df["sourceTCPFlagsDescription"].apply(
            lambda x: int(flag in x.split(",")) if pd.notna(x) else 0
        )
        df[f"destinationTCPFlag_{flag}"] = df["destinationTCPFlagsDescription"].apply(
            lambda x: int(flag in x.split(",")) if pd.notna(x) else 0
        )

    # Drop unnecessary columns
    columns_to_drop = [
        "source",
        "destination",
        "sourcePort",
        "destinationPort",
        "totalSourceBytes",
        "totalDestinationBytes",
        "totalSourcePackets",
        "totalDestinationPackets",
        # "duration",
        "startDateTime",
        "stopDateTime",
        "sourcePayloadAsBase64",
        "sourcePayloadAsUTF",
        "destinationPayloadAsBase64",
        "destinationPayloadAsUTF",
        "sourceTCPFlagsDescription",
        "destinationTCPFlagsDescription",
        "Tag",
        "sensorInterfaceId",
        "startTime",
    ]
    df.drop(columns=columns_to_drop, inplace=True, errors="ignore")

    return df


# Récupération des données
df = pd.read_pickle("data/flow_df.pkl")

# Split the dataframe into two based on the "protocolName" column
df_HTTPWeb = df[df["appName"] == "HTTPWeb"]
df_SSH = df[df["appName"] == "SSH"]

# # Print the shapes of the resulting dataframes
print(f"Shape of HTTPWeb dataframe: {df_HTTPWeb.shape}")
print(f"Shape of SSH dataframe: {df_SSH.shape}")

print(df_HTTPWeb.head())

# Parsing des fichiers XML
df_preprocessed = preprocess_data(df)
print(df_preprocessed.head())

# Sauvegarder le DataFrame avec les features manquantes
with open("data/df_preprocessed.pkl", "wb") as file:
    pickle.dump(df_preprocessed, file)
