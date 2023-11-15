# Import necessary libraries
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import ipaddress
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import classification_report

# Set display options for pandas
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

# Function to process DataFrame
def process_df(df):
    # Step 2: Perform conversions on the data

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

    # ... (Previous code)

    # Step 4: Perform One-Hot Encoding on Categorical Data
    categorical_columns = [
        "appName",
        "direction",
        "protocolName",
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

    df_encoded = pd.get_dummies(
        df,
        columns=[
            "appName",
            "direction",
            "protocolName",
        ].extend(categorical_columns),
    )
    return df_encoded


# Function to index encoded DataFrame into Elasticsearch
def indexing_enc(df_encoded, batch_num):
    # Convert DataFrame to a list of dictionaries
    flows = df_encoded.to_dict(orient="records")

    # Create actions for helpers.bulk()
    actions = [
        {
            "_op_type": "index",
            "_index": enc_index_name,  # Replace with the name of your new index
            "_source": flow,
        }
        for flow in flows
    ]

    # Use helpers.bulk for efficient indexing
    success, failed = helpers.bulk(es, actions, raise_on_error=False)

    if success:
        print(f"Batch {batch_num + 1}: Success in indexing: {success}")

    if failed:
        print(f"Batch {batch_num + 1}: Failures in indexing: {failed}")

    # If there are failures, print details for each failed document
    for failure in failed:
        print(f"Failed document: {failure['index']}")
        print(f"Error details: {failure['error']}")


# Specify the Elasticsearch node URLs (can be a single or list of nodes)
hosts = ["http://localhost:9200"]

# Connect to Elasticsearch
es = Elasticsearch(hosts=hosts)

# Apply transport options to the Elasticsearch object
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Define Elasticsearch indices
index_name = "flow_data_index"
enc_index_name = "flow_data_enc"

# Delete the existing index (careful, this deletes all data in the index)
es.indices.delete(index=enc_index_name, ignore=[400, 404])

# Initial search request
result = es.search(index=index_name, query={"match_all": {}}, scroll="2m", size=10000)

# Initialize a counter for batches
batch_counter = 1

# Continue scrolling until no more results
while len(result["hits"]["hits"]) > 0:
    # Process the current batch of results and create the DataFrame
    df = pd.DataFrame(hit["_source"] for hit in result["hits"]["hits"])

    # Process the DataFrame (replace this with your actual processing logic)
    df_enc = process_df(df)

    # Index the processed DataFrame (replace this with your actual indexing logic)
    indexing_enc(df_enc, batch_counter)

    # Increment the batch counter
    batch_counter += 1
    print(f"Batch {batch_counter} completed.")

    # Use the scroll ID to retrieve the next batch
    result = es.scroll(scroll_id=result["_scroll_id"], scroll="2m")

# Close the scroll
es.clear_scroll(scroll_id=result["_scroll_id"])

# Assuming df_encoded is your DataFrame with one-hot encoded features
# and 'is_anomalous' is your target variable
X = df_encoded.drop('is_anomalous', axis=1)
y = df_encoded['is_anomalous']

# Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Preprocess Data (Standardization)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Train k-NN Model
knn_model = KNeighborsClassifier(n_neighbors=5)
knn_model.fit(X_train_scaled, y_train)

# Predict on Test Set
y_pred = knn_model.predict(X_test_scaled)

# Evaluate Performance
print("\nClassification Report:")
print(classification_report(y_test, y_pred))

print("Data encoding and k-NN completed.")
