# Import necessary libraries
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import ipaddress
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import classification_report

# Specify the Elasticsearch node URLs (can be a single or list of nodes)
hosts = ["http://localhost:9200"]

# Connect to Elasticsearch
es = Elasticsearch(hosts=hosts)

# Apply transport options to the Elasticsearch object
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Define Elasticsearch indices
# index_name = "flow_data_index"
index_name = "flow_data_enc"

result = es.search(
    index=index_name,
    query={"match": {"appName.keyword": "HTTPWeb"}},
    scroll="2m",
    size=10000,
)

# Initialize an empty DataFrame to store the results
df_list = []

# Continue scrolling until no more results
while len(result["hits"]["hits"]) > 0:
    # Process the current batch of results and create the DataFrame
    df_list.append(hit["_source"] for hit in result["hits"]["hits"])

    # Use the scroll ID to retrieve the next batch
    result = es.scroll(scroll_id=result["_scroll_id"], scroll="2m")

# Close the scroll
es.clear_scroll(scroll_id=result["_scroll_id"])

print("df_list.size: ", len(df_list))

# Concatenate all the results into a single DataFrame
df = pd.concat(df_list, axis=0)

# Print the DataFrame
print(df.head())

# Assuming df_encoded is your DataFrame with one-hot encoded features
# and 'is_anomalous' is your target variable
# X = df.drop("is_anomalous", axis=1)
# y = df["is_anomalous"]

# Train-Test Split
# X_train, X_test, y_train, y_test = train_test_split(
#     X, y, test_size=0.2, random_state=42
# )

# Preprocess Data (Standardization)
# scaler = StandardScaler()
# X_train_scaled = scaler.fit_transform(X_train)
# X_test_scaled = scaler.transform(X_test)

# Train k-NN Model
# knn_model = KNeighborsClassifier(n_neighs

# Predict on Test Set
# y_pred = knn_model.predict(X_test_scaled)

# Evaluate Performance
# print("\nClassification Report:")
# print(classification_report(y_test, y_pred))

print("Data encoding and k-NN completed.")
