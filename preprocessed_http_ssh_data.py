from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, accuracy_score
# Specify the Elasticsearch node URLs (can be a single or list of nodes)
hosts = ["http://localhost:9200"]

# Connect to Elasticsearch
es = Elasticsearch(hosts=hosts)

# Apply transport options to the Elasticsearch object
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Define Elasticsearch index
index_name = "flow_data_enc"

# Define the query for HTTPWeb
query_httpweb = {
    "query": {
        "term": {"appName_HTTPWeb": True}
    }
}

# Define the query for SSH
query_ssh = {
    "query": {
        "term": {"appName_SSH": True}
    }
}

# Execute the queries
response_httpweb = es.search(index=index_name, body=query_httpweb)
response_ssh = es.search(index=index_name, body=query_ssh)

# Extract the hits from the responses
hits_httpweb = response_httpweb["hits"]["hits"]
hits_ssh = response_ssh["hits"]["hits"]

# Convert the hits to a DataFrame (if needed)
df_httpweb = pd.DataFrame(hit["_source"] for hit in hits_httpweb)
df_ssh = pd.DataFrame(hit["_source"] for hit in hits_ssh)

# Set display options to show all rows and columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

# Print or use the filtered DataFrames as needed
print("Filtered Data for HTTPWeb:")
print(df_httpweb)

print("Filtered Data for SSH:")
print(df_ssh)

# Define the features and target variable for classification
X_httpweb = df_httpweb.drop(['Tag_Attack', 'Tag_Normal'], axis=1)
y_httpweb = df_httpweb['Tag_Attack']

X_ssh = df_ssh.drop(['Tag_Attack', 'Tag_Normal'], axis=1)
y_ssh = df_ssh['Tag_Attack']

# Train-Test Split
X_train, X_test, y_train, y_test = train_test_split(
    X_httpweb, y_httpweb, test_size=0.2, random_state=42
)

# Preprocess Data (Standardization)
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Train k-NN Model
knn_model = KNeighborsClassifier(n_neighbors=5)  # You can adjust the number of neighbors

# Fit the model
knn_model.fit(X_train_scaled, y_train)

# Predict on Test Set
y_pred = knn_model.predict(X_test_scaled)

# Evaluate Performance
accuracy = accuracy_score(y_test, y_pred)
classification_report_result = classification_report(y_test, y_pred)

print("\n Knn Classification Report:")
print(classification_report_result)
print("Accuracy:", accuracy)


