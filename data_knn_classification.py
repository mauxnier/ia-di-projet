from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
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

print("Length of http web data",len(df_httpweb))
print("Length of http web data",len(df_ssh))

# Define the features and target variable for classification
X_httpweb = df_httpweb.drop(['Tag_Attack', 'Tag_Normal'], axis=1)
y_httpweb = df_httpweb['Tag_Attack']

X_ssh = df_ssh.drop(['Tag_Attack', 'Tag_Normal'], axis=1)
y_ssh = df_ssh['Tag_Attack']

# Train-Test Split for SSH
X_train_ssh, X_test_ssh, y_train_ssh, y_test_ssh = train_test_split(
    X_ssh, y_ssh, test_size=0.2, random_state=42
)

# Impute missing values
imputer = SimpleImputer(strategy='mean')  # You can choose other strategies as well
X_train_ssh_imputed = imputer.fit_transform(X_train_ssh)
X_test_ssh_imputed = imputer.transform(X_test_ssh)

# Preprocess Data (Standardization)
scaler_ssh = StandardScaler()
X_train_scaled_ssh = scaler_ssh.fit_transform(X_train_ssh_imputed)
X_test_scaled_ssh = scaler_ssh.transform(X_test_ssh_imputed)

# Train k-NN Model for SSH
knn_model_ssh = KNeighborsClassifier(n_neighbors=5)
knn_model_ssh.fit(X_train_scaled_ssh, y_train_ssh)
y_pred_ssh = knn_model_ssh.predict(X_test_scaled_ssh)

# Evaluate Performance for SSH
accuracy_ssh = accuracy_score(y_test_ssh, y_pred_ssh)
classification_report_result_ssh = classification_report(
    y_test_ssh, y_pred_ssh, zero_division=1  # Set zero_division to 1
)

print("\nKnn Classification Report for SSH:")
print(classification_report_result_ssh)
print("Accuracy for SSH:", accuracy_ssh)