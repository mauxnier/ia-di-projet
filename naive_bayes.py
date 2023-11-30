from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import MultinomialNB
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

# Define the features and target variable for classification for HTTPWeb
X_httpweb = df_httpweb.drop(['Tag_Attack', 'Tag_Normal'], axis=1)
y_httpweb = df_httpweb['Tag_Attack']

# Train-Test Split
X_train_httpweb, X_test_httpweb, y_train_httpweb, y_test_httpweb = train_test_split(
    X_httpweb, y_httpweb, test_size=0.2, random_state=42
)

# Preprocess Data (Standardization)
scaler = StandardScaler()
X_train_scaled_httpweb = scaler.fit_transform(X_train_httpweb)
X_test_scaled_httpweb = scaler.transform(X_test_httpweb)

# Train k-NN Model for HTTPWeb
knn_model_httpweb = KNeighborsClassifier(n_neighbors=5)
knn_model_httpweb.fit(X_train_scaled_httpweb, y_train_httpweb)
y_pred_knn_httpweb = knn_model_httpweb.predict(X_test_scaled_httpweb)
accuracy_knn_httpweb = accuracy_score(y_test_httpweb, y_pred_knn_httpweb)
classification_report_knn_httpweb = classification_report(y_test_httpweb, y_pred_knn_httpweb)

print("\nKnn Classification Report for HTTPWeb:")
print(classification_report_knn_httpweb)
print("Accuracy for HTTPWeb:", accuracy_knn_httpweb)

# Instantiate the Naive Bayes classifier for HTTPWeb
naive_bayes_model_httpweb = MultinomialNB()

# Fit the model for HTTPWeb
naive_bayes_model_httpweb.fit(X_train_scaled_httpweb, y_train_httpweb)

# Predict on Test Set for HTTPWeb
y_pred_httpweb = naive_bayes_model_httpweb.predict(X_test_scaled_httpweb)

# Evaluate Performance for HTTPWeb
accuracy_httpweb = accuracy_score(y_test_httpweb, y_pred_httpweb)
classification_report_result_httpweb = classification_report(y_test_httpweb, y_pred_httpweb)

print("\nNaive Bayes Classification Report for HTTPWeb:")
print(classification_report_result_httpweb)
print("Accuracy for HTTPWeb:", accuracy_httpweb)


# Repeat the process for SSH
# Define the features and target variable for classification for SSH
X_ssh = df_ssh.drop(['Tag_Attack', 'Tag_Normal'], axis=1)
y_ssh = df_ssh['Tag_Attack']

# Train-Test Split
X_train_ssh, X_test_ssh, y_train_ssh, y_test_ssh = train_test_split(
    X_ssh, y_ssh, test_size=0.2, random_state=42
)

# Preprocess Data (Standardization)
X_train_scaled_ssh = scaler.fit_transform(X_train_ssh)
X_test_scaled_ssh = scaler.transform(X_test_ssh)

# Instantiate the Naive Bayes classifier for SSH
naive_bayes_model_ssh = MultinomialNB()

# Fit the model for SSH
naive_bayes_model_ssh.fit(X_train_scaled_ssh, y_train_ssh)

# Predict on Test Set for SSH
y_pred_ssh = naive_bayes_model_ssh.predict(X_test_scaled_ssh)

# Evaluate Performance for SSH
accuracy_ssh = accuracy_score(y_test_ssh, y_pred_ssh)
classification_report_result_ssh = classification_report(y_test_ssh, y_pred_ssh)

print("\nNaive Bayes Classification Report for SSH:")
print(classification_report_result_ssh)
print("Accuracy for SSH:", accuracy_ssh)
