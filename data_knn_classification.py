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

# query = {"match_all": {}}

query = {
    "bool": {
        "should": [
            {"term": {"appName_HTTPWeb": 1}},
            {"term": {"appName_SSH": 1}},
        ],
        "minimum_should_match": 1,
    }
}

result = es.search(
    index=index_name,
    query=query,
    scroll="2m",
    size=10000,
)

# Initialize an empty DataFrame to store the results
df_list = []

# Continue scrolling until no more results
while len(result["hits"]["hits"]) > 0:
    # Process the current batch of results and create the DataFrame
    df = pd.DataFrame(hit["_source"] for hit in result["hits"]["hits"])
    df_list.append(df)

    # Use the scroll ID to retrieve the next batch
    result = es.scroll(scroll_id=result["_scroll_id"], scroll="2m")

# Close the scroll
es.clear_scroll(scroll_id=result["_scroll_id"])

print("df_list.size x 10.000: ", len(df_list))

# Concatenate all the results into a single DataFrame
df = pd.concat(df_list, axis=0)

# # Print the DataFrame
# # pd.set_option("display.max_columns", None)
# # pd.set_option("display.max_rows", None)
# print(df.head())
print("Dataframe shape: ", df.shape)

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