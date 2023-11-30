from elasticsearch import Elasticsearch
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import MultinomialNB
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, accuracy_score

# Connect to Elasticsearch
es = Elasticsearch(hosts=["http://localhost:9200"])
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)
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
