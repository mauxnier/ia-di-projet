from elasticsearch import Elasticsearch
import pandas as pd

# Connect to Elasticsearch
es = Elasticsearch(hosts=["http://localhost:9200"])

# Apply transport options to the Elasticsearch object
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)

# Define Elasticsearch index
index_name = "flow_data_enc_1"

query = {"match_all": {}}

result = es.search(
    index=index_name,
    query=query,
    size=10,
)

df = pd.DataFrame(hit["_source"] for hit in result["hits"]["hits"])

pd.set_option("display.max_columns", None)
print(df.head())
