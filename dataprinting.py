from elasticsearch import Elasticsearch

es = Elasticsearch(hosts=["http://localhost:9200"])
es.options(request_timeout=60, max_retries=5, retry_on_timeout=True)
index_name = "flow_data_enc"

result = es.search(
    index=index_name,
    query={"match_all": {}},
    scroll="2m",
    size=1,
)

print("result: ", result)