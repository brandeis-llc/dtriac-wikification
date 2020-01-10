from elasticsearch import Elasticsearch as ES

ES_HOST = 'http://tarski.cs-i.brandeis.edu:9200'
HTTP_JSON_HEADER = {'Content-Type': 'application/json'}

es = ES(ES_HOST, timeout=30, max_retries=10, retry_on_timeout=True)
