from elasticsearch import Elasticsearch
es = Elasticsearch('https://localhost:9200', verify_certs=True, basic_auth=('yana', '5ysfuGptvh'), max_retries=20)
print('Success' if es.ping()==True else 'Fail')