from elasticsearch import Elasticsearch

es = Elasticsearch(['http://10.65.202.168'])

query={"query" : {"match_all" : {}}}

"""
response= es.search(index="products", doc_type="product", body=query)
for hit in response["hits"]["hits"]:
  print hit
"""

scanResp= es.search(index="products", doc_type="product", body=query, search_type="scan", scroll="10m")  
scrollId= scanResp['_scroll_id']
print scrollId

#response['hits']['hits'][0]['_source']['epid']

response=es.scroll(scroll_id=scrollId, scroll= "10m")
print response
response=es.scroll(scroll_id=scrollId, scroll= "10m")
print response

"""
from elasticsearch.helpers import scan
scanResp=scan(client=es, query=query, scroll= "10m", index="products", doc_type="product", timeout="10m")

for resp in scanResp:
    print resp
"""

