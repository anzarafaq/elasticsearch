import json

from werkzeug.wrappers import Request, Response

from elasticsearch import Elasticsearch

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'place'
TYPE_NAME = 'place'
ID_FIELD = 'placeid'


@Request.application
def application(request):
	if request.url.endswith('favicon.ico'):
		return Response('')

	lat = request.values.get('lat')
	lon = request.values.get('lon')

	search_query = """{
	  "query": {
	    "filtered" : {
	        "query" : {
	            "match_all" : {}
	        },
	        "filter" : {
	            "geo_distance" : {
	                "distance" : "20km",
	                "location" : {
	                    "lat" : %s,
	                    "lon" : %s
	                }
	            }
	        }
	    }
	  }
	}""" %(lat, lon)
	print search_query

	es = Elasticsearch(hosts = [ES_HOST])
	res = es.search(index = INDEX_NAME, size=10, body=search_query)
	print "----------------------------------------------------"
	print res

	resp = []
	for hit in res['hits']['hits']:
    		print(hit["_source"])
		resp.append(hit["_source"])
	return Response(json.dumps(resp))

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    run_simple('localhost', 4000, application)

