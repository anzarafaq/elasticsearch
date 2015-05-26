import json

from elasticsearch import Elasticsearch
from werkzeug.wrappers import Response

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'place'
TYPE_NAME = 'place'
ID_FIELD = 'placeid'


def welcome(request):
    return Response("Snugabug!")


def search(request):
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
