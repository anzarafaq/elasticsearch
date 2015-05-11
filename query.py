from elasticsearch import Elasticsearch

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'place'
TYPE_NAME = 'place'
ID_FIELD = 'placeid'


# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST])

# Query by Geo location (lat, lon)
###_search_query = {
###		"sort" : [
###		{
###			"_geo_distance" : {
###				"location" : {
###					"lat": 38.9174,
###					"lon": -122.3050
###				}, 
###				"order" : "asc",
###				"unit" : "km"
###			}
###		}
###		],
###			"query": {
###				"filtered" : {
###					"query" : {
###						"match_all" : {}
###					},
###					"filter" : {
###						"geo_distance" : {
###							"distance" : "50km",
###							"location" : {
###								"lat" : 38.9174,
###								"lon" : -122.3050
###							}
###						}
###					}
###				}
###			}
###	}
###
###
###print _search_query

_search_query = {
  "query": {
    "filtered" : {
        "query" : {
            "match_all" : {}
        },
    }
  }
}

res = es.search(index = INDEX_NAME, size=100, body=_search_query)
print(" response: '%s'" % (res))
print("results:")

for hit in res['hits']['hits']:
    print(hit["_source"])

###Term query (search by zip code)
###_search_query = {
###"query": {
###  "multi_match" : {
###    "query" : "CA",
###    "fields" : [ "state" ]
###  }
###}
###}
###
###res = es.search(index = INDEX_NAME, body=_search_query)
###print(" response: '%s'" % (res))
###print("results:")
###for hit in res['hits']['hits']:
###    print(hit["_source"])
###
