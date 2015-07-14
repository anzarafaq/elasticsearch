import json
import csv

from elasticsearch import Elasticsearch
from werkzeug.wrappers import Response
from bookmarks_handler import add_bookmarks, get_bookmarks


ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'place'
TYPE_NAME = 'place'
ID_FIELD = 'placeid'

_collections = {}


def get_place_info(filter_by):
    _query = {
            "query": {
                "bool": {
                    "must": [
                            ]
                    }
                }
            }

    _query["query"]["bool"]["must"].append({ "match": { "placeid": "%s" % filter_by}})
    search_query = json.dumps(_query)

    print "-------------------------"
    print search_query
    print "-------------------------"

    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index = INDEX_NAME, size=10, body=search_query)

    resp = []
    for hit in res['hits']['hits']:
        print(hit["_source"])
        resp.append(hit["_source"])

    return resp


def welcome(request):
    return Response("Snugabug!")


def bookmarks(request):
    if request.method == 'POST':
        return add_bookmarks(request)
    elif request.method == 'GET':
        return get_bookmarks(request)

def collections(request):
    if _collections:
        print "From cache"
        return Response(json.dumps(_collections))

    reader = csv.reader(open('data/collections/collections.csv', 'rUb'))
    count = 0
    place_ids = []
    for row in reader:
        if count == 0:
            count += 1
            continue
        cid = row[0]
        _collections[cid] = {}
        _collections[cid]['title'] = row[1]
        _collections[cid]['description'] = row[2]
        _collections[cid]['places'] = []

        for item in row[3:]:
            if item.strip() not in ('', None):
                _collections[cid]['places'].append(get_place_info(item))
    return Response(json.dumps(_collections))


def search(request):
    lat = request.values.get('lat')
    lon = request.values.get('lon')

    _query = {
            "query": {
                "bool": {
                    "must": [
                        ]
                    }
                }
            }


    if lat and lon:
        _query["query"]["bool"]["must"].append({
                            "filtered" : {
                                "query" : {
                                    "match_all" : {}
                                    },
                                "filter" : {
                                    "geo_distance" : {
                                        "distance" : "2km",
                                        "location" : {
                                            "lat" : lat,
                                            "lon" : lon
                                            }}}}
                                        })

    keywords = request.values.get('keywords', None)
    if keywords:
        keywords = keywords.split(',')
        nested = { "nested": {
            "path": "otherdata","query": {"bool": {"must": []}}}
        }
        for keyword in keywords:
            nested["nested"]["query"]["bool"]["must"].append(
                    {"match": {"otherdata.%s" % keyword.strip() : "Y"}})
        _query["query"]["bool"]["must"].append(nested)

    filter_by = request.values.get('filter_by')
    if filter_by:
        filter_by = filter_by.split(',')
        for fb in filter_by:
            fbk, fbv = fb.split(':')
            _query["query"]["bool"]["must"].append({ "match": { "%s" % fbk: "%s" % fbv }})
    search_query = json.dumps(_query)

    print "-------------------------"
    print search_query
    print "-------------------------"

    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index = INDEX_NAME, size=50, body=search_query)
    print "----------------------------------------------------"
    print res
    print "----------------------------------------------------"

    resp = []
    for hit in res['hits']['hits']:
        print(hit["_source"])
        resp.append(hit["_source"])
    return Response(json.dumps(resp))
