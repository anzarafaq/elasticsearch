import json
import csv
import os
#from bson import json_util
from datetime import datetime

from elasticsearch import Elasticsearch
from werkzeug.wrappers import Response
from bookmarks_handler import add_bookmarks, get_bookmarks

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'place'
TYPE_NAME = 'place'
ID_FIELD = 'placeid'

_collections = []


def get_place_info(filter_by):
    _query = {
            "query": {
                "bool": {
                    "must": [
                            ]
                    }
                }
            }

    _query["query"]["bool"]["must"].append({"match": {"placeid": "%s" % filter_by}})

    search_query = json.dumps(_query)
    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index=INDEX_NAME, size=10, body=search_query)

    resp = []
    for hit in res['hits']['hits']:
        resp.append(hit["_source"])

    return resp


def welcome(request):
    return Response("Snugabug!")


def bookmarks(request):
    if request.method == 'POST':
        return add_bookmarks(request)
    elif request.method == 'GET':
        return get_bookmarks(request)


def category_name_filter(request):
    raw = open('data/category_name_filter/catfilter.json', 'r').read()
    cat_filter = json.loads(raw)
    return Response(json.dumps(cat_filter))


def collections(request):
    lat = request.values.get('lat')
    lon = request.values.get('lon')

    global _collections
    if _collections:
        print "From cache"
        return Response(json.dumps(_collections))

    tmp_collections = []
    reader = csv.reader(open('data/collections/collections.csv', 'rUb'))
    count = 0
    for row in reader:
        if count == 0:
            count += 1
            continue
        coll = {}
        coll['f_date'] = datetime.strptime(row[1], '%m/%d/%Y')
        coll[row[0]] = {}
        coll[row[0]]['date'] = row[1]
        coll[row[0]]['title'] = row[2]
        coll[row[0]]['description'] = row[3]
        coll[row[0]]['places'] = []

        place_ids = []
        for item in row[4:]:
            if item.strip() not in ('', None):
                place_ids.append(item)

        query_string = 'placeid:' + ','.join(place_ids)
        search = _search(lat, lon, filter_by=query_string)
        coll[row[0]]['places'] = search
        _collections.append(coll)

    _collections.sort(key=lambda r: r['f_date'])
    for dic in _collections:
        del dic['f_date']

    return Response(json.dumps(_collections))#, default=json_util.default))


def search(request):
    lat = request.values.get('lat')
    lon = request.values.get('lon')
    radius = request.values.get('radius', "80")
    keywords = request.values.get('keywords', None)
    filter_by = request.values.get('filter_by')

    resp = _search(lat, lon, radius, keywords, filter_by)
    return Response(json.dumps(resp))

def _search(lat=None, lon=None, radius=80, keywords=None, filter_by=None):
    _query = {
            "query": {
                "bool": {
                    "must": [
                        ]
                    }
                }
            }

    if lat and lon:
        _query = {
                "query": {
                    "bool": {
                        "must": [
                            ]
                        }
                    },
                "sort": [
                    {
                        "_geo_distance": {
                            "location": {
                                "lat":  lat,
                                "lon": lon
                                },
                            "order":         "asc",
                            "unit":          "km",
                            "distance_type": "plane"
                            }
                        }
                    ],
                "fields": [
                    "_source",
                    ],
                "script_fields": {
                    "distance": {
                        "params": {
                            "llat": float(lat),
                            "llon": float(lon)
                            },
                        "script": "doc[\u0027location\u0027].distanceInKm(llat,llon)"
                        }
                    }
                }

        _query["query"]["bool"]["must"].append({
            "filtered" : {
                "query" : {
                    "match_all" : {}
                    },
                "filter" : {
                    "geo_distance" : {
                        "distance" : "%skm" % radius,
                        "location" : {
                            "lat" : lat,
                            "lon" : lon
                            }}}}
                        })

    if keywords:
        keywords = keywords.split(',')
        nested = { "nested": {
            "path": "otherdata","query": {"bool": {"must": []}}}
        }
        for keyword in keywords:
            nested["nested"]["query"]["bool"]["must"].append(
                    {"match": {"otherdata.%s" % keyword.strip() : "Y"}})
        _query["query"]["bool"]["must"].append(nested)

    if filter_by:
        fbk, fbv = filter_by.split(':')
        if fbk and fbv:
            _query["query"]["bool"]["must"].append(
                    {"match": {"%s" % fbk: "%s" % fbv}})

    search_query = json.dumps(_query)
    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index = INDEX_NAME, size=50, body=search_query)

    resp = []
    for hit in res['hits']['hits']:
        result = hit["_source"]
        result["distance"] = ''
        if lat and lon:
            result["distance"] = hit.get('fields').get('distance')[0] * 0.621371 ## Miles
        resp.append(result)
    return resp
