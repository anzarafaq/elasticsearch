import json
import csv
import os
from datetime import datetime

from elasticsearch import Elasticsearch
from werkzeug.wrappers import Response
from bookmarks_handler import add_bookmarks, get_bookmarks

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'place'
TYPE_NAME = 'place'
ID_FIELD = 'placeid'


def get_two_weeks_events(lat, lon, radius=80):
    events = []

    _query = {
            "query" : {
                        "bool" : {
                            "must" : [
                                {"range" : {
                                    "start_date" : {
                                        "lte" : "now+2w",
                                        }
                                    }
                                    },
                                {"range" : {
                                    "end_date" : {
                                        "gte" : "now",
                                        }
                                    }
                                    }

                                ]
                            }
                }
            }

    if lat and lon:
        _query = {
                "query" : {
                            "bool" : {
                                "must" : [
                                    {"range" : {
                                        "start_date" : {
                                            "lte" : "now+2w",
                                            }
                                        }
                                        },
                                    {"range" : {
                                        "end_date" : {
                                            "gte" : "now",
                                            }
                                        }
                                        }

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

    search_query = json.dumps(_query)
    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index="event", size=30, body=search_query)

    for hit in res['hits']['hits']:
        events.append(hit["_source"])

    return events


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

def category_name_filter_v2(request):
    raw = open('data/category_name_filter/catfilter_v2.json', 'r').read()
    cat_filter = json.loads(raw)
    return Response(json.dumps(cat_filter))


def _make_collections(lat, lon):
    _collections = []
    tmp_collections = []
    reader = csv.reader(open('data/collections/collections.csv', 'rUb'))
    count = 0
    for row in reader:
        if count == 0:
            count += 1
            continue
        coll = {}
        coll['id'] = row[0]
        coll['f_date'] = datetime.strptime(row[1], '%m/%d/%y')
        coll['date'] = row[1]
        coll['title'] = row[2]
        coll['description'] = row[3]
        coll['places'] = []

        place_ids = []
        for item in row[4:]:
            if item.strip() not in ('', None):
                place_ids.append(item)

        query_string = 'placeid:' + ','.join(place_ids)
        search = _search(lat, lon, radius=200, filter_by=query_string)
        coll['places'] = search
        _collections.append(coll)

    _collections.sort(key=lambda r: r['f_date'])
    _collections.reverse()
    for dic in _collections:
        del dic['f_date']

    return _collections


def collections_v2(request):
    lat = request.values.get('lat')
    lon = request.values.get('lon')

    collections = _make_collections(lat, lon)

    ##adding events
    ev_coll = {}
    ev_coll['id'] = 'TWOWEEKSEVENTS'
    ev_coll['events'] = get_two_weeks_events(lat, lon)
    ev_coll['title'] = 'Our favorite events'
    ev_coll['description'] = 'From festivals to art, science to music, here are our top picks for the next two weeks'
    collections.append(ev_coll)

    return Response(json.dumps(collections))


def collections(request):
    _collections = []
    lat = request.values.get('lat')
    lon = request.values.get('lon')

    collections = _make_collections(lat, lon)
    return Response(json.dumps(collections))


def search(request):
    lat = request.values.get('lat')
    lon = request.values.get('lon')
    radius = request.values.get('radius', "80")
    keywords = request.values.get('keywords', '')
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

    if filter_by not in (None, ''):
        for filt in filter_by.split("|"):
            fbk, fbv = filt.split(':')
            if fbk and fbv:
                ###ASSUMING THAT only ONE category will ever be searched on
                ###If more categories are passed in one shot, this will NOT WORK
                if fbk == 'category':
                    _query["query"]["bool"]["must"].append(
                            {"match_phrase": {"%s" % fbk: "%s" % fbv}})
                elif fbk == 'Playground_Material':
                    if keywords not in ('', None):
                        keywords += ','
                    keywords += 'Playground_Material=|%s' % fbv
                    continue
                else:
                    _query["query"]["bool"]["must"].append({"match": {"%s" % fbk: "%s" % fbv}})

    if keywords:
        keywords = keywords.split(',')
        nested = {"nested": {
            "path": "otherdata","query": {"bool": {"must": []}}}
        }
        for keyword in keywords:
            if keyword.find('=|') != -1:
                ky, vl = keyword.split('=|')
                nested["nested"]["query"]["bool"]["must"].append(
                    {"match": {"otherdata.%s" % ky : vl}})
            else:
                nested["nested"]["query"]["bool"]["must"].append(
                    {"match": {"otherdata.%s" % keyword.strip() : "Y"}})
        _query["query"]["bool"]["must"].append(nested)

    search_query = json.dumps(_query)
    print search_query
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


def events(request):
    filter_by = request.values.get('filter_by')
    keywords = request.values.get('keywords')
    type_data = request.values.get('type_data')
    limit = int(request.values.get('limit', '100'))
    lat = request.values.get('lat')
    lon = request.values.get('lon')
    radius = request.values.get('radius', "80")

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

    if filter_by not in (None, ''):
        for filt in filter_by.split("|"):
            fbk, fbv = filt.split(':')
            if fbk and fbv:
                _query["query"]["bool"]["must"].append({"match": {"%s" % fbk: "%s" % fbv}})

    if keywords:
        keywords = keywords.split(',')
        nested = {"nested": {
            "path": "keywords","query": {"bool": {"must": []}}}
        }
        for keyword in keywords:
                nested["nested"]["query"]["bool"]["must"].append(
                    {"match": {"keywords.%s" % keyword.strip() : "Y"}})
        _query["query"]["bool"]["must"].append(nested)


    if type_data:
        type_data = type_data.split(',')
        nested = {"nested": {
            "path": "type_data","query": {"bool": {"must": []}}}
        }
        for keyword in type_data:
                nested["nested"]["query"]["bool"]["must"].append(
                    {"match": {"type_data.%s" % keyword.strip() : "Y"}})
        _query["query"]["bool"]["must"].append(nested)
    print "************************************************"
    print _query
    print "************************************************"

    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index = 'event', size=limit, body=_query)
    print(" response: '%s'" % (res))
    resp = []
    for hit in res['hits']['hits']:
        result = hit["_source"]
        resp.append(result)

    return Response(json.dumps(resp))
