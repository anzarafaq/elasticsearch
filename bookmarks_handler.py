import json
from elasticsearch import Elasticsearch
from werkzeug.wrappers import Response


ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'bookmarks'
TYPE_NAME = 'bookmark'
#bookmarks are tied to deviceid
ID_FIELD = 'deviceid'


def add_bookmarks(request):
    deviceid = request.values.get('deviceid')
    bookmark = request.values.get('placeid')

    op_dict = {
            "index": {
                "_index": INDEX_NAME,
                "_type": TYPE_NAME,
                "_id": deviceid
                }
            }

    bookmarks = _get_bookmarks(deviceid)
    bookmarks.append(bookmark)

    bookmark_d = {}
    bookmark_d['deviceid'] = deviceid
    bookmark_d['bookmarks'] = ','.join(set(bookmarks))

    bulk_data = []
    bulk_data.append(op_dict)
    bulk_data.append(bookmark_d)

    es = Elasticsearch(hosts = [ES_HOST])
    res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
    return Response('{"Result": "SUCCESS"}')

def _get_bookmarks(deviceid):
    _query = {
            "query": {
                "bool": {
                    "must": [
                            ]
                    }
                }
            }

    _query["query"]["bool"]["must"].append(
            {"match": {"deviceid": "%s" % deviceid}})

    search_query = json.dumps(_query)

    print "-------------------------"
    print search_query
    print "-------------------------"

    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index = INDEX_NAME, size=100, body=search_query)

    resp = []
    for hit in res['hits']['hits']:
        print(hit["_source"])
        resp.append(hit["_source"]['bookmarks'])

    bookmarks = []
    if resp:
        bookmarks = resp[0].split(",")

    return bookmarks


def get_bookmarks(request):
    deviceid = request.values.get('deviceid')
    return Response(json.dumps(_get_bookmarks(deviceid)))
