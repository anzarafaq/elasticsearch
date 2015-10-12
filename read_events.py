import os
import csv
import json
import time

from elasticsearch import Elasticsearch
from aws_images import get_image_locations

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'event'
TYPE_NAME = 'event'
ID_FIELD = 'eventid'

IMAGE_LOCATIONS = get_image_locations()


def find_images(placeid):
        return IMAGE_LOCATIONS.get(placeid, [])


def setup_index(es):
    request_body = {
            "settings" : {
                "number_of_shards": 1,
                "number_of_replicas": 0
                },
            "mappings": {
                "event": {
                    "properties": {
                        "eventid": {"type": "string"},
                        "category": {"type": "string"},
                        "subcategory": {"type": "string"},
                        "placeid": {"type": "string"},
                        "location": {"type": "geo_point"},
                        "event_type": {"type": "string"},
                        "website": {"type": "string"},
                        "description": {"type": "string"},
                        "start_date": {"type": "date", "format": "basic_date"},
                        "end_date": {"type": "date", "format": "basic_date"},
                        "event_name": {"type": "string"},
                        "start_time": {"type": "string"},
                        "end_Time": {"type": "string"},
                        "series_id": {"type": "string"},
                        "cost": {"type": "string"},
			"images": {"type": "object", "enabled" : False},
                        "keywords": {"type": "nested", "enabled" : True},
                        "type_data": {"type": "nested", "enabled" : True}
                        }
                    }
                }
            }
    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index = INDEX_NAME, body = request_body)
    print(" response: '%s'" % (res))


def delete_index(es):
    try:
        for index_name in ['event', '']:#, 'bookmarks'):
            if es.indices.exists(index_name):
                print("deleting '%s' index..." % (index_name))
                res = es.indices.delete(index = index_name)
                print(" response: '%s'" % (res))
    except Exception, ex:
        print ex


def get_event_location(placeid):
    _query = {
            "query": {
                "bool": {
                    "must": [
                            ]
                    }
                }
            }

    _query["query"]["bool"]["must"].append({"match": {"placeid": "%s" % placeid}})

    search_query = json.dumps(_query)
    es = Elasticsearch(hosts = [ES_HOST])
    res = es.search(index="place", size=10, body=search_query)

    resp = []
    for hit in res['hits']['hits']:
        resp.append(hit["_source"])

    lat=-1.0
    lon=-1.0

    if resp and len(resp) >= 1:
        try:
            lat = resp[0].get('location').get('lat')
            lon = resp[0].get('location').get('lon')
        except:
            print "failed to get lat, lon"

    return lat, lon


def format_date(indate):
    return "20%s%s%s" %(indate[6:8], indate[:2], indate[3:5])


if __name__ == '__main__':

    es = Elasticsearch(hosts = [ES_HOST])
    delete_index(es)
    setup_index(es)

    bulk_data = []
    afile = 'data/events/Events.csv'
    with open(afile, 'Ub') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                event = {}
                event['eventid'] = row['Event ID']
                event['category'] = row['Category']
                event['subcategory'] = row['Subcategory']
                event['placeid'] = row['Place ID']
                event['end_time'] = row['End_time']
                event['type'] = row['Type']
                event['website'] = row['Website']
                event['description'] = row['Description'].decode('utf-8', errors='ignore')
                event['start_date'] = format_date(row['Start_date'])
                event['end_date'] = format_date(row['End_date'])
                event['event_name'] = row['Event_name']
                event['start_time'] = row['Start_time']
                event['series_id'] = row['Series_ID']
                event['cost'] = row['Cost'].decode('utf-8', errors='ignore')

                event['type_data'] = {}
                event['type_data']['type_outdoor'] = row['Type_outdoor']
                event['type_data']['type_performingarts'] = row['Type_performingarts']
                event['type_data']['type_seasonal'] = row['Type_seasonal']
                event['type_data']['type_other'] = row['Type_Other']
                event['type_data']['type_transportation'] = row['Type_transportation']
                event['type_data']['type_music'] = row['Type_music']
                event['type_data']['type_fairsandfestivals'] = row['Type_fairsandfestivals']
                event['type_data']['type_food'] = row['Type_food']
                event['type_data']['type_sports'] = row['Type_sports']
                event['type_data']['type_storytime'] = row['Type_storytime']
                event['type_data']['type_cultural'] = row['Type_cultural']
                event['type_data']['type_animals'] = row['Type_animals']
                event['type_data']['type_science'] = row['Type_science']
                event['type_data']['type_community'] = row['Type_community']
                event['type_data']['type_artsandcrafts'] = row['Type_artsandcrafts']
                event['type_data']['type_museums'] = row['Type_museums']

                event['keywords'] = {}
                event['keywords']['school_aged'] = row['School_aged']
                event['keywords']['paid'] = row['Paid']
                event['keywords']['parent_required'] = row['Parent_required']
                event['keywords']['toddler'] = row['Toddler']
                event['keywords']['series'] = row['Series']
                event['keywords']['Snugabug Pick'] = row['Snugabug Pick']
                event['keywords']['free'] = row['Free']
                event['keywords']['preschooler'] = row['Preschooler']
                event['keywords']['reservation'] = row['Reservation']
                event['keywords']['parent_notrequired'] = row['Parent_notrequired']

                lat, lon = get_event_location(event['placeid'])
                event['location'] = {'lat': lat, 'lon': lon}

		event['images'] = find_images(event['eventid'])

                print event

                op_dict = {
                    "index": {
                        "_index": INDEX_NAME,
                        "_type": TYPE_NAME,
                        "_id": event[ID_FIELD]
                    }
                }
                bulk_data.append(op_dict)
                bulk_data.append(event)

    try:
        # bulk index the data
        print("bulk indexing...")
        res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
    except Exception, ex:
        print "Failed to index: %s" % afile
        print ex

    # sanity check
    res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
    print(" response: '%s'" % (res))

    print("results:")
    for hit in res['hits']['hits']:
        print(hit["_source"])
