import os
import csv
import time

from elasticsearch import Elasticsearch
from geocode import _normalise as address_normalise


"""
So we have address as the index filed, with may be description as well
other information is just OTHER data
"""

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'place'
TYPE_NAME = 'place'
ID_FIELD = 'placeid'


def delete_index(es):
    try:
        if es.indices.exists(INDEX_NAME):
            print("deleting '%s' index..." % (INDEX_NAME))
            res = es.indices.delete(index = INDEX_NAME)
            print(" response: '%s'" % (res))
    except Exception, ex:
        print ex

def setup_index(es):
# since we are running locally, use one shard and no replicas
    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "place": {
                "properties": {
                    "placeid": {"type": "string"},
                    "place": {"type": "string"},
                    "website": {"type": "string"},
                    "description": {"type": "string"},
                    "street": {"type": "string"},
                    "city": {"type": "string"},
                    "state": {"type": "string"},
                    "postal_code": {"type": "string"},
                    "phone": {"type": "string"},
                    "location": {"type": "geo_point"},
                    "category": {"type": "string"},
                    "hours": {"type": "object", "enabled": False},
                    "images": {"type": "object", "enabled" : False},
                    "otherdata": {"type": "object", "enabled" : False}
                }
            }
        }
    }

    print("creating '%s' index..." % (INDEX_NAME))
    res = es.indices.create(index = INDEX_NAME, body = request_body)
    print(" response: '%s'" % (res))


def make_placeid(place):
    return (place.strip()
                .replace(" ", "")
                .replace("'", "")
                .replace("_", "")
                .replace("-", ""))


def time_in_12hour(timevalue_24hour):
    if (timevalue_24hour.find("Sunset") != -1
        or timevalue_24hour.find("Sunrise") != -1):
        return timevalue_24hour


    if timevalue_24hour.find(":") == -1:
        timevalue_24hour = timevalue_24hour +":00"

    if timevalue_24hour == '24:00':
        timevalue_24hour = '00:00'

    t = time.strptime(timevalue_24hour, "%H:%M")
    return time.strftime( "%I:%M%p", t )

def format_hours(row):
    hours = {}
    for day in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']:
        open_key = "%s Open" % day
        close_key = "%s Close" % day
        open_v = row.get(open_key)
        close_v = row.get(close_key)
        if open_key in row.keys() and close_key in row.keys() and open_v and close_v:
            hours[day] = "%s - %s" % (time_in_12hour(open_v), time_in_12hour(close_v)) 
    return hours


def find_csv_files():
    for afile in os.listdir('.'):
        if afile.endswith('.csv'):
	    print "************************"
	    print afile
	    print "***********************"
            yield afile


if __name__ == '__main__':
    ## Initialize ES
    # create ES client, create index
    es = Elasticsearch(hosts = [ES_HOST])
    delete_index(es)
    setup_index(es)

    for afile in find_csv_files():
    	bulk_data = []
        print afile
        with open(afile, 'Ub') as csvfile:
            reader = csv.DictReader(csvfile)
            #print reader.fieldnames
            for row in reader:
                store = {}
                place_name = row.get('Place Name', '').decode('ascii', errors='replace') or row.get('Name', '').decode('ascii', errors='replace')
                if place_name in ('', None):
                    print "Empty line found"
                    continue
                print place_name

                store['placeid'] = make_placeid(place_name)
                store['place'] = place_name
                store['website'] = row.get('Website') or row.get('URL')
                store['description'] = row.get('Description', '').decode('ascii', errors='replace')

                location = dict(street=row.get('Address'), city=row.get('City'), region=row.get('State'), postcode=row.get('Zip'))
                try:
                    n_address = address_normalise(location)
                    store['street'] = n_address.get('street')
                    store['city'] = n_address.get('city')
                    store['state'] = n_address.get('state')
                    store['postal_code'] = n_address.get('postcode')
                    store['location'] = {'lon': n_address.get('longitude'), 'lat': n_address.get('latitude')}
                    store['phone'] = row.get('Phone', '')
                    store['hours'] = format_hours(row)
                except Exception, ex:
                    print ex
                    print row
                    #No point in continuing such a location
                    continue

                #Deal with other data
                otherdata = {}
                for key in row.keys():
                    if key not in ['Place Name', 'Name', 'Address', 'City', 'State', 'Zip', 'Phone', 'Website', 'Description']:
                        if (key in ['Saturday Open', 'Sunday Open', 'Monday Open', 'Tuesday Open', 'Wednesday Open', 'Thursday Open', 'Friday Open']
                            or key in ['Saturday Close', 'Sunday Close', 'Monday Close', 'Tuesday Close', 'Wednesday Close', 'Thursday Close', 'Friday Close']
                            or key.startswith('Keyword')):
                            continue
                        otherdata[key] = row.get(key)
                store['otherdata'] = otherdata

                print store
                op_dict = {
                    "index": {
                        "_index": INDEX_NAME, 
                        "_type": TYPE_NAME, 
                        "_id": store[ID_FIELD]
                    }
                }

                bulk_data.append(op_dict)
                bulk_data.append(store)
	try:
    		# bulk index the data
    		print("bulk indexing...")
    		res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
	except Exception, ex:
		print "Failed to index: %s" % afile

	print "One file done: %s ..." % afile

    # sanity check
    res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
    print(" response: '%s'" % (res))

    print("results:")
    for hit in res['hits']['hits']:
        print(hit["_source"])