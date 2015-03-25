import csv
from elasticsearch import Elasticsearch

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME = 'store'
TYPE_NAME = 'store'
ID_FIELD = 'storeid'


bulk_data = []
with open('BestBuy.csv', 'rb') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		store = {}
		print row['longitude']
		print row['latitude']
		print row['address']
		#4500 Wisconsin Ave Nw, Washington, DC 20016, 202-895-1580
		street, city, zip, ph = row['address'].split(',')
		print street, city, zip, ph
		st_code, postal_code = zip.split()
		print st_code
		print postal_code

		store['storeid'] = row['description'].split()[3]
		store['description'] = row['description']
		store['street'] = street
		store['city'] = city
		store['state'] = st_code.strip()
		store['postal_code'] = postal_code
		store['phone'] = ph
		store['location'] = {'lon': row['longitude'], 'lat': row['latitude']}

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


print bulk_data

# create ES client, create index
es = Elasticsearch(hosts = [ES_HOST])

if es.indices.exists(INDEX_NAME):
    print("deleting '%s' index..." % (INDEX_NAME))
    res = es.indices.delete(index = INDEX_NAME)
    print(" response: '%s'" % (res))

# since we are running locally, use one shard and no replicas
request_body = {
	"settings" : {
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings": {
		"store": {
			"properties": {
				"storeid": {"type": "string"},
				"description": {"type": "string"},
				"street": {"type": "string"},
				"city": {"type": "string"},
				"state": {"type": "string"},
				"postal_code": {"type": "string"},
				"phone": {"type": "string"},
				"location": {"type": "geo_point"}
			}
		}
	}
}

print("creating '%s' index..." % (INDEX_NAME))
res = es.indices.create(index = INDEX_NAME, body = request_body)
print(" response: '%s'" % (res))

# bulk index the data
print("bulk indexing...")
res = es.bulk(index = INDEX_NAME, body = bulk_data, refresh = True)
import ipdb; ipdb.set_trace()

# sanity check
res = es.search(index = INDEX_NAME, size=2, body={"query": {"match_all": {}}})
print(" response: '%s'" % (res))

print("results:")
for hit in res['hits']['hits']:
    print(hit["_source"])

