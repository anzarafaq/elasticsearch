import csv
import ipdb
from datetime import datetime

reader = csv.reader(open('collections.csv', 'rUb'))
collections = {}
count = 0
for row in reader:
    if count == 0:
        count += 1
        continue
    cid = row[0]
    collections[cid] = {}
    collections[cid]['date'] = row[1]
    collections[cid]['f_date'] = datetime.strptime(row[1], '%m/%d/%Y')
    collections[cid]['title'] = row[2]
    collections[cid]['description'] = row[3]
    collections[cid]['places'] = []

    for item in row[4:]:
        if item.strip() not in ('', None):
            collections[cid]['places'].append(item.strip())

print collections

s_collections = sorted(collections.items(), key=lambda x: x[1])

r_coll={}
for item in s_collections:
    del item[1]['f_date']
    r_coll[item[0]] = item[1]
    print item[0], item[1]

#print s_collections

