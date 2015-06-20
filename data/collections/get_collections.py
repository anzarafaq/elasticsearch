import csv

reader = csv.reader(open('collections.csv', 'rUb'))
collections = {}
count = 0
for row in reader:
    if count == 0:
        count += 1
        continue
    cid = row[0]
    collections[cid] = {}
    collections[cid]['title'] = row[1]
    collections[cid]['description'] = row[2]
    collections[cid]['places'] = []

    for item in row[3:]:
        if item.strip() not in ('', None):
            collections[cid]['places'].append(item.strip())


print collections

