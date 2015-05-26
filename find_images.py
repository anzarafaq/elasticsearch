import os

city="SanJose"
placeid="KirkPark"

IMAGES_BASE = os.path.join('/Users/mafaq/Dropbox/', 'SnugabugPhotos')

#/images/:SanJose/:SanJose:KirkPark/
image_path = ":%s/:%s:%s/" %(city, city, placeid)
complete_image_path = os.path.join(IMAGES_BASE, image_path)

#print os.path.join(IMAGES_BASE, image_path)
print complete_image_path

for afile in os.listdir(complete_image_path):
    print afile


