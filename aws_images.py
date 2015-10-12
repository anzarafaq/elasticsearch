from collections import defaultdict
from boto.s3.connection import S3Connection


def get_image_locations():
	aws_access_key='AKIAJUXT6ZEWG72IHH7A'
	aws_secret_key='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'

	conn = S3Connection(aws_access_key, aws_secret_key)
	mybucket = conn.get_bucket('snugabug')
	images_dict = defaultdict(list)

	for l in mybucket.list():
	    filepath = l.key
	    filename = filepath.split('/')[-1]
            if filename.startswith('.') or filename.startswith('_'):
                continue
            else:
	        placeid = filepath.split('/')[2].split('_')[0]
	        images_dict[placeid].append('/' + filepath)

	return images_dict
