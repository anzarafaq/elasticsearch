import os
import json

raw = open('catfilter.json', 'r').read()
cat_filter = json.loads(raw)
