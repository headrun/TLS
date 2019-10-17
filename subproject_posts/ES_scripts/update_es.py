# coding:utf-8

from elasticsearch import Elasticsearch
import json

# Define config
host = "127.0.0.1"
port = 9200
timeout = 1000
index = "forum_posts"
doc_type = "post"
size = 1000
body = {}
es = Elasticsearch(['10.2.0.90:9342'])
# Init Elasticsearch instance
'''es = Elasticsearch(
    [
        {
            'host': host,
            'port': port
        }
    ],
    timeout=timeout
)'''


# Process hits here
def process_hits(hits):
    for item in hits:
        doc = item['_source']
	sk = item['_id']
	import pdb;pdb.set_trace()
	try:
	    sub = doc['sub_category']
	    sub_ = ','.join(eval(sub))
	    doc.update({'sub_category':sub_})
	except:
	    try:
		sub = doc['sub_category']
		sub_ = ','.join(sub)
		doc.update({'sub_category':sub_})
	    except:
	    	import pdb;pdb.set_trace()

	try:
	    link = doc['links']
	    links_ = ', '.join(eval(link))
	    doc.update({'links':links_})
	except:
	    try:
		link = doc['links']
		links_ = ', '.join(link)
		doc.update({'links':links_})
	    except:
	        import pdb;pdb.set_trace()

	try:
	    post_id = doc['post_id']
	    post_id_ = ''.join(post_id)
	    doc.update({'post_id':post_id_})
	except:
	    pass
	import pdb;pdb.set_trace()
	es.index(index="forum_posts", doc_type='post', id=sk, body=doc)
        #print(json.dumps(item, indent=2))


# Check index exists
if not es.indices.exists(index=index):
    print("Index " + index + " not exists")
    exit()

# Init scroll by search
data = es.search(
    index=index,
    doc_type=doc_type,
    scroll='2m',
    size=size,
    body=body
)

# Get the scroll ID
sid = data['_scroll_id']
scroll_size = len(data['hits']['hits'])

# Before scroll, process current batch of hits
process_hits(data['hits']['hits'])

while scroll_size > 0:
    "Scrolling..."
    data = es.scroll(scroll_id=sid, scroll='2m')

    # Process current batch of hits
    process_hits(data['hits']['hits'])

    # Update the scroll ID
    sid = data['_scroll_id']

    # Get the number of results that returned in the last scroll
    scroll_size = len(data['hits']['hits'])
