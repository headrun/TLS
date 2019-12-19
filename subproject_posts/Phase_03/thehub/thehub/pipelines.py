# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from elasticsearch import Elasticsearch
es = Elasticsearch(['10.2.0.90:9342'])
import hashlib


class ThehubPipeline(object):

    def process_item(self, item, spider):
        post_url = item.get('doc',{}).get('post_url','')
	if post_url:
	    sk = hashlib.md5(post_url).hexdigest()
	    doc = item.get('doc',{})
	    query={"query":{"match":{"_id":sk}}}
            res = es.search(index="forum_posts",body=query)
            if res['hits']['hits'] == []:
	        es.index(index="forum_posts", doc_type='post', id=sk, body=doc)
	user_name = item.get('rec',{}).get('username','')
	domain = item.get('rec',{}).get('domain')
	if user_name:
	    sk = hashlib.md5(domain+user_name).hexdigest()
	    rec = item.get('rec',{})
	    es.index(index="forum_author", doc_type='post', id=sk,body = rec)
