import scrapy
from scrapy.http import Request
from scrapy.http import FormRequest
from scrapy import Spider
import json
import re
from urllib import urlencode
import requests
from pprint import pprint
import hashlib
from elasticsearch import Elasticsearch
es = Elasticsearch(['10.2.0.90:9342'])
import time
import MySQLdb


class Virus(Spider):
    name = 'virustotal_commants'

    def __init__(self):
        self.conn = MySQLdb.connect(db= "virestotsl", host = "localhost", user="root", passwd = "123", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()

    def start_requests(self):
        self.cursor.execute('select author_id from virustotal_crawl')
        data = self.cursor.fetchall()
        for da  in data:	
            user_comment = 'https://www.virustotal.com/ui/users/'+ da[0] + '/comments?relationships=item,author'
            yield Request(user_comment,callback = self.user_comments,meta={'user_id':da[0]})


    def user_comments(self,response):
        text = json.loads(response.body)
        next_page = text.get('links',{}).get('next','')
	data = text.get('data',[])
        if next_page:
	    try:
		da_ = data[-1]
		sk__ = da_.get('relationships',{}).get('item',{}).get('data',{}).get('id','')
		user_id_ = da.get('relationships',{}).get('author',{}).get('data',{}).get('id','')
		test_case = hashlib.md5(sk_ + user_id).hexdigest()
		query={"query":{"match":{"_id":test_case}}}
		res = es.search(body=query)
		if res['hits']['hits']==[]:
 	            yield Request(next_page,callback = self.user_comments)
	    except:
		pass
        for da in data:
            if type(da) is not dict:
                continue
            user_id = da.get('relationships',{}).get('author',{}).get('data',{}).get('id','')
            hashtags = da.get('attributes',{}).get('tags',[])
            comment = da.get('attributes',{}).get('text','')
            URL_ = da.get('relationships',{}).get('item',{}).get('data',{}).get('id','')
            if URL_:
                URL = 'https://www.virustotal.com/gui/url/'+URL_+'/detection'
            else:URL = ''
            doc = {
		    'domain': 'virustotal.com',
                    'days_ago':da.get('attributes',{}).get('date',0)*1000,
                    'Comment': URL_ + \
                            '\n'+ da.get('attributes',{}).get('text','') ,
                    'Hashtags':da.get('attributes',{}).get('tags',[]),
                    'user_id': user_id,
                    'Hash': URL_,
                    'fetch_time':round(time.time()*1000),
                    'URL': URL
                    }
            sk_ = da.get('relationships',{}).get('item',{}).get('data',{}).get('id','')
            if sk_ and user_id:
                sk = hashlib.md5(sk_ + user_id).hexdigest()
		query={"query":{"match":{"_id":sk}}}
		res = es.search(body=query)
		if res['hits']['hits'] == []:
                    es.index(index= 'forum_posts',doc_type='post',id=sk,body = doc)
