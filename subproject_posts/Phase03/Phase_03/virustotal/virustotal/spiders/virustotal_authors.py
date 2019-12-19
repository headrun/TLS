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
    name = 'virustotal_authors'
	
    def __init__(self):
        self.conn = MySQLdb.connect(db= "virestotsl", host = "localhost", user="root", passwd = "123", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()

    def start_requests(self):
	self.cursor.execute('select author_id from virustotal_crawl')
	data = self.cursor.fetchall()
	for da  in data:
	    yield Request('https://www.virustotal.com/ui/users/'+da[0],callback = self.parse)

    def parse(self, response):
        data = json.loads(response.body)
        user_id = data.get('data',{}).get('id','')
        if data.get('data',{}).get('type') == 'user' and user_id:
            doc = data.get('data',{}).get('attributes',{})
            Date_created = doc.get('user_since',0) * 1000
            doc.update({
		'domain':'virustotal.com',
                'Date_created':Date_created,
                'fetch_time':round(time.time()*1000),
                'user_id':user_id,
                'user_url':'https://www.virustotal.com/gui/user/'+user_id,
                })
            doc.pop('user_since')
            sk = hashlib.md5(user_id).hexdigest()
            es.index(index= 'forum_author',doc_type='post',id=sk,body =doc)

