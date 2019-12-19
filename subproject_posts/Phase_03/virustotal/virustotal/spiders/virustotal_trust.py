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
    name = 'virustotal_trust'
	
    def __init__(self):
        self.conn = MySQLdb.connect(db= "virestotsl", host = "localhost", user="root", passwd = "123", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()

    def start_requests(self):
        self.cursor.execute('select author_id from virustotal_crawl')
        data = self.cursor.fetchall()
        for da  in data:
	    user_id = da[0]
            trusted_users_urls = 'https://www.virustotal.com/ui/users/' + da[0] +'/trusted_users?limit=40'
            trusted_by_users = 'https://www.virustotal.com/ui/users/' + da[0] +'/trusted_by_users?limit=40'
            #user_comment = 'https://www.virustotal.com/ui/users/' + da[0] +'/comments?relationships=item,author'
            yield Request(trusted_by_users,callback = self.trusted,meta={'user_id':user_id})
            yield Request(trusted_users_urls,callback = self.trusted,meta={'user_id':user_id})

    def trusted(self, response):
	in_qwe = 'insert into virustotal_crawl(`author_id`,`author_link`) values(%(user_id)s,%(user_url)s) on duplicate key update `author_id` = %(user_id)s,`author_link` = %(user_url)s'
        user_id = response.meta['user_id']
        text = json.loads(response.body)
        data = text.get('data',[])
        for da in data:
            if 'trusted_users' in response.url:
                doc = {}
                doc.update({
		'domain':'virustotal.com',
                'user_id':user_id,
                'target_user_trusted_userid': da.get('id',''),
                'target_user_trusted_reputation': da.get('attributes',{}).get('reputation',0),
                'fetch_time':round(time.time()*1000),
                })
                if da.get('id',''):
                    es.index(index= 'forum_posts',doc_type='post',id=sk,body = doc)
		    self.cursor.execute(in_qwe,{'user_id':da.get('id',''),'user_url':'https://www.virustotal.com/gui/user/'+da.get('id','')})
            if 'trusted_by_users' in response.url:
                doc = {}
                doc.update({
		'domain':'virustotal.com',
                'user_id':user_id,
                'user_trusting_userid': da.get('id',''),
                'reputation_user_trusting_userid': da.get('attributes',{}).get('reputation',0),
                'fetch_time':round(time.time()*1000),
                })
                if da.get('id',''):
                    es.index(index= 'forum_posts',doc_type='post',id=sk,body = doc)
		    self.cursor.execute(in_qwe,{'user_id':da.get('id',''),'user_url':'https://www.virustotal.com/gui/user/'+da.get('id','')})
		    
        next_page = text.get('links',{}).get('next','')
        if next_page:
            yield Request(next_page,callback = self.trusted,meta={'user_id':user_id})



