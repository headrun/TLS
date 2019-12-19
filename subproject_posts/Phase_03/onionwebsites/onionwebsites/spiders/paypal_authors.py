import scrapy
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import requests
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
from urllib import urlencode
from datetime import date, timedelta
from elasticsearch import Elasticsearch
import hashlib

class Paypal(scrapy.Spider):
    name = "paypal_authors"
    es = Elasticsearch(['10.2.0.90:9342'])

    def __init__(self):
         self.conn = MySQLdb.connect(host="localhost", user="tls_dev", passwd="hdrn!", db="posts", charset="utf8", use_unicode=True)
         self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = 'select DISTINCT(links) from paypal_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select auth_meta from paypal_authors_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))
            meta = {'publish_epoch':set(activetime)}
            if url:
                yield Request(url, callback=self.parse_author,meta = meta)

    def parse_author(self, response):
        json_posts = {}
        domain = 'flkcpcprcfouwj33.onion'
        user_name = ''.join(response.xpath('//dl//dt[contains(text(),"Username")]/following-sibling::dd/text()').extract()[0])
        author_signature = ''.join(response.xpath('//div[@class="postsignature postmsg"]//p/text()').extract())
        join_date = ''.join(response.xpath('//dt[contains(text(),"Registered")]/following-sibling::dd/text()').extract())
        join = datetime.datetime.strptime(join_date,'%Y-%m-%d')
        joindate = time.mktime(join.timetuple())*1000
        totalposts = ''.join(response.xpath('//dl//dt[contains(text(),"Posts")]/following-sibling::dd/text()').extract()[0]).replace('-','').strip()
        groups = ''.join(response.xpath('//dl//dt[contains(text(),"Username")]/following-sibling::dd/text()').extract()[1])
        last_active = ''.join(response.xpath('//dt[contains(text(),"Last post")]/following-sibling::dd/text()').extract()[0]).replace('Today',datetime.datetime.now().strftime('%Y-%m-%d')).replace('Yesterday',(datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
        try:
            last = datetime.datetime.strptime(last_active,'%Y-%m-%d %H:%M:%S')
            lastactive = time.mktime(last.timetuple())*1000
        except:
            pass
	activetimes_ =  response.meta.get('publish_epoch')
        activetimes = []
        activetimes = utils.activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        contact_info = []
        icq = ''.join(response.xpath('//dt[contains(text(),"ICQ")]//following-sibling::dd/text()').extract())
        if icq:
            contact_info.append({'user_id':icq,'channel':'ICQ'})
        json_posts.update({'username': user_name,
                          'domain': "flkcpcprcfouwj33.onion",
                          'auth_sign':author_signature,
                          'join_date': joindate,
                          'lastactive': lastactive,
                          'totalposts': totalposts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': '',
                          'credits': '',
                          'awards': '',
                          'rank': '',
                          'activetimes': activetimes,
                          'contact_info': str(contact_info),
        })
	try:
	    sk = hashlib.md5(domain + json_posts['username']).hexdigest()
            self.es.index(index="forum_author",doc_type='post',id= sk, body=json_posts)
	except:
	    sk = hashlib.md5(domain + json_posts['username'].encode('utf8')).hexdigest()
	    self.es.index(index="forum_author",doc_type='post',id= sk, body=json_posts)

