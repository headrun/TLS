import sys
from elasticsearch import Elasticsearch
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
import hashlib
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy import Spider
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import json
import xpaths
import MySQLdb
import os
import logging
from scrapy.utils.log import configure_logging
from time import strftime
import time
#A_QUE = utils.generate_upsert_query_authors("bbs")

import requests
import re

HEADERS = {
 "Accept": "text/plain, */*; q=0.01",
 "Origin": "https://bbs.pediy.com",
 "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36"
}

class Bbs_authors(Spider):
    name = "bbs_authors"

    def __init__(self,*args, **kwargs2):
        self.conn = MySQLdb.connect(db= "posts", host = "localhost", user="tls_dev", passwd = "hdrn!", use_unicode=True, charset="utf8mb4")
        self.cursor = self.conn.cursor()
        self.es = Elasticsearch(['10.2.0.90:9342'])
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)
    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        select_que = "select distinct(links) from bbs_authors_crawl where crawl_status = 0 "
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select DISTINCT(auth_meta) from bbs_authors_crawl where links = "%s"'%url
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime = []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))
            uno = re.sub('(.*)user-','',url)
            url = "https://passport.kanxue.com/user-public_json-%s"%(uno)
            meta = {'publish_epoch':set(activetime),'uno':uno}
            yield Request(url, callback = self.parse_meta,meta = meta, headers = HEADERS)

    def parse_meta(self,response):
        active_times = response.meta.get('publish_epoch')
        res = json.loads(response.text)
        uno = response.meta.get('uno')
        url = "https://bbs.pediy.com/user-get_gid-%s"%(uno)
        meta = {'res':res,'active_times':active_times,'uno':uno}
        yield Request(url,callback = self.get_gid,headers = HEADERS,meta =meta)

    def get_gid(self,response):
        group_data = json.loads(response.text)
        res = response.meta.get('res')
	username = res.get('message').get('username')
        last_active_ = res.get('message').get('login_date_fmt')
        join_date_ = res.get('message').get('create_date_fmt')
        join_date = utils.time_to_epoch(join_date_,"%Y-%m-%d %H:%M")
        if join_date == False: 
	    join_date = 0
        last_active = utils.time_to_epoch(last_active_,"%Y-%m-%d %H:%M")
        if last_active == False: 
	    last_active = 0
        total_posts = res.get('message').get('posts')
        rank = res.get('message').get('rank')
        group = group_data.get('message').get('level')
        reputation = re.sub('stars','',''.join(re.findall('stars\d+',group_data.get('message').get('stars'))))
        active_time = utils.activetime_str(response.meta.get('active_times'),total_posts)
        reference_url = 'https://bbs.pediy.com/user-'+response.meta.get('uno')
        json_author = {
                        'crawl_type':"keep up",
                        'reputation':reputation,
                        'domain':"bbs.pediy.com",
                        'user_name': username,
                        'author_signature': '',
                        'join_date':join_date,
                        'last_active':last_active,
                        'total_posts': total_posts,
                        'credits':" ",
                        'awards': " ",
                        'rank':rank ,
                        'groups' : group,
                        'active_time': active_time,
                        'contact_info': ' ',
                        'reference_url': reference_url
                        }
	#sk = hashlib.md5(json_author['domain']+username).hexdigest() 
        #sk = hashlib.md5(str(username)).hexdigest()
        self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(username.encode("utf-8"))).hexdigest(), body=json_author)
