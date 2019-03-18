import scrapy
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from scrapy.selector import Selector
from scrapy.http import Request
import datetime, csv
import time
import MySQLdb
import json
import utils
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib

class WilderAuthors(scrapy.Spider):
    name = 'wilderssecurity_author'
    start_urls = ['http://wilderssecurity.com/']
    handle_httpstatus_list=[403]

    def __init__(self, *args, **kwargs):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="POSTS_WILDER",
                                    host="localhost",
                                    user="root",
                                    passwd = "",
                                    use_unicode=True,
                                    charset="utf8")
	self.cursor=self.conn.cursor()
	dispatcher.connect(self.close_conn, signals.spider_closed)
	
    def close_conn(self, spider):
	self.conn.commit()
	self.conn.close()

    def parse(self, response):
        select_query = 'select DISTINCT(links) from wilder_crawl where crawl_status = 0'
	#import pdb;pdb.set_trace()
        self.cursor.execute(select_query)
	self.conn.commit()

        self.data = self.cursor.fetchall()	
        for da in self.data:
            url = da[0]
	    meta_query = 'select DISTINCT(auth_meta) from wilder_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime, join_date, total_posts, author,group_ = [], [], [], [], []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch', ''))
                join_date.append(meta.get('join_date', ''))
                total_posts.append(meta.get('total_posts', ''))
                author.append(meta.get('author', '')) 
                group_.append(meta.get('groups', ''))
            publish_epoch = set(activetime)
            join_date = set(join_date)
            total_posts = set(total_posts)
            author = set(author)
            groups_ = set(group_)
            meta = {'publish_epoch':publish_epoch, 'join_date':join_date, 'total_posts':total_posts, 'author':author, 'groups_':groups_}
            if url and meta:
                self.parse_author(meta)

    def parse_author(self, meta):
        active_time = []
        json_data = {}
        count, user_name, total_posts = "", "", ""
        domain = "www.wilderssecurity.com"
        activetime_ = meta.get("publish_epoch")
        totalposts = meta.get("total_posts","")
        groups_ = meta.get("groups_","")
        for count in totalposts:
            total_posts = str(count)
        for activetime_1 in activetime_:
            try:
                dt = time.gmtime(int(activetime_1)/1000)
                activetime = """[ { 'year': '%s', 'month': '%s', 'dayofweek': '%s', 'hour': '%s', 'count': '%s' }]"""%(str(dt.tm_year),str(dt.tm_mon), str(dt.tm_wday), str(dt.tm_hour), count)
                active_time.append(activetime)
            except:
                active_time = "-"
        joindate = meta.get("join_date","")
        for joindatie_ in joindate: join_date = str(joindatie_)
        for group in groups_: groups = str(group)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        username = meta.get("author","")
        for un in username: user_name = str(un)
	last_active = 0

        json_data.update({'username': user_name,
                         'domain': domain,
                         'crawl_type': 'keep_up',
                         'auth_sign': "",
                         'join_date': join_date,
                         'lastactive': last_active,
                         'totalposts': total_posts,
                         'fetch_time':fetch_time,
                         'groups': groups,
                         'reputation': '',
                         'credits': '',
                         'awards': '',
                         'rank': '',
                         'activetimes': (''.join(active_time)),
                         'contactinfo': '',
                         #'reference_url': response.url
        })
        upsert_query_authors = utils.generate_upsert_query_authors('POSTS_WILDER')
        #self.cursor.execute(upsert_query_authors, json_data)
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(user_name)).hexdigest(), body=json_data)
