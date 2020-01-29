import scrapy
#from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import sys
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from antionline_xpaths import *
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
query_authors = utils.generate_upsert_query_authors('antionline')


class formus(scrapy.Spider):
    name="antionline_author"
    handle_httpstatus_list = [403]

    def __init__(self):
	self.conn ,self.cursor = self.mysql_conn()
	self.es = Elasticsearch(['10.2.0.90:9342'])
        dispatcher.connect(self.close_conn, signals.spider_closed)
	select_query = 'select DISTINCT(links) from antionline_crawl;'
        self.cursor.execute(select_query)
	self.conn.commit()
        self.data = self.cursor.fetchall()

    def mysql_conn(self):
        conn = MySQLdb.connect(db="tls_phase_2",host="localhost",user="tls_dev",passwd="hdrn!",use_unicode=True,charset="utf8")
        cursor = conn.cursor()
	return conn,cursor
	
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        for da in self.data:
            url = da[0]
	    meta_query = 'select DISTINCT(auth_meta) from antionline_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
	    self.conn.commit()
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('time',''))

            publish_time = set(activetime)
            meta = {'publish_time' : publish_time}
            if url and meta:
                yield Request(url, callback=self.parse_author,meta = meta)

    def parse_author(self, response):
        sel = Selector(response)
        username = ''.join(sel.xpath(USER_NAME).extract()).strip().encode('ascii','ignore').replace('\t','').replace('\n','')
        if username:
           query = 'update blackhat_status set crawl_status = 1 where reference_url = %(url)s'
           json_data = {'url':reference_url}
           self.cursor.execute(query,json_data)
	   self.conn.commit()
        domain = "www.antionline.com"
        post_count = ''.join(sel.xpath(TOTAL_POST_COUNT).extract()).replace('\t', '').replace('\n', '')
        fetchtime = int(datetime.datetime.now().strftime("%s")) * 1000
        join_date = '0'
        author_signature = ''.join(sel.xpath(AUTHOR_SIGNATURE).extract())
        lastactive = '0'
        post_count = '0'
        groups = ''.join(sel.xpath('//span[@class="usertitle"]//text()').extract()).replace('\n','').replace('\t','').replace('\r','')
        reputation = '0'
        rank = '0'
        activetimes_ = response.meta.get('publish_time')
        activetimes = []
        for activetime in activetimes_:
            try:
                dt = time.gmtime(int(activetime)/1000)
                post_count = ''.join(sel.xpath(TOTAL_POST_COUNT).extract()).replace('\t', '').replace('\n', '')
                activetime = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),post_count)
                activetimes.append(activetime)
            except:
                activetime = ' '
                activetimes.append(activetime)
    	json_authors = {}
        json_authors.update({
			  'username' : username,
                          'domain' : domain,
                          'auth_sign': author_signature,
                          'join_date' : join_date,
                          'lastactive' : lastactive,
                          'totalposts' : post_count,
                          'fetch_time' : fetchtime,
                          'groups' : groups,
                          'reputation' : reputation,
                          'credits' : '',
                          'awards' : '',
                          'rank' : rank,
                          'activetimes' : ''.join(activetimes),
                          'contact_info' : '',
            })
	
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(username)).hexdigest(), body=json_authors)



