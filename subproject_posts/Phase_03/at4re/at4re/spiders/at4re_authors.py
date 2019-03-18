'''
at4re_Authors
'''

import scrapy
import  unicodedata
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
import datetime
from datetime import timedelta
import calendar
import sys
import re
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from at4re_xpaths import *
import utils
from elasticsearch import Elasticsearch
import hashlib

query_authors = utils.generate_upsert_query_authors('at4re')



class formus(scrapy.Spider):
    name="at4re_author"
    allowed_domain = ["://www.at4re.net"]
    start_urls = ["https://www.at4re.net/f"]
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="at4re",host="localhost",user="root",passwd="",use_unicode=True,charset="utf8")
        self.cursor = self.conn.cursor()
        select_query = 'select DISTINCT(links) from at4re_crawl;'
        self.cursor.execute(select_query)
        self.data = self.cursor.fetchall()

    def parse(self, response):
        urls = []
        for da in self.data:
            urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from at4re_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
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
        username = ''.join(set(sel.xpath(USERNAME).extract())).strip().replace('\t','').replace('\n','').replace('\r','')
        if username == "":
            username = ''
        if username:
           query = 'update at4re_browse set crawl_status = 1 where reference_url = %(url)s'
           json_data = {'url': response.url}
           self.cursor.execute(query,json_data)


        domain = "at4re.net"
        author_signature = ''.join(sel.xpath(AUTHOR_SIGNATURE).extract())
	fetchtime = int(datetime.datetime.now().strftime("%s")) * 1000
        totalposts = ''.join(sel.xpath(TOTAL_POSTS).extract()).replace('\n','').replace('\t','').replace('\r','').split('(')[0]
        groups = sel.xpath(GROUPS).extract()[0].replace('\r','').replace('\n','').replace('\t','')
	reputation = ''.join(sel.xpath('//strong[@class="reputation_positive"]/text()').extract()) or ''.join(sel.xpath('//strong[@class="reputation_neutral"]//text()').extract())
        Rank = ''.join(sel.xpath(RANK).extract()).replace('\r','').replace('\n','').replace('\t','')
        try:
            rank = Rank.split('.')[-1].count('')-1
        except:
            try:
	        if Rank:
                    rank = Rank
                else:
                    rank = ' '
            except:pass

        join_dates = ''.join(sel.xpath(JOIN_DATES).extract())
        try:
	    joindate = datetime.datetime.strptime(join_dates, '%d-%m-%Y')
            join_date = time.mktime(joindate.timetuple())*1000
	except:
	    try:
	        if u'\u0623\u0645\u0633' in join_dates:
                  joined = ''.join(join_dates.replace(u'\u0623\u0645\u0633',(datetime.datetime.now() - timedelta(days=1)).strftime('%d %B %Y ')))
                  joindate= datetime.datetime.strptime(joined, '%d %B %Y ')
                  join_date = time.mktime(joindate.timetuple())*1000
	    except:
		pass

        last = ''.join(sel.xpath(LAST_ACTIVE).extract())
        try:
	   if u'\u0645\u0646\u0630' in last and u'\u062f\u0642\u0627\u0626\u0642' in last:
		last_datetime = datetime.datetime.now() - timedelta(minutes=40)
		last_active =  time.mktime(last_datetime.timetuple())*1000
	   
	   elif u'\u0645\u0646\u0630' in last and u'\u0633\u0627\u0639\u0627\u062a' in last:
		last_datetime = datetime.datetime.now() - timedelta(hours=4)
		last_active =  time.mktime(last_datetime.timetuple())*1000
           
	   else:
	        last_datetime = datetime.datetime.strptime(last, '%d-%m-%Y, %I:%M %p')
	        last_active =  time.mktime(last_datetime.timetuple())*1000
        except:
	    try:
	       if u'(\u0645\u062e\u0641\u064a)' in last:
		   last_active = 0
	       else:
		   last_datetime = datetime.datetime.strptime(last, '%d-%m-%Y')
                   last_active =  time.mktime(last_datetime.timetuple())*1000
            except:
	        pass
	activetimes_ = response.meta.get("publish_time")
        activetimes = []
        for activetime in activetimes_:
            try:
                dt = time.gmtime(int(activetime)/1000)
                totalposts =''.join(sel.xpath('//strong[contains(text(), "%s")]/../../td/text()' % u'\u0625\u062c\u0645\u0627\u0644\u064a \u0627\u0644\u0645\u0634\u0627\u0631\u0643\u0627\u062a :').extract()).replace('\n','').replace('\t','').replace('\r','').split('(')[0]
                activetime = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),totalposts)
                activetimes.append(activetime)
            except:
                activetime = ' '
                activetimes.append(activetime)
        activetimes = ',  '.join(activetimes)

	json_authors = {}
        json_authors.update({'username' : username,
                          'domain' : domain,
                          'crawl_type' : "keepup",
                          'auth_sign': author_signature,
                          'join_date' : join_date,
                          'lastactive' : last_active,
                          'totalposts' : totalposts,
                          'fetch_time' : fetchtime,
                          'groups' : groups,
                          'reputation' : reputation,
                          'credits' : '',
                          'awards' : '',
                          'rank' : rank,
                          'activetimes' : activetimes,
                          'contactinfo' : '',
            })
        self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(username)).hexdigest(), body=json_authors)

