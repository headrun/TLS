import datetime
import json
import MySQLdb
import re
import time
from datetime import date
from datetime import timedelta
import scrapy
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
import json
from scrapy import signals
import random
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
import hashlib

class BleepingSpider(Spider):
    name = 'bleeping_author'
    start_urls = ['https://www.bleepingcomputer.com/forums/']

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
	self.conn,self.cursor = self.mysql_conn()
	self.upsert_query_authors = utils.generate_upsert_query_authors('bleeping_computer')
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):	
        conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="",
                                    use_unicode=True,
                                    charset="utf8")
        cursor = conn.cursor()
	return conn, cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self,response):
	try:
	    key = ''.join(re.findall("secure_hash\'](.*)= '(.*)",x)[0][1].split(';'))[:-1]
	except:
	     key = '880ea6a14ea49e853634fbdc5015a024'
        data = {
            'auth_key': key,
            'referer': 'https://www.bleepingcomputer.com/forums/index.php',
            'ips_username': 'inqspdr2',
            'ips_password': 'lolw4@123~',
            'rememberMe': '1',
        }
        url = 'https://www.bleepingcomputer.com/forums/index.php?app=core&module=global&section=login&do=process'
        yield FormRequest(url, callback=self.parse_next, formdata=data)

    def parse_next(self,response):
        start_query = 'select DISTINCT(links) from bleeping_authors_crawl limit 5'
        self.cursor.execute(start_query)
	self.conn.commit()
        data = self.cursor.fetchall()
        urls = [datum[0] for datum in data]
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from  bleeping_authors_crawl where links = %(url)s'
            val = {'url':url}
            self.cursor.execute(meta_query,val)
	    self.conn.commit()
            meta_query = self.cursor.fetchall()
            publish_time = []
            thread_title = []
            for data in meta_query:
                meta = eval(data[0])
                thread_title.append(meta.get('thread_title', ''))
                publish_time.append(meta.get('publish_time', ''))
            publish_time  = set(publish_time)
            thread_title = ', '.join(set(thread_title))
            author_meta = {'publish_time':publish_time,'thread_title':thread_title}
            if author_meta and url:
                yield Request(url, callback=self.parse_author,meta = author_meta)

    def parse_author(self, response):
        json_data = {}
        thread_title = response.meta.get('thread_title','')
        sel = Selector(response)
        user_name = ''.join(sel.xpath('//h1//span[@class="fn nickname"]//text()').extract())
        domain = "www.bleepingcomputer.com"
        if user_name:
            up_que_1 = "update bleeping_authors_crawl set crawl_status = 1 where links = %(url)s"
            up_que_val ={'url':response.url}
            self.cursor.execute(up_que_1,up_que_val)
	    self.conn.commit()
        json_data.update({'username': user_name,
                          'domain': domain,
                          'crawl_type': 'keep_up'})
        activetimes_ = response.meta.get('publish_time')
        activetimes = []
        try:
            total_posts = int(''.join(response.xpath(xpaths.TOTALPOSTS).extract()).replace(',',''))
        except:
            total_posts = 0
        for activetime in activetimes_:
            try:
                dt = time.gmtime(activetime/1000)
                activetime ="""[ { 'year': '%s','month': '%s', 'dayofweek': '%s', 'hour': '%s', 'count': '%s' }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),total_posts)
            except:
                activetime = '-'
            activetimes.append(activetime)
        joindate = ''.join(response.xpath(xpaths.JOINDATE).extract())
        joindate = ''.join(re.findall('\w+, \d\d:\d\d \wM',joindate)) or ''.join(re.findall('\d\d \w+ \d\d\d\d',joindate))
        lastactive = ''.join(response.xpath(xpaths.LAST_ACTIVE).extract()).replace("Last Active ",'')
        try:
            joindate = datetime.datetime.strptime(joindate,'%d %b %Y')
            joindate = time.mktime(joindate.timetuple())*1000
        except:
            try:
                if 'esterday' in joindate:
                    joindate_ = date.today() - timedelta(1)
                    joindate_ = joindate_.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',joindate))
                    joindate = datetime.datetime.strptime(joindate_,'%b %d %Y %I:%M %p')
                    joindate = time.mktime(joindate.timetuple())*1000
                elif 'oday' in joindate:
                    joindate_ = date.today()
                    joindate_ = joindate_.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',joindate))
                    joindate = datetime.datetime.strptime(joindate_,'%b %d %Y %I:%M %p')
                    joindate = time.mktime(joindate.timetuple())*1000
            except:
                pass
        author_signature = ""
        try:
            lastactive = datetime.datetime.strptime(lastactive,'%b %d %Y %I:%M %p')
            lastactive =  time.mktime(lastactive.timetuple())*1000
        except:
            if 'esterday' in lastactive:
                yesterday = date.today() - timedelta(1)
                yesterday =  yesterday.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',lastactive))
                lastactive = datetime.datetime.strptime(yesterday,'%b %d %Y %I:%M %p')
                lastactive =  time.mktime(lastactive.timetuple())*1000
            elif 'oday' in lastactive:
                yesterday = date.today()
                yesterday = yesterday.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',lastactive))
                lastactive = datetime.datetime.strptime(yesterday,'%b %d %Y %I:%M %p')
                lastactive =  time.mktime(lastactive.timetuple())*1000
            else:
                lastactive = 00
        author_signature = ''.join(sel.xpath(xpaths.AUTHOR_SIGN).extract()).replace('#','')
        if not "smile.png" in author_signature:
            author_signature = ''.join(sel.xpath(xpaths.AUTHOR_SIGN_).extract()).encode('utf-8').replace('#','')

        groups = ''.join(response.xpath(xpaths.GROUPS).extract())
        fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000

        json_data.update({'auth_sign': author_signature,
			  'username':user_name,
                          'join_date': joindate,
                          'lastactive': lastactive,
                          'totalposts': total_posts,
                          'fetch_time': fetch_epoch,
                          'groups': groups,
                          'reputation': "",
                          'credits': "",
                          'awards': "",
                          'rank': "",
                          'activetimes': ''.join(activetimes),
                          'contactinfo': ''
        })
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(user_name).hexdigest(), body=json_data)
