import scrapy
from scrapy.spider import Spider
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from scrapy.selector import Selector
from scrapy.http import Request
import datetime,csv
import time
import re
from datetime import timedelta
import MySQLdb
import json
import utils
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import logging
import xpaths

class skyfraud(scrapy.Spider):
   name ="skyfraud_author"
   start_urls = ["https://sky-fraud.ru/"]
   
   def __init__(self,*args,**kwargs):
       self.conn = MySQLdb.connect(db="posts",host="localhost", use_unicode = True,charset="utf8")
       self.cursor = self.conn.cursor()
       select_query = 'select DISTINCT(links) from skyfraud_crawl;'
       self.cursor.execute(select_query)
       self.data = self.cursor.fetchall()
       dispatcher.connect(self.close_conn, signals.spider_closed)

   def close_conn(self, spider):
       self.conn.commit()
       self.conn.close()

   def parse(self,response):
       for url in self.data:
           meta_query = 'select DISTINCT(auth_meta) from skyfraud_crawl where links = "%s"'%url[0].encode('utf8')
           self.cursor.execute(meta_query)
           meta_query = self.cursor.fetchall()
           activetime=[]
           for da1 in meta_query:
               meta = json.loads(da1[0])
               activetime.append(meta.get('publish_epoch'))
           publish_epoch = set(activetime)
           meta = {'publish_epoch':publish_epoch}
           if url and meta:
               yield Request(url[0], callback=self.parse_author,meta = meta)


   def parse_author(self, response):
       sel = Selector(response)
       json_data = {}
       domain = "sky-fraud.ru"
       user_name = ''.join(sel.xpath(xpaths.USERNAME).extract())
       crawl_type = ''
       authorsignature = ' '.join(response.xpath(xpaths.AUTHORSIGNATURE).extract())
       joindate = ''.join(sel.xpath(xpaths.JOINDATE).extract()[4])
       try:
           join = datetime.datetime.strptime(joindate,' %d.%m.%Y')
           join_date = time.mktime(join.timetuple())*1000
       except:pass
       
       lastactive = ''.join(sel.xpath(xpaths.LASTACTIVE).extract()[2]).strip().replace(u'\u0412\u0447\u0435\u0440\u0430',(datetime.datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')).replace(u'\u0421\u0435\u0433\u043e\u0434\u043d\u044f',datetime.datetime.now().strftime('%d.%m.%Y'))
       try:
	   lastactive = ''.join(re.findall('\d+.\d+.\d+',lastactive))
           last =  datetime.datetime.strptime(lastactive,'%d.%m.%Y')
           last_active = time.mktime(last.timetuple())*1000
       except:
           if '\xd0\x92\xd1\x87\xd0\xb5\xd1\x80\xd0\xb0' in lastactive:
              last = (datetime.datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y ')
              last_active = time.mktime(last.timetuple())*1000
           elif '\xd0\xa1\xd0\xb5\xd0\xb3\xd0\xbe\xd0\xb4\xd0\xbd\xd1\x8f' in lastactive:
              last = (datetime.datetime.now().strftime('%d.%m.%Y'))
              last_active = time.mktime(last.timetuple())*1000

       total_posts = response.xpath(xpaths.TOTALPOSTS).extract()[1].strip()
       fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
       groups = ''.join(sel.xpath(xpaths.GROUPS).extract())
       activetime_ = response.meta.get("publish_epoch")
       activetime = []
       reputations = ''
       credits = ''
       awards = ''
       rank = ''
       for activetime_i in activetime_:
           try:
               dt = time.gmtime(int(activetime_i)/1000)
               activetime_i = """[ { "year": "%d","month": "%d", "dayofweek": "%d", "hour": "%d", "count": "%s" }]"""%\
                      (dt.tm_year,dt.tm_mon,dt.tm_wday,dt.tm_hour,total_posts)
               activetime.append(activetime_i)
           except:
               activetime.append('-')
       contact_info = ''
       reference_url = response.url
       upsert_query_authors = utils.generate_upsert_query_authors('posts_skyfraud')
       json_data.update({'user_name': user_name,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'author_signature':authorsignature,
                          'join_date': join_date,
                          'last_active': last_active,
                          'total_posts': total_posts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': reputations,
                          'credits': '',
                          'awards': '',
                          'rank': '',
                          'active_time': (''.join(activetime)),
                          'contact_info': contact_info,
                          'reference_url': reference_url
        })
       self.cursor.execute(upsert_query_authors, json_data)

