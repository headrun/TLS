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
import calendar
import csv
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
#import at4re_csv
query_authors = utils.generate_upsert_query_authors('at4re')



class formus(scrapy.Spider):
    name="at4re_author"
    allowed_domain = ["://www.at4re.net"]
    start_urls = ["https://www.at4re.net/f"]
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
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
        #import pdb;pdb.set_trace
        #author_signature = ''.join(sel.xpath('//td[@class="trow1 scaleimages"]//text()').extract()).replace('\n','').replace('\t','')
        #author_signature = ''.join(response.xpath('//td[strong[contains(., "%s")]]/following::td//text()'%u'\u062a\u0648\u0642\u064a').extract())
        author_signature = ''.join(sel.xpath(AUTHOR_SIGNATURE).extract())
	fetchtime = int(datetime.datetime.now().strftime("%s")) * 1000

        #totalposts = ''.join(sel.xpath('//strong[contains(text(), "%s")]/../../td/text()' % u'\u0625\u062c\u0645\u0627\u0644\u064a \u0627\u0644\u0645\u0634\u0627\u0631\u0643\u0627\u062a :').extract()).replace('\n','').replace('\t','').replace('\r','').split('(')[0]
        totalposts = ''.join(sel.xpath(TOTAL_POSTS).extract()).replace('\n','').replace('\t','').replace('\r','').split('(')[0]

        #groups = sel.xpath('//span[@class="smalltext"]//text()').extract()[0].replace('\r','').replace('\n','').replace('\t','')
        groups = sel.xpath(GROUPS).extract()[0].replace('\r','').replace('\n','').replace('\t','')
	reputation = ''.join(sel.xpath('//strong[@class="reputation_positive"]/text()').extract()) or ''.join(sel.xpath('//strong[@class="reputation_neutral"]//text()').extract())

	#Rank = ''.join(sel.xpath('//span[@class="smalltext"]//@alt').extract()).replace('\r','').replace('\n','').replace('\t','')
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

	#join_date = sel.xpath('//td[contains(.,"%s")]//following-sibling::td//text()' %u'\u0627\u0644\u0633\u0645\u0639\u0629 :').extract()[2]
        #join_dates = ''.join(sel.xpath('//strong[contains(text(), "%s")]/../../td/text()' % u'\u0625\u0646\u0636\u0645 \u0625\u0644\u064a\u0646\u0627 :').extract())
        join_dates = ''.join(sel.xpath(JOIN_DATES).extract())
        joindate = datetime.datetime.strptime(join_dates, '%d-%m-%Y')
        join_date = time.mktime(joindate.timetuple())*1000
        #import pdb;pdb.set_trace()

	#last = ''.join(sel.xpath('//strong[contains(text(), "%s")]/../../td/text()' % u'\u0622\u062e\u0631 \u0632\u064a\u0627\u0631\u0629').extract())
        last = ''.join(sel.xpath(LAST_ACTIVE).extract())
        try:
            #import pdb;pdb.set_trace()
            last_datatime = datetime.datetime.strptime(last, '%d-%m-%Y, %H:%M %p')
            last_active =  time.mktime(last_datatime.timetuple())*1000
            last_active = last_active
        except:
           try:
              if u'\u0623\u0645\u0633' in last:
                  last = ''.join(last.replace(u'\u0623\u0645\u0633',(datetime.datetime.now() - timedelta(days=1)).strftime('%d %B %Y ')))
                  last_datatime_= datetime.datetime.strptime(last, '%d %B %Y ')
                  last_active = time.mktime(last_datatime_.timetuple())*1000

              elif u'\u0627\u0644\u064a\u0648\u0645' in last:
                  last = ''.join(last.replace(u'\u0627\u0644\u064a\u0648\u0645',datetime.datetime.now().strftime('%d %B %Y ')))
                  last_datatime_= datetime.datetime.strptime(last, '%d %B %Y ')
                  last_active = time.mktime(last_datatime_.timetuple())*1000
              elif u'(\u0645\u062e\u0641\u064a)' in last:
                  last_active =0
              elif  u'\u0645\u0646\u0630' in last and u'\u0633\u0627\u0639\u0627\u062a' in last:
                  last_datetime_ = ''.join(re.sub(u'\u0645\u0646\u0630 \d+ \u0633\u0627\u0639\u0627\u062a',datetime.datetime.now().strftime('%d %B %Y '),last))
                  x = datetime.datetime.now().strftime('%d-%m-%Y')
                  last_= datetime.datetime.strptime(x, '%d-%m-%Y')
                  last_active = time.mktime(last_.timetuple())*1000
              elif u'\u0645\u0646\u0630'  in last and u'\u062f\u0642\u0627\u0626\u0642' in last:
                  last_datetime_ = ''.join(re.sub(u'\u0645\u0646\u0630 \d+ \u062f\u0642\u0627\u0626\u0642',datetime.datetime.now().strftime('%d %B %Y '),last))
                  y = datetime.datetime.now().strftime('%d-%m-%Y')
                  last_= datetime.datetime.strptime(y, '%d-%m-%Y')
                  last_active = time.mktime(last_.timetuple())*1000
           except:
                try:
                    if u'\u0623\u0645\u0633' in last:
                        last = ''.join(last.replace(u'\u0623\u0645\u0633',(datetime.datetime.now() - timedelta(days=1)).strftime('%d %B %Y , %H:%M %p')))
                        last_datatime_= datetime.datetime.strptime(last, '%d %B %Y , %H:%M %p')
                        last_active = time.mktime(last_datatime_.timetuple())*1000
                except:
                    try:
                        if  u'(\u062e\u0641\u064a)' or u'\u0645\u062e\u0641\u064a' in last:
                            last_active = 0
                    except:
                        last_active = 0
                        #import pdb;pdb.set_trace() 


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




        #credits = ''
        #awards = ''
        #contact_info  = ''
	json_authors = {}
        json_authors.update({'user_name' : username,
                          'domain' : domain,
                          'crawl_type' : "keepup",
                          'author_signature': author_signature,
                          'join_date' : join_date,
                          'last_active' : last_active,
                          'total_posts' : totalposts,
                          'fetch_time' : fetchtime,
                          'groups' : groups,
                          'reputation' : reputation,
                          'credits' : '',
                          'awards' : '',
                          'rank' : rank,
                          'active_time' : activetimes,
                          'contact_info' : '',
                          'reference_url' : response.url
            })

        self.cursor.execute(query_authors, json_authors)


