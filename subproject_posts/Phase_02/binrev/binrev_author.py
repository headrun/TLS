'''
Binrev_Authors
'''
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
import datetime
import calendar
import csv
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from binrev_xpaths import *
import utils
import binrev_csv
query_authors = utils.generate_upsert_query_authors('binrev')



class formus(scrapy.Spider):
    name="binrev_author"
    allowed_domain = ["http://www.binrev.com"]
    start_urls = ["http://www.binrev.com/forums/"]
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="binrev",host="localhost",user="root",passwd="",use_unicode=True,charset="utf8")
        self.cursor = self.conn.cursor()
        select_query = 'select DISTINCT(links) from binrev_crawl;'
        self.cursor.execute(select_query)
        self.data = self.cursor.fetchall()
 
    def parse(self, response):
        urls = []
        for da in self.data:
            urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from binrev_crawl where links = "%s"'%url.encode('utf8')
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
        username = ''.join(sel.xpath(USER_NAME).extract()).strip().encode('ascii','ignore').replace('\t','').replace('\n','')
        if username == "":
            username = ''
	if username:
           query = 'update binrev_browse set crawl_status = 1 where reference_url = %(url)s'
           json_data = {'url': response.url}
           self.cursor.execute(query,json_data)
	

        domain = "www.binrev.com"
        post_count = ''.join(sel.xpath(TOTAL_POST_COUNT).extract()).replace('\t', '').replace('\n', '')
        fetchtime = int(datetime.datetime.now().strftime("%s")) * 1000
        groups = ''.join(response.xpath('//div[@class="ipsPos_left ipsPad cProfileHeader_name"]//span//text()').extract())
        reputation = ''.join(response.xpath('//span[@class="cProfileRepScore_points"]//text()').extract())
        Rank = ''.join(sel.xpath(RANK).extract()).replace('\t', '').replace('\n', '')
        if Rank:
            rank = Rank
        else:
            rank = ' '

        join_dates = ''.join(sel.xpath(JOIN_DATE).extract()).replace('\t', '').replace('\n', '')
        #joindate = datetime.datetime.strptime(joindates, '%a %b %d, %Y %I:%M %p')
        joindate = datetime.datetime.strptime(join_dates, '%m/%d/%Y %I:%M %p')
        join_date = time.mktime(joindate.timetuple())*1000


        lastactives = ''.join(sel.xpath(LAST_ACTIVE).extract()).replace('\t', '').replace('\n', '')
        try:
            #last_active =  datetime.datetime.strptime(lastactives, '%a %b %d, %Y %I:%M %p')
            last_active = datetime.datetime.strptime(lastactives, '%m/%d/%Y %I:%M %p')
            lastactive =  time.mktime(last_active.timetuple())*1000
        except:
            lastactive = 0

        activetimes_ = response.meta.get("publish_time")
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
        activetimes = ',  '.join(activetimes)

        json_authors = {}
        json_authors.update({'user_name' : username,
			  'domain' : domain,
                          'crawl_type' : "keepup",
			  'author_signature': '',
			  'join_date' : join_date,
			  'last_active' : lastactive,
			  'total_posts' : post_count,
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


