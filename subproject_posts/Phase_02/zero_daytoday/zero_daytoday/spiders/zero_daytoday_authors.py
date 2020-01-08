'''
  0daytoday_Authors
'''
import time
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
#from  test_tor import*
from zero_daytoday_xpaths import*
import re
import utils
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
import hashlib
query_authors = utils.generate_upsert_query_authors('0_daytoday')


class formus(scrapy.Spider):
    name="0daytoday_author"
    start_urls = ["http://mvfjfugdwgc5uwho.onion/"]
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="0_daytoday",host="localhost",user="root",passwd="qwe123",use_unicode=True,charset="utf8")
        self.cursor = self.conn.cursor()
        select_query = 'select DISTINCT(links) from 0daytoday_data_crawl;'
        self.cursor.execute(select_query)
        self.data = self.cursor.fetchall()


    def parse(self, response):
    	sel = Selector(response)
        #cookies = {
        #    'PHPSESSID': 'vad5eagpoj3je0entf7tva1880',
        #}
        headers = {
            'Host': 'mvfjfugdwgc5uwho.onion',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:45.0) Gecko/20100101 Firefox/45.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'http://mvfjfugdwgc5uwho.onion/',
            'Connection': 'keep-alive',
        }

        data = {
          'agree': 'Yes, I agree'
        }

        yield FormRequest('http://mvfjfugdwgc5uwho.onion/', callback=self.parse_next, headers=headers, formdata=data)

    def parse_next(self,response):
        urls = []
        for da in self.data:
	     urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from 0daytoday_meta where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('time',''))

            comment_publish_time = set(activetime)
            meta = {'comment_publish_time' : comment_publish_time} #'thread_title': thread_title
            if url and meta:
                yield Request(url, callback=self.parse_author,meta = meta)



    def parse_author(self, response):
        sel = Selector(response)
        user_name = ''.join(sel.xpath(USER_NAME).extract()).replace('\n', '').replace('\t', '').replace('\r', '')
        if user_name == "":
            user_name = ''
        domain = "mvfjfugdwgc5uwho.onion"
        crawl_type= ''
        total_posts = ''
        #fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        fetch_time =  (round(time.time()*1000))
        join_dates = ''.join(sel.xpath(JOIN_DATE).extract())
        joindate = datetime.datetime.strptime(join_dates, '%d-%m-%Y')
        join_date = time.mktime(joindate.timetuple())*1000
        if join_date == "":
            join_date = ''

        author_signature = ''
        last_active = ''
        total_posts = ''
        groups = ''
        reputations = ''.join(response.xpath('//div[@class="td allow_tip"] //img[contains(@src,".gif")]/@src').extract())
	reputation_ = ''.join(re.findall('\d+',reputations)).strip()
	reputation =  sum([int(i) for i in reputation_])
	credits = ''
        contact_info = ''
        awards= ''
        rank = ''
        activetimes_ = response.meta.get('comment_publish_time')
        activetimes = []
        for activetime in activetimes_:
            try:
                dt = time.gmtime(int(activetime)/1000)
                total_posts= ''
                activetime = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),total_posts)
                activetimes.append(activetime)
            except:
                activetime = ' '
                activetimes.append(activetime)
        activetimes = ',  '.join(activetimes)
        BL = ''.join(sel.xpath(BL_XPATH).extract())
        exploits = ''.join(set(sel.xpath(EXPLOITS).extract()))
        readers = ''.join(sel.xpath(READERS).extract())
	json_authors = {}
        json_authors.update({'username' : user_name,
                          'domain' : domain,
                          'auth_sign': author_signature,
                          'join_date' : join_date,
                          'lastactive' : last_active,
                          'totalposts' : total_posts,
                          'fetch_time' : fetch_time,
                          'groups' : groups,
                          'reputation' : reputation,
                          'credits' : credits,
                          'awards' : awards,
                          'rank' : rank,
                          'activetimes' : activetimes,
                          'contactinfo' : '',
			  'BL' : BL,
 			  'exploits' : exploits,
			  'readers'  : readers,
                          'reference_url' : response.url
        })
        #self.cursor.execute(query_authors, json_authors)
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(user_name)).hexdigest(), body=json_authors)


