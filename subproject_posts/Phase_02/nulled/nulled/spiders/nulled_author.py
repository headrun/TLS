import  nulled_xpath
import datetime
from datetime import timedelta
import scrapy
import time
import re
import json
import MySQLdb
from scrapy.selector import Selector
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from dbc_utils import get_googlecaptcha
import cfscrape
import requests
from scrapy.http import FormRequest
from scrapy.http import Request
from elasticsearch import Elasticsearch
import hashlib
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
que = utils.generate_upsert_query_authors('nulled')


class nulled(scrapy.Spider):
    name = "nulled_author"
    handle_httpstatus_list = [403,503]

    def __init__(self):
        self.conn = MySQLdb.connect(db= "nulled", host = "localhost", user="root", passwd = "qwe123", charset="utf8mb4")
        self.cursor = self.conn.cursor()
	self.es = Elasticsearch(['10.2.0.90:9342'])
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def urls_in_db(self):
        urls = []
        select_qry = 'select DISTINCT(links) from nulled_author_crawl limit 1000'
        self.cursor.execute(select_qry)
	self.conn.commit()
        data = self.cursor.fetchall()
        for da in data:
            urls.append(da[0])
        return urls

    def start_requests(self):
        scraper = cfscrape.create_scraper()
        r = scraper.get("https://www.nulled.to/")#index.php?app=core&module=global&section=login")
        request_cookies = r.request._cookies.get_dict()
        response_cookies = r.cookies.get_dict()
        cookies = {}
        cookies.update(request_cookies)
        cookies.update(response_cookies)
        headers = {
            'origin': 'https://www.nulled.to',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
            'pragma': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': r.request.headers.get('User-Agent', ''),
            'content-type': 'application/x-www-form-urlencoded',
            'cache-control': 'no-cache',
            'authority': 'www.nulled.to',
            'referer': 'https://www.nulled.to/index.php?app=core&module=global&section=login',
        }
	yield Request('https://www.nulled.to/user/1508270-dwc',headers=headers,cookies= cookies,callback = self.parse_next)

    def parse_next(self,response):
        headers = response.meta.get('headers')
    	sel = Selector(response)
        urls = self.urls_in_db()
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from nulled_author_crawl where links like "%s" '%url
            self.cursor.execute(meta_query) 
            self.conn.commit()
            meta_query = self.cursor.fetchall()
            PublishTime = []
            ThreadTitle = []
            for da1 in meta_query:
                meta = json.loads(da1[0].replace("'",''))
                PublishTime.append(meta.get('PublishTime',''))
                ThreadTitle.append(meta.get('ThreadTitle',''))
            PublishTime  = set(PublishTime)
            ThreadTitle= set(ThreadTitle)
            author_meta = {'PublishTime':PublishTime, 'ThreadTitle':', '.join(ThreadTitle)}
            yield Request(url.replace("'",''),callback = self.parse_author,headers=headers, meta = author_meta)

    def parse_author(self,response):
        reference_url = response.url
        username = ''.join(response.xpath(nulled_xpath.username_xpath).extract()).encode('utf8')
	if username:
	    up_que = 'update nulled_author_crawl set crawl_status = 1 where links = "%s"'%(response.url)
	    self.cursor.execute(up_que)
	    self.conn.commit()
	elif response.status > 400:
	    up_que = 'update nulled_author_crawl set crawl_status = 9 where links = "%s"'%(response.url)
	    self.cursor.execute(up_que)
	    self.conn.commit()
        Threadtitle = response.meta.get('ThreadTitle','')
        activetime_ = response.meta.get('PublishTime','')
        totalposts = ''.join(response.xpath(nulled_xpath.totalposts_xpath).extract()).replace('\n','').replace(' ','')
        activetime = nulled_xpath.activetime_str(activetime_,totalposts)
        author_signature = ' '.join(response.xpath(nulled_xpath.author_signature_xpath).extract()).encode('utf8')
        join_date_ = ''.join(response.xpath(nulled_xpath.join_date_xpath).extract()).replace('\n','').encode('utf8')
        try:
            join_date_ = ''.join(re.findall('\d+-\d+-\d+',join_date_))
            join_date_ = datetime.datetime.strptime(join_date_ , '%d-%m-%y')
            join_date = time.mktime(join_date_.timetuple())*1000
        except:
            join_date = 0
        lastactive = ''.join(response.xpath(nulled_xpath.lastactive_xpath).extract()).replace('\n','')
        if 'Today,' in lastactive or 'Yesterday,' in lastactive:
            lastactive = lastactive.replace('Today,',datetime.datetime.now().strftime('%b %d %Y ')).replace('Yesterday,',(datetime.datetime.now() - timedelta(days=1)).strftime('%b %d %Y '))
        try:
            lastactive = datetime.datetime.strptime(lastactive, '%b %d %Y  %H:%M %p')
            lastactive = time.mktime(lastactive.timetuple())*1000
        except:lastactive= 0
        groups = response.xpath(nulled_xpath.groups_xpath).extract()
        reputation = ''.join(response.xpath(nulled_xpath.reputation_xpath).extract()).encode('utf8')
        credits = ''.join(response.xpath(nulled_xpath.credits_xpath).extract()).replace('\n','').replace(' ','').encode('utf8')
        awards = ', '.join(response.xpath(nulled_xpath.awards_xpath).extract())
        rank_ = response.xpath(nulled_xpath.rank_xpath).extract()
        rank = []
        for r in rank_:
            if 'profile' not in r and 'http' not in r:
                    rank.append('https://www.nulled.to'+r)
            elif 'profile' not in r:
                rank.append(r)
        rank = ',  '.join(rank)
        json_val = {}
        json_val.update({
               'username':username,
               'domain': "nulled.to",
               'auth_sign': utils.clean_text(author_signature),
               'join_date': join_date,
               'lastactive': lastactive,
               'totalposts': totalposts,
               'fetch_time': nulled_xpath.FetchTime,
               'groups': groups,
               'reputation': reputation,
               'credits': credits,
               'awards': awards,
               'rank': rank,
               'activetimeis': activetime,
               'contactinfo': "",
               })
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(username).hexdigest(), body=json_val)
