import  nulled_xpath
import datetime
from datetime import timedelta
import scrapy
import time
import re
import json
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from dbc_utils import get_googlecaptcha
#import cfscrape
import requests
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
QUE = utils.generate_upsert_query_posts_crawl('nulled')

class nulled(scrapy.Spider):
    name = "nulled_thread_crawl"
    start_urls = ["https://www.nulled.to/"]

    def __init__(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn =  MySQLdb.connect(db= "nulled", user="root", passwd="qwe123", host = "localhost", charset="utf8mb4", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    '''def start_requests(self):
        scraper = cfscrape.create_scraper()
	start_url = 'https://www.nulled.to/#!General+Discussion'
        r = scraper.get(start_url)
        request_cookies = r.request._cookies.get_dict()
        response_cookies = r.cookies.get_dict()
        cookies = {}
        cookies.update(request_cookies)
        cookies.update(response_cookies)
        headers = {
    'authority': 'www.nulled.to',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': r.request.headers.get('User-Agent'),
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'referer': 'https://www.nulled.to/forum/110-feedback-and-suggestions/',
}'''
    def parse(self,response):
	#response = Selector(text = r.text)	
        formus = response.xpath(nulled_xpath.formus_xpath).extract()
	meta = {'crawl_type':'keep_up'}
        for url in formus:
             if 'https://www.nulled.to/' in url:
		yield Request(url,callback = self.parse_forums_, meta=meta)

    def parse_forums_(self,response):
	#headers = response.meta.get('headers')
    	crawl_type = response.meta.get('crawl_type','')
	threads_id = response.xpath(nulled_xpath.threads_id_xpath).extract()
        threads_links = response.xpath(nulled_xpath.threads_urls_xpath).extract()
        for id_, url in zip(threads_id,threads_links):
            sk =''.join(re.findall('\d+',id_))
            val = {
                'sk': sk,
                'post_url': url,
                'crawl_status': 0,
                'reference_url': response.url,
        	}
            self.cursor.execute(QUE,val)
	    self.conn.commit()
        next_page = ''.join(response.xpath(nulled_xpath.threads_next_page_xpath).extract())
        if next_page:
            meta = {'crawl_type':'catch_up'}
            if 'https://www.nulled.to/' in  next_page:
		yield Request(next_page,callback = self.parse_forums_,meta=meta)
