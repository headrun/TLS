'''
 binrev_browse
'''
import datetime
import sys
import json
import re
import unicodedata
from scrapy.selector import Selector
from scrapy.spiders import BaseSpider
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
reload(sys)
import MySQLdb
from binrev_xpaths import *
import utils
import binrev_csv
import random

class formus(BaseSpider):
    name = 'binrev_browse'
    allowed_domain = ["http://www.binrev.com/"]
    start_urls = ["http://www.binrev.com/forums/"]
    handle_httpstatus_list = [403, 404, 504]
    #custom_settings = {
        #'DOWNLOAD_DELAY':2,
        #'CONCURRENT_REQUESTS_PER_IP':1,
        #'CONCURRENT_REQUESTS_PER_DOMAIN':1,
        #'CONCURRENT_REQUESTS':1,
        #}

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="", db="binrev", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)
        self.proxy_pool = ['http://95.85.212.59:49305','http://77.51.150.189:8080','http://101.255.52.162:31941','http://89.249.251.21:3128','http://77.104.99.83:8080', 'http://89.140.169.24:42446', 'http://128.199.158.234:3128', 'http://193.85.228.180:54455', 'http://181.115.183.170:4839', 'http://45.235.44.235:8080', 'http://137.59.161.162:43938', 'http://159.138.21.170:80', 'http://183.89.81.14:8080', 'http://35.185.24.212:80', 'http://88.146.227.253:8080', 'http://186.46.39.66:41759', 'http://145.255.159.56:39615', 'http://109.99.223.98:32881', 'http://118.174.148.105:4048', 'http://178.219.20.90:3128', 'http://31.134.106.66:31199', 'http://103.229.200.66:37260', 'http://125.25.165.105:33850', 'http://5.10.86.242:3128', 'http://87.255.13.217:8080', 'http://114.199.123.194:8080', 'http://139.99.57.60:80', 'http://94.143.42.30:8080', 'http://38.99.117.149:58501', 'http://175.143.37.201:41960']

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()



    def add_http(self, url):
        '''
          Adding http link
        '''
        if 'http' not in url:
            url = 'http://www.binrev.com/%s'%url
        return url


    def parse(self, response):
        '''
          Main page urls
        '''
        sel = Selector(response)
        start_page_urls = sel.xpath(MAIN_URLS).extract()
        for main_urls in start_page_urls:
            main_urls = self.add_http(main_urls)
            #yield Request(main_urls, callback=self.parse_nextpage, meta={'proxy':'http://141.193.189.1:44464'})
            yield self.get_request(main_urls, callback=self.parse_nextpage)


    def get_request(self, url, callback):
        req = Request(url=url, callback=callback)
        if self.proxy_pool:
            req.meta['proxy'] = random.choice(self.proxy_pool)
        return req

    def parse_nextpage(self, response):
        '''
          Navigation for next_pages links for posts
        '''
        sel = Selector(response)
        thread_links = sel.xpath(THREAD_URLS).extract()
        for thread_urls in thread_links:
	    sk = ''.join(thread_urls).split('?')[-1]
            query_status = utils.generate_upsert_query_posts_crawl('binrev')
            json_posts = {'sk':sk,
                          'post_url':thread_urls,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
            #yield self.get_request(thread_urls, callback=self.parse_all_pages_links)
            #yield Request(thread_urls, callback=self.parse_all_pages_links)
        navigation_click = sel.xpath(NAVIGATION).extract()
        for click in navigation_click:
            if click:
                if 'http' not in click: click = self.add_http(click)
                #yield Request(click, callback=self.parse_nextpage)
                yield self.get_request(click, callback=self.parse_nextpage)


