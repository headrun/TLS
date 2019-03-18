'''
 binrev_browse
'''
from scrapy.http import FormRequest
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
import random
import scrapy 
from requests.auth import HTTPProxyAuth
from w3lib.http import basic_auth_header

class formus(BaseSpider):
    name = 'binrev_crawl'
    
    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="", db="binrev", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

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

    def start_requests(self):
        yield Request('http://www.binrev.com/forums/',callback =self.login_page)

    '''def login_page1(self,response):
        csrfKey = ''.join(response.xpath('//input[@name="csrfKey"]/@value').extract())
        data = {
                'login__standard_submitted': '1',
'csrfKey': csrfKey,
'auth': 'kerspdr',
'password': 'qwerty123@',
'remember_me': '0',
'remember_me_checkbox': '1',
'signin_anonymous': '0',
}
        yield FormRequest.from_response(response, callback = self.login_page,formdata = data)'''

    def login_page(self, response):
        sel = Selector(response)
        start_page_urls = sel.xpath(MAIN_URLS).extract()
        for main_urls in start_page_urls:
            main_urls = self.add_http(main_urls)
            yield Request(main_urls, callback=self.parse_nextpage)

    def parse_nextpage(self, response):
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
        navigation_click = sel.xpath(NAVIGATION).extract()
        for click in navigation_click:
            if click:
                if 'http' not in click: click = self.add_http(click)
                yield Request(click, callback=self.parse_nextpage)


