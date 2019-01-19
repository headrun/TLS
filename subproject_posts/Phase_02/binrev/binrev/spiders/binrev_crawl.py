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
import cfscrape
class formus(BaseSpider):
    name = 'binrev_browse'

    def __init__(self, *args, **kwargs):
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
        scraper = cfscrape.create_scraper()
        r1 = scraper.get('http://www.binrev.com/forums/index.php?/login/')
        headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Origin': 'http://www.binrev.com',
    'Upgrade-Insecure-Requests': '1',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': r1.request.headers.get('User-Agent', ''),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Referer': 'http://www.binrev.com/forums/index.php',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
        cookies = { 'ips4_ipsTimezone': 'Asia/Calcutta',
    'ips4_hasJS': 'true',}
        request_cookies = r1.request._cookies.get_dict()
        response_cookies = r1.cookies.get_dict()
        cookies.update(request_cookies)
        cookies.update(response_cookies)
        sel = Selector(text = r1.text)
        csrfKey = ''.join(sel.xpath('//input[@name="csrfKey"]/@value').extract())
        data = {
                'login__standard_submitted': '1',
'csrfKey': csrfKey,
'auth': 'saikrishna',
'password': 'ammananna1@',
'remember_me': '0',
'remember_me_checkbox': '1',
'signin_anonymous': '0',
}
        yield FormRequest('http://www.binrev.com/forums/index.php?/login/', callback = self.parse, headers = headers,cookies = cookies,formdata = data)

    def parse(self, response):
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


