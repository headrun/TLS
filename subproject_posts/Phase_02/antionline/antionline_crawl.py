import datetime
import time
import sys
import json
import re
import scrapy
from scrapy.selector import Selector
from scrapy.spiders import BaseSpider
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
reload(sys)
sys.setdefaultencoding('UTF8')
import MySQLdb
import unicodedata
from antionline_xpaths import *
#from scrapy.conf import settings
import utils

class formus(BaseSpider):
    name = 'antionline_crawl'
    start_urls = ["http://www.antionline.com/forum.php"]
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="", db="antionline", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def add_http(self, url):
        if 'http' not in url:
            url = 'http://www.antionline.com/%s'%url
        return url

    def parse(self, response):
        sel = Selector(response)
        start_page_urls = sel.xpath(MAIN_URLS).extract()
        for main_urls in start_page_urls:
            main_urls = "http://www.antionline.com/" + main_urls
            yield Request(main_urls, callback=self.parse_nextpage)

    def parse_nextpage(self, response):
        sel = Selector(response)
        thread_links = sel.xpath(THREAD_URLS).extract()
        for thread_urls in thread_links:
            thread_urls = "http://www.antionline.com/" + thread_urls
            sk = ''.join(thread_urls).split('?')[-1]
            query_status = utils.generate_upsert_query_posts_crawl('antionline')
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

