# -*- coding: utf-8 -*-
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import re
import datetime
import time
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import json
import sys
import utils
import xpaths
query_status = utils.generate_upsert_query_posts_crawl('prologic')


class PrlogicSpider(scrapy.Spider):
    name = 'prologic_threads_crawl'
    start_urls = ['http://prologic.su/']

    def __init__(self):
        self.conn = MySQLdb.connect(db="prologic", host="localhost", use_unicode=True, charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        forum_urls = response.xpath(xpaths.FORUM_URLS).extract()
        for forum_url in forum_urls:
            yield Request(forum_url, callback = self.parse_next)

    def parse_next(self, response):
        thread_urls = response.xpath(xpaths.THREAD_URLS).extract()
        for thread_url in thread_urls:
            sk = ''.join(re.findall('/\d+',thread_url)).replace('/','')
	    json_posts = {'sk':sk,
                         'post_url':thread_url,
                         'crawl_status':0,
                         'reference_url':response.url
                       }
            self.cursor.execute(query_status, json_posts)
        navigation = response.xpath(xpaths.NAVIGATION).extract()
        for pagenation in navigation:
            yield Request(pagenation, callback = self.parse_next)

