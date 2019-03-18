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
from wilderssecurity_xpaths import *
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils


class WilderssecuritySpider(scrapy.Spider):
    name = 'wilderssecurity_crawl'
    start_urls = ['http://wilderssecurity.com/']
    handle_httpstatus_list=[403]
    
    def __init__(self):
        self.query = utils.generate_upsert_query_posts_crawl('POSTS_WILDER')
        self.conn = MySQLdb.connect(db="POSTS_WILDER",
                                    host="localhost",
                                    user="root",
                                    passwd = "",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        urls = response.xpath(URLS).extract()
        for url in urls:
            if "http" not in url: url = site_domain + url
            yield Request(url, callback = self.parse_next)

    def parse_next(self, response):
        thread_urls = response.xpath('//h3[@class="title"]//a//@href').extract() or response.xpath('//h3[@class="nodeTitle"]//a//@href').extract()
        for thread_url in thread_urls:
            if "http:" not in thread_url: thread_url = site_domain +thread_url
            post_id = ''.join(thread_url.split('/')[-2])
	    json_posts = {'sk':post_id,
                          'post_url':thread_url,
                          'crawl_status':0,
                          'reference_url':thread_url
            }
            self.cursor.execute(self.query, json_posts)
	    self.conn.commit()

        navigation = ''.join(response.xpath(NAVIGATION).extract())
        if navigation:
            if "http" not in navigation: page_nation = site_domain + navigation
            yield Request(page_nation, callback = self.parse_next)


