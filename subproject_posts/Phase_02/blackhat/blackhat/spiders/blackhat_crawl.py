import datetime
import time
import json
import re
import MySQLdb
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import xpaths
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
from scrapy.selector import Selector
from scrapy.xlib.pydispatch import dispatcher
import tls_utils as utils
from scrapy import signals

class BlackHat(scrapy.Spider):
    name = "blackhat_crawl"
    start_urls = ["https://www.blackhatworld.com/forums/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts_blackhat",host="localhost",user="root",passwd="1216" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
	self.query_status = utils.generate_upsert_query_posts_crawl('posts_blackhat')
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
	self.cursor.close()
	self.conn.close()

    def parse(self, response):
        sel = Selector(response)
        forums = sel.xpath(xpaths.FORUMS).extract()
        for forum in forums:
            forum = "https://www.blackhatworld.com/" + forum
            yield Request(forum,callback= self.parse_nxt)

    def parse_nxt(self, response):
        sel = Selector(response)
        urls = sel.xpath(xpaths.THREADURLS).extract()
        for url in urls:
            post_url = "https://www.blackhatworld.com/" + url
            sk = ''.join(url).split('.')[-1]
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(self.query_status, json_posts)

        for page in set(sel.xpath(xpaths.INNERPAGENAVIGATION).extract()):
            if page:
                page = "https://www.blackhatworld.com/" + page
                yield Request(page, callback = self.parse_nxt)

