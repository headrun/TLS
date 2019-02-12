#encoding: utf- 8
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import utils
import xpaths
import md5

class Forumbit(scrapy.Spider):
    name = "forumbit_crawl"
    start_urls = ["https://forum.bits.media/"]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="posts_forumbit",host="localhost",user="root",passwd="1216" , use_unicode = True , charset = 'utf8mb4')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        sel = Selector(response)
        links = sel.xpath(xpaths.MAIN_LINKS).extract()
        for link in links:
            yield Request(link, callback=self.parse_urls)

    def parse_urls(self, response):
        sel = Selector(response)
        link = sel.xpath(xpaths.FORUM_LINKS).extract()
        for forum_link in link:
			yield Request(forum_link, callback=self.parse_nxt)

    def parse_nxt(self, response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREAD_URLS).extract()
        for thread_url in thread_urls:
            post_url = thread_url
            sk = md5.md5(thread_url).hexdigest()
            query_status = utils.generate_upsert_query_posts_crawl('posts_forumbit')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)

