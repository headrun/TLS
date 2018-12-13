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
from wilderssecurity_xpaths import *
import unicodedata

def clean_spchar_in_text(self, text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    text = unicodedata.normalize('NFKD', text.decode('utf8'))
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
    text = re.sub(r'(\n\s*)', '\n', text)
    return text



class WilderssecuritySpider(scrapy.Spider):
    name = 'wilderssecurity_crawl'
    start_urls = ['http://wilderssecurity.com/']
    handle_httpstatus_list=[403]

    def __init__(self, *args, **kwargs):
        super(WilderssecuritySpider, self).__init__(*args, **kwargs)
        self.query = utils.generate_upsert_query_posts('wilder_security')
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
            que = "insert into wilder_status(post_id, links,crawl_status) values(%s,%s,0) ON DUPLICATE KEY update post_id =%s , links =%s, crawl_status=0" 
            values = (post_id, thread_url, post_id, thread_url)
            self.cursor.execute(que, values)

        navigation = ''.join(response.xpath(NAVIGATION).extract())
        if navigation:
            if "http" not in navigation: page_nation = site_domain + navigation
            yield Request(page_nation, callback = self.parse_next)


