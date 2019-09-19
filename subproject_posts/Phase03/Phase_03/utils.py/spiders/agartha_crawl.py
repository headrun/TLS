import time
import datetime
import sys
import requests
from scrapy.http import Request
import scrapy
import MySQLdb
from onionwebsites.utils import *
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

class Agartha(scrapy.Spider):
    name = "agartha_crawl"
    start_urls = ["http://agartha2oooh2cxa.onion/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user="tls_dev",passwd="hdrn!" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        main_urls=response.xpath('//td[@class="info"]//a[@class="subject"]//@href').extract()
        for main_url in main_urls:
            yield Request(main_url,callback= self.parse_nxt)

    def parse_nxt(self, response):
        thread_urls = response.xpath('//span[contains(@id,"msg_")]//a/@href').extract()
        for post_url in thread_urls:
            sk = ''.join(post_url).split('=')[-1]
            query_status = generate_upsert_query_posts_crawl('agartha')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
             }
            self.cursor.execute(query_status, json_posts)
        num = response.xpath('//div[@class="pagelinks"]//strong/following-sibling::a[@class="navPages"]/@href').extract_first()
	if num:
            yield Request(num,callback=self.parse_nxt)

