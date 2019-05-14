import time
import datetime
import sys
import requests
from scrapy.http import Request
import scrapy
from onionwebsites import utils
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from onionwebsites.scripts import *
from onionwebsites import utils

class Paypal(scrapy.Spider):
    name = "paypal_crawl"
    start_urls = ["http://flkcpcprcfouwj33.onion/"]

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user="tls_dev", passwd="hdrn!", db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        main_urls=response.xpath('//div[@class="tclcon"]//h3//a/@href').extract()
        for main_url in main_urls:
            main_url = "http://flkcpcprcfouwj33.onion/" + main_url
            yield Request(main_url,callback= self.parse_nxt)

    def parse_nxt(self, response):
        thread_urls = response.xpath('//div[@class="tclcon"]//span//a/@href').extract()
        for thread_url in thread_urls:
            post_url = "http://flkcpcprcfouwj33.onion/" + thread_url
            sk = ''.join(post_url).split('=')[-1]
            query_status = utils.generate_upsert_query_posts_crawl('paypal')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
             }
            self.cursor.execute(query_status, json_posts)
        inner_nav = ''.join(response.xpath('//p[@class="pagelink conl"]//strong[@class="item1"]//a[@rel="next"]//@href').extract())
        if inner_nav:
            inner_nav = "http://flkcpcprcfouwj33.onion/" + inner_nav
            yield Request(inner_nav,callback=self.parse_nxt)
