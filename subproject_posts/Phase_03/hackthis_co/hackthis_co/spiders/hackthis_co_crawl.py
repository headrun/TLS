import scrapy
from scrapy import Spider
from scrapy.selector import Selector
import time
from scrapy.http import Request
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
import json
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import unicodedata


class HackThisCo(scrapy.Spider):
    name = "hackthis_co_crawl"
    start_urls = ["https://www.hackthis.co.uk/forum/"]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="posts_hackthis_co", host="localhost",
                                    user="tls_dev", passwd="hdrn!", use_unicode=True, charset='utf8')
        self.crawl_query = utils.generate_upsert_query_posts_crawl(
            'posts_hackthis_co')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        sel = Selector(response)
        forums = sel.xpath('//ul[@class="sections"]//li//a[not(contains(@href,"/level"))]/@href').extract()
        for forum in forums:
            forum = "https://www.hackthis.co.uk" + forum
            yield Request(forum, callback=self.parse_nxt)

    def parse_nxt(self, response):
        thread_urls = response.xpath('//div[@class="section_info col span_16"]//a[@class="strong"]/@href').extract()
        for thread_url in thread_urls:
            thread_url = "https://www.hackthis.co.uk" + thread_url
            sk = ''.join(re.findall('\d+',thread_url))
            query_status = utils.generate_upsert_query_posts_crawl('posts_hackthis_co')
            json_posts = {'sk':sk,
                          'post_url':thread_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
            self.conn.commit()
        pagenav = ''.join(response.xpath('//li[@class="right"]//a[@rel="next"]/@href').extract())
        if pagenav:
            page = re.sub('\?page=\d+','',response.url)+pagenav
            yield Request(page,callback=self.parse_nxt)
