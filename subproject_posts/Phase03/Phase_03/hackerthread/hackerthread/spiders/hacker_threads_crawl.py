import unicodedata
import re
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import MySQLdb
import json
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
import time
from scrapy.http import Request
import datetime
import sys 
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils

class Hacker_threads(scrapy.Spider):
    name = "hacker_threads_crawl"
    start_urls = ["https://www.hackerthreads.org/"]

    def __init__(self, *args, **kwargs):
        super(Hacker_threads, self).__init__(*args, **kwargs)
        self.query = utils.generate_upsert_query_posts('hacker_threads')
        self.conn = MySQLdb.connect(db="POSTS_HACKERTHREADS",
                                    host="localhost",
                                    user="root",
                                    passwd="",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def add_http(self, url):
        if 'http' not in url:
            url = url.replace('./', 'https://www.hackerthreads.org/')
        else:
            url = url.replace('./', '')
        return url

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        sel = Selector(response)
        categories = sel.xpath('//a[@class="forumtitle"]/@href').extract()
        for cate in categories:
            cate =  self.add_http(cate)
	    yield Request(cate, callback=self.parse_links)

    def parse_links(self, response):
        sel = Selector(response)
        links = sel.xpath('//div[@class="list-inner"]//a[@class="topictitle"]/@href').extract()
        for link in links:
            link = self.add_http(link)
	    sk = link.split('&t=')[-1]
	    query_status = utils.generate_upsert_query_posts_crawl('hacker_threads')
            json_posts = {'sk':sk,
                          'post_url':link,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
	    self.conn.commit()

        nxt_pg=''.join(sel.xpath('//div[@class="action-bar bar-top"]//following-sibling::div[@class="pagination"]//li[@class="arrow next"]//a[@class="button button-icon-only"]/@href').extract())
        if nxt_pg:
           nxt_pg = self.add_http(nxt_pg)
           yield Request(nxt_pg, callback=self.parse_links)

