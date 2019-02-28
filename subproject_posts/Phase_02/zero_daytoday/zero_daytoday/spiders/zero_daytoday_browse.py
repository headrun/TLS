'''
   Onion site using tor 0_daytoday_browse
'''
import time
import datetime
import MySQLdb
import sys
import re
import requests
import json
import scrapy
#import  daytoday_csv
import utils
from zero_daytoday_xpaths import *
#from middlewares import *
from zero_daytoday_posts import *
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request, FormRequest
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher

class formus(Spider):
    name = '0_daytoday_browse'
    start_urls = ['http://mvfjfugdwgc5uwho.onion/']

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="hdrn59!", db="0_daytoday", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def add_http(self, url):
        if 'http' not in url:
            url = 'http://mvfjfugdwgc5uwho.onion%s'%url
        return url


    def parse(self, response):
        sel = Selector(response)
        #print response.body
        #import pdb;pdb.set_trace()

        #cookies = {
        #    'PHPSESSID': 'vad5eagpoj3je0entf7tva1880',

        #}

        headers = {
            'Host': 'mvfjfugdwgc5uwho.onion',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:45.0) Gecko/20100101 Firefox/45.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'http://mvfjfugdwgc5uwho.onion/',
            'Connection': 'keep-alive',
        }

        data = {
          'agree': 'Yes, I agree'
        }

        yield FormRequest('http://mvfjfugdwgc5uwho.onion/', callback=self.parse_next, headers=headers, formdata=data)

    def parse_next(self, response):
        sel = Selector(response)
        #thread_links = sel.xpath('//div[@class="ExploitTableContent"]//h3//a//@href').extract()
        thread_links = sel.xpath(THREAD_LINKS).extract()
        reference_url = response.url
        for thread_urls in thread_links:
              thread_urls = self.add_http(thread_urls)
              print thread_urls
	      sk = ''.join(thread_urls).split('/')[-1]
              query_status = utils.generate_upsert_query_posts_crawl('0_daytoday')
              json_posts = {'sk':sk,
                          'post_url':thread_urls,
                          'crawl_status':0,
                          'reference_url':reference_url
              }
              self.cursor.execute(query_status, json_posts)





