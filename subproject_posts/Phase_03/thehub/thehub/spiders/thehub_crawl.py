import time
import datetime
import MySQLdb
import sys
import re
import requests
import json
import scrapy
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request, FormRequest
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from urllib import urlencode
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils

class Thehub(Spider):
    name = "thehub_crawl"
    start_urls = ["http://thehub7xbw4dc5r2.onion"]

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="qwe123", db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        sel = Selector(response)
        name = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@name').extract())
        value = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@value').extract())
	params = (
    ('action', 'login2'),
)
	data={
name : value,
'cookielength':'',	
'hash_passwrd':'',	
'passwrd'	:'!nq!nq7@6',
'user':	'inqspdr',
}
	url = 'http://thehub7xbw4dc5r2.onion/index.php?' +urlencode(params)
	yield FormRequest(url,formdata=data,callback=self.after_login)
        

    def after_login(self, response):
        sel = Selector(response)
	main_urls=response.xpath('//td[@class="info"]//a[@class="subject"]//@href | //td[@class="children windowbg"]//strong//following-sibling::a//@href | //tr[contains(@id,"board_children")]//td[@class="children windowbg"]//strong//a//@href').extract()
	for main_url in main_urls:
	    main_url = main_url.replace('action=unread','')
	    yield Request(main_url,callback=self.parse_nxt)

    def parse_nxt(self, response):
	sel = Selector(response)
	thread_urls = response.xpath('//span[contains(@id,"msg_")]//a/@href').extract()
	for url in thread_urls:
	    sk = ''.join(url).split('=')[-1]
            query_status = utils.generate_upsert_query_posts_crawl('thehub_tor')
            json_posts = {'sk':sk,
                          'post_url':url,
                          'crawl_status':0,
                          'reference_url':response.url
             }
	    self.cursor.execute(query_status, json_posts)
	innerpage=sel.xpath('//div[@class="pagelinks floatleft"]//a/@href').extract()
	for page in innerpage:
	    yield Request(page,callback=self.parse_nxt)
