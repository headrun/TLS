import xpaths
import unicodedata
import re
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import MySQLdb
import json
import scrapy
from scrapy.selector import Selector
import time
from scrapy.http import Request
import datetime
import sys
from MySQLdb import OperationalError
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils

class HackThis(scrapy.Spider):
    name = "hackthis_crawl"
    start_urls = ["https://www.hackthissite.org/forums/index.php"]

    def __init__(self):
	self.conn,self.cursor = self.mysql_conn()
	self.query_status = utils.generate_upsert_query_posts_crawl('posts_hackthissite')
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
        conn = MySQLdb.connect(db="posts_hackthissite", host="localhost",user="tls_dev", passwd="hdrn!", use_unicode=True, charset='utf8')	
        cursor = conn.cursor()
	return conn,cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def add_http(self, url):
        if 'http' not in url:
            url = url.replace('./', 'https://www.hackthissite.org/forums/')
        else:
            url = url.replace('./', '')
        return url

    def parse(self, response):
        sel = Selector(response)
        forums = sel.xpath(xpaths.FORUMS).extract()
        for forum in forums:
            forum = self.add_http(forum)
            yield Request(forum, callback=self.parse_nxt)

    def parse_nxt(self, response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREADURLS).extract()
        for thread_url in thread_urls:
            thread_url = self.add_http(thread_url)
            if '&start=' in response.url:
	            reference_url = ''.join(re.sub('&sid=(.*)&start','&start',response.url))
    	    else:
        	    reference_url = ''.join(re.sub('&sid=(.*)','',response.url))
            sk = ''.join(thread_url).split('t=')[-1]
            json_posts = {'sk':sk,
                          'post_url':thread_url,
                          'crawl_status':0,
                          'reference_url':reference_url
            }
            self.cursor.execute(self.query_status, json_posts)

        if not re.findall('start=\d+',response.url):
            try:
                l_page = sel.xpath(xpaths.PAGE_NAVIGATION).extract()[-1]
                page_num = int(''.join(re.findall('start=\d+',l_page)).replace('start=',''))
                for i in range(10,page_num,10):
                    n_page = response.url + '&start='+str(i)
                    yield Request(n_page,callback = self.parse_nxt)
            except:pass

