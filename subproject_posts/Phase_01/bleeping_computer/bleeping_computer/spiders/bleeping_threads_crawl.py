import datetime
import json
import re
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
import time
import unicodedata
import scrapy
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import xpaths
import bleeping_confi
import utils
import MySQLdb 

post_url_que = utils.generate_upsert_query_posts_crawl('bleeping_computer')

class BleepingSpider(scrapy.Spider):
    name = 'bleeping_threads_crawl'
    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()


    def clean_spchar_in_text(self, text):
        text = unicodedata.normalize('NFKD', text.decode('utf8'))
        text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
        text = re.sub(r'(\n\s*)', '\n', text)
        return text
    
    def start_requests(self):
        data = {
            'auth_key': '880ea6a14ea49e853634fbdc5015a024',
            'referer': 'https://www.bleepingcomputer.com/forums/index.php',
            'ips_username': 'inqspdr',
            'ips_password': 'Inq2018.',
            'rememberMe': '1',
        }
        url = 'https://www.bleepingcomputer.com/forums/index.php?app=core&module=global&section=login&do=process'
        yield FormRequest(url, callback=self.parse_1st, formdata=data)

    def parse_1st(self, response):
        sel = Selector(response)
        urls = sel.xpath(xpaths.URLS).extract()
        for url in urls[0:5]:
            if "https://www.bleepingcomputer.com/" in url:
                yield FormRequest(url, callback=self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        thread_urls = response.xpath(xpaths.THREAD_URLS).extract()
        for url in thread_urls:
            if "bleepingcomputer" in url:
                val= {
                        'sk': url.split('/')[-3],
                        'post_url':url,
                        'crawl_status':0,
                        'reference_url':response.url
                    }
                self.cursor.execute(post_url_que, val)
        navigation = ''.join(response.xpath(
            xpaths.NAVIGATIONS_LINKS).extract())
        if navigation:
            yield Request(navigation, callback=self.parse_next)

