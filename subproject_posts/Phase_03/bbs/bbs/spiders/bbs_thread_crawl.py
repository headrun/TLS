import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import MySQLdb
import re
import xpaths
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
QUE = utils.generate_upsert_query_posts_crawl('bbs')


class Bbs(Spider):
    name="bbs_thread_crawl"
    start_urls = ["https://bbs.pediy.com/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db= "posts", host = "127.0.0.1", user="root", passwd = "qwe123", use_unicode=True,charset="utf8")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.conn_close, signals.spider_closed)

    def conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        urls = response.xpath(xpaths.FORUMS).extract()
        for url in urls:
            if 'http'not in url: url = 'https://bbs.pediy.com/'+url
            yield Request(url,callback = self.parse_forum)

    def parse_forum(self,response):
        threads = response.xpath(xpaths.THREAD).extract()
        for thread in threads:
            if 'http' not in thread: thread = 'https://bbs.pediy.com/' + thread
            json_val = {}
            json_val = {
                    'sk': ''.join(re.findall('\d+',thread)),
                    'post_url': thread,
                    'crawl_status': 0,
                    'reference_url': response.url,
                    }
            self.cursor.execute(QUE,json_val)
        next_page = ''.join(response.xpath(xpaths.NEXT_PAGE).extract())
        if next_page:
            nextpage = 'https://bbs.pediy.com/'+next_page
            yield Request(nextpage,callback = self.parse_forum)
