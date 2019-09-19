import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import MySQLdb
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils

QUE = utils.generate_upsert_query_posts_crawl('hellbound')

class Hellbound(scrapy.Spider):
    name="hellbound_thread_crawl"
    start_urls = ["https://www.hellboundhackers.org/forum/index.php"]

    def __init__(self):
        self.conn = MySQLdb.connect(db= "hellbound", host = "localhost",user="tls_dev",passwd="hdrn!", use_unicode=True,charset="utf8")
	self.cursor = self.conn.cursor()
	dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        forums = response.xpath('//h2[@class="hlink"]//a//@href').extract()
        for forum in forums:
            yield Request("https://www.hellboundhackers.org/forum/"+forum, callback = self.parse_forum, meta = {'crawl_type':'keep up',})

    def parse_forum(self,response):
        threads = response.xpath('//h2[@class="hlink"]//a//@href').extract()
        crawl_type = response.meta.get('crawl_type','')
        for thread in threads:
            json_val = {}
            thread_url = "https://www.hellboundhackers.org/forum/"+thread
            json_val = {
                    'sk': thread.replace('.html',''),
                    'post_url': thread_url,
                    'crawl_status': 0,
                    'crawl_type': crawl_type,
                    'reference_url': response.url,
                    }
            self.cursor.execute(QUE,json_val)
	    self.conn.commit()
        try:
            nextpage = response.xpath('//td[contains(@class,"tbl")]/a[contains(text(),">")]/@href').extract()[0].replace('./','https://www.hellboundhackers.org/forum/')
            yield Request(nextpage, callback = self.parse_forum, meta = {'crawl_type':'catch up'})
        except:pass
