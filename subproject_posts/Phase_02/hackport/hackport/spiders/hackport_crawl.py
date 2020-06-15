import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import re
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
query_status = utils.generate_upsert_query_posts_crawl('hackport')


class HackportSpider(scrapy.Spider):
    name = 'hackport_crawl'
    start_urls = ['http://hack-port.ru/']

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", host="localhost", user="root", passwd="qwe123", use_unicode=True, charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
       	urls = response.xpath('//td[@class="threadIcoTd"]//a[contains(@href,"/forum/") and not(contains(@href,"-0-17"))]//@href').extract()
        for url in urls:
            url = "http://hack-port.ru" + url
            yield Request(url, callback = self.parse_next)

    def parse_next(self, response):
        thread_urls = response.xpath('//div[@class="gDivRight"]//td[@class="threadNametd"]//a//@href').extract()
        for thread_url in thread_urls:
            thread_url = "http://hack-port.ru" + thread_url
            sk = ''.join(re.findall('-\d+',thread_url)[0]).replace('-','')
	    json_posts = {'sk':sk,
                         'post_url':thread_url,
                         'crawl_status':0,
                         'reference_url':response.url
                       }
            self.cursor.execute(query_status, json_posts)
	    self.conn.commit()
        navigation = response.xpath('//td[@class="FrmBotCl11"]//li[@class="switch switch-next"]//a//@href').extract()
        for pagenation in navigation:
            pagenation = "http://hack-port.ru" + pagenation
            yield Request(pagenation, callback = self.parse_next)
            
