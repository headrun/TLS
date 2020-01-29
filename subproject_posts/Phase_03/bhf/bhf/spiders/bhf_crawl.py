import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import  tls_utils as utils
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

query_status = utils.generate_upsert_query_posts_crawl('bhf')

class TheGub(scrapy.Spider):
    name="bhf_crawl"
    start_urls = ["https://bhf.io/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db= "posts", host = "localhost", user="tls_dev", passwd = "hdrn!", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        sel = Selector(response)
        main_urls = sel.xpath('//h3[@class="node-title"]/a/@href').extract()
        for url in main_urls:
            url = "https://bhf.io" + url
            yield Request(url,callback=self.parse_next)
    def parse_next(self,response):
        sel = Selector(response)
        yield Request(response.url,callback=self.parse)
        sub_forums = sel.xpath('//div[@class="structItem-title"]/a[@data-tp-primary="on"]/@href').extract()
        for sub_forum in sub_forums:
            post_url= "https://bhf.io" + sub_forum
            sk = ''.join(sub_forum).split("/")[-2]
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
            self.conn.commit()
        navigations = ''.join(set(response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract()))
        if navigations:
            navigations = "https://bhf.io" + navigations
            yield Request(navigations,callback=self.parse_next)

