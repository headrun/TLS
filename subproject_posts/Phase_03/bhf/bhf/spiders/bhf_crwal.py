import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
import  utils

query_status = utils.generate_upsert_query_posts_crawl('tls')

class TheGub(Spider):
    name="bhf_crawl"
    start_urls = ["https://bhf.io/"]

    def __init__(self,*args,**kwargs):
        self.conn = MySQLdb.connect(db= "tls", host = "localhost", user="root", passwd = "123", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()

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
            print post_url
            self.cursor.execute(query_status, json_posts)
        navigations = response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract()
        for navigation in set(navigations):
            navigation = "https://bhf.io" + navigation
            yield Request(navigation,callback=self.parse_next)

