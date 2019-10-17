import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
import datetime
import time
import json
import re
import MySQLdb
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import xpaths

class Skyfraud(scrapy.Spider):
    name = "skyfraud_crawl"
    start_urls = ["https://sky-fraud.ru"]

    def __init__(self, *args, **kwargs):
        super(Skyfraud,  self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(db="posts",host="localhost" ,user="root",passwd="qwe123",  use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def parse(self, response):
        sel = Selector(response)
        links = sel.xpath(xpaths.LINKS).extract()
        for link in links:
            link = "https://sky-fraud.ru/" + link
            yield Request(link,callback= self.parse_nxt)

    def parse_nxt(self, response):
        sel = Selector(response)
        urls = sel.xpath(xpaths.URLS).extract()
        for url in urls:
            url = "https://sky-fraud.ru/" + url
            sk = re.findall('\d+',url)
            query_status = utils.generate_upsert_query_posts_crawl('posts_skyfraud')
            json_posts = {'sk':sk,
                          'post_url':url,
                          'crawl_status':0,
                          'reference_url':response.url
                        }
            self.cursor.execute(query_status, json_posts)
	num = response.xpath('//td[@class="alt1"]//a[@rel="next"]//@href').extract_first()
        if num:
            num= "https://sky-fraud.ru/" + num
	    yield Request(num,callback=self.parse_nxt)
