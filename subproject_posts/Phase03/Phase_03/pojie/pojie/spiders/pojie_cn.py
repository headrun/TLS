import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import re
import datetime
import time
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
from pojie_cn_xpaths import *


class Pojie_CN(scrapy.Spider):
    name = "pojie_cn"
    start_urls = ['https://www.52pojie.cn/']

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="POJIE_DB",
                                        host="localhost",
                                        user="root",
                                        passwd="1216",
                                        use_unicode=True,
                                        charset="utf8mb4")
        self.cursor =  self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        sel = Selector(response)
        urls = sel.xpath(URLS).extract()
        for url in urls:
            if "http" not in url:
                url = site_domain + url
                yield Request(url, callback = self.parse_next)

    def parse_next(self, response):
        sel = Selector(response)
        thread_urls = sel.xpath(THREAD_URLS).extract()
        for url_ in thread_urls:
            if "http" not in url_: url_ = site_domain + url_
            sk = ''.join(re.findall('.cn/(.*)',url_))
            json_posts = {'sk':sk,
                          'post_url':url_,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            query_status = utils.generate_upsert_query_posts_crawl('pojie')
            self.cursor.execute(query_status, json_posts)


        nxt_pg = ''.join(sel.xpath(NXT_PG).extract())
        if nxt_pg:
            if "http" not in nxt_pg: nxt_pg =  site_domain + nxt_pg
            yield Request(nxt_pg, callback=self.parse_next)





