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
        self.query = utils.generate_upsert_query_posts('pojie_cn')
        self.conn = MySQLdb.connect(db="POJIE_DB",
                                        host="localhost",
                                        user="root",
                                        passwd="root",
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
            post_id = ''.join(re.findall('.cn/(.*)',url_))
            que = "insert into pojie_status(post_id, links, crawl_status) values(%s,%s,0) \
                    ON DUPLICATE KEY update post_id =%s, links =%s, crawl_status=0"
            values = (post_id, url_, post_id, url_)
            self.cursor.execute(que, values)


        nxt_pg = ''.join(sel.xpath(NXT_PG).extract())
        if nxt_pg:
            if "http" not in nxt_pg: nxt_pg =  site_domain + nxt_pg
            yield Request(nxt_pg, callback=self.parse_next)





