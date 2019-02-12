import datetime
from datetime import timedelta
import scrapy
import re
import json
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
import cfscrape
import requests
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import xpaths

QUE = utils.generate_upsert_query_posts_crawl('posts_hacklife')


class Hacklife(scrapy.Spider):
    name = "hacklife_crawl"

    def __init__(self):
        self.conn = MySQLdb.connect(
            db="posts_hacklife",
            host="127.0.0.1",
            user="root",
            passwd="1216",
            use_unicode=True,
            charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        log_in_url = 'https://hack-life.net/login/login'
        scraper = cfscrape.CloudflareScraper()
        r1 = scraper.get(log_in_url)
        sel = Selector(text=r1.text)
        token = ''.join(sel.xpath('//input[@name="_xfToken"]/@value').extract())
        data = {
            'login': 'inqspdr',
            'password': 'h@ck!nqspdr',
            'remember': '1',
            '_xfRedirect': '',
            '_xfToken': token,
        }
        r2 = scraper.post(log_in_url,data = data)
        headers = {
    'authority': 'hack-life.net',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': r2.request.headers.get('User-Agent'),
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}
        cookies = r2.cookies.get_dict()
        cooks = r2.request.headers.get('Cookie').split(";")
        [cookies.update({cook.split("=")[0]:cook.split("=")[1]}) for cook in cooks]
        response = Selector(text=r2.text)
        forms_urls = response.xpath(xpaths.FORUMS).extract()
        return (Request(form,
                        callback=self.parse_next,
                        headers=headers,
                        cookies=cookies) for form in forms_urls)

    def parse_next(self, response):
        threads = response.xpath(xpaths.THREADS).extract()
        for url in threads:
            json_val = {
               'sk': ''.join(re.findall('.\d+/', url)).replace('.', '').replace("/", ''),
               'post_url':  url,
               'crawl_status': 0,
               'reference_url': response.url
            }
            self.cursor.execute(QUE, json_val)

        for page_nav in set(response.xpath(xpaths.PAGENAV).extract()):
            if page_nav:
                yield Request(page_nav, callback=self.parse_next,headers=response.request.headers)



