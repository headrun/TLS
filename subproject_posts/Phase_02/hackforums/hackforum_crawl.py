import sys
reload(sys)
sys.setdefaultencoding('utf8')
import json
import re
import datetime
import time
import MySQLdb
import scrapy
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import xpaths
import logging

class Hackforums(scrapy.Spider):
    name ="hackforums_crawl"
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
        super(Hackforums, self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(db="posts_hackforums", host="localhost", user="root", passwd="", use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url = "https://hackforums.net/"
        time.sleep(3)
        yield Request(url, callback=self.parse)


    def parse(self, response):
        sel = Selector(response)
        headers = {
         'authority': 'hackforums.net',
         'pragma': 'no-cache',
         'cache-control': 'no-cache',
         'origin': 'https://hackforums.net',
         'upgrade-insecure-requests': '1',
         'content-type': 'application/x-www-form-urlencoded',
         'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
         'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,/;q=0.8',
         'referer': 'https://hackforums.net/member.php?action=login',
         'accept-encoding': 'gzip, deflate, br',
         'accept-language': 'en-US,en;q=0.9',
        }
        data = {
         'username': 'kerspdr',
         'password': 'Inqspdr2018.',
         'quick_gauth_code': '',
         'submit': 'Login',
         'action': 'do_login',
         'url': 'https://hackforums.net/'
        }
        url_form = "https://hackforums.net/member.php"
        time.sleep(5)
        yield FormRequest(url_form, callback=self.parse_next, formdata=data,meta={'proxy':'http://74.70.67.218:59112'}, dont_filter=True)

    def parse_next(self, response):
        sel = Selector(response)
        links = sel.xpath(xpaths.LINKS).extract()
        links.append('forumdisplay.php?fid=259')
        for link in links:
            link = 'https://hackforums.net/' + link
            yield Request(link, callback=self.parse_urls ,meta={'proxy':'http://74.70.67.218:59112'}, dont_filter=True)

    def parse_urls(self, response):
        sel = Selector(response)
        link = sel.xpath(xpaths.INNERLINKS).extract()
        for forum_link in link:
            forum_link = 'https://hackforums.net/' + forum_link
            yield Request(forum_link, callback=self.parse_nxt, meta={'proxy':'http://74.70.67.218:59112'}, dont_filter=True)

    def parse_nxt(self, response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREADURLS).extract()
        for thread_url in thread_urls:
            if 'https://hackforums.net/' not in thread_urls:
                thread_url = 'https://hackforums.net/' + thread_url
            else:
                thread_url
            sk = ''.join(thread_url).split('tid=')[-1]
            query_status = utils.generate_upsert_query_posts_crawl('posts_hackforums')
            json_posts = {'sk':sk,
                          'post_url':thread_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)

        for page_nav in set(sel.xpath(xpaths.PAGENAVIGATION).extract()):
            if page_nav:
                page_nav = "https://hackforums.net/" + page_nav
                yield Request(page_nav, callback=self.parse_nxt, meta={'proxy':'http://74.70.67.218:59112'})


