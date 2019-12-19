import sys
import cfscrape
from dbc_utils import get_googlecaptcha
from urllib import urlencode
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
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
import tls_utils as utils
import xpaths
import logging
import requests
from urlparse import urljoin

class Hackforums(scrapy.Spider):
    name ="hackforums_crawl"
    #handle_httpstatus_list = [403]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts_hackforums", host="localhost", user="tls_dev", passwd="hdrn!", use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()
    

    def start_requests(self):
        import pdb;pdb.set_trace()
        scraper = cfscrape.create_scraper(delay=10)
        res = scraper.get('https://hackforums.net/')
        data = {
            'username': 'blackstorm52@protonmail.com',
            'password': 'Iwanttos33forums',
            'quick_gauth_code': '',
            'remember': 'yes',
            'submit': 'Login',
            'action': 'do_login',
            'url': ''
        }
        login_url = 'https://hackforums.net/member.php'
        import pdb;pdb.set_trace()
        resp_body = scraper.post(login_url, data=data)
        cookies = resp_body.cookies.get_dict()
        cooks = resp_body.request.headers.get('Cookie').split(";")
        for cook in cooks:
            cookies.update({cook.split("=")[0]:cook.split("=")[1]})
        req_url = 'https://hackforums.net/index.php'
        headers = {
            'authority': 'hackforums.net',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'origin': 'https://hackforums.net',
            'upgrade-insecure-requests': '1',
            'content-type': 'application/x-www-form-urlencoded',
	    'user-agent': resp_body.request.headers.get('User-Agent'),
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3',
            'referer': 'https://hackforums.net/',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
        import pdb;pdb.set_trace()
        yield FormRequest(req_url, headers= headers, cookies = cookies, callback = self.parse_next)
    def parse_next(self, response):
        sel = Selector(response)
        import pdb;pdb.set_trace()
        #links = sel.xpath(xpaths.LINKS).extract()
	links = response.xpath('//div[@class="td-foat-left mobile-link"]//strong//a//@href').extract()
        #links.append('forumdisplay.php?fid=259')
        for link in links:
            link = urljoin('https://hackforums.net/' ,link)
            yield Request(link, callback=self.parse_urls ,headers = response.request.headers)
    
    def parse_urls(self, response):
        sel = Selector(response)
        link = sel.xpath(xpaths.INNERLINKS).extract()
        for forum_link in  link:
            forum_link = urljoin( 'https://hackforums.net/' , forum_link)
	    yield Request(forum_link, callback=self.parse_nxt, headers = response.request.headers)

    def parse_nxt(self, response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREADURLS).extract()
        for thread_url in thread_urls:
	    json_posts = {}
            thread_url = urljoin('https://hackforums.net/' ,thread_url)
            sk = ''.join(thread_url).split('tid=')[-1]
            query_status = utils.generate_upsert_query_posts_crawl('posts_hackforums')
            json_posts.update({'sk':sk,
                          'post_url':thread_url,
                          'crawl_status':0,
                          'reference_url':response.url
            })
            self.cursor.execute(query_status, json_posts)
	    self.conn.commit()

        for page_nav in set(sel.xpath(xpaths.PAGENAVIGATION).extract()):
            if page_nav:
                page_nav =urljoin("https://hackforums.net/", page_nav)
                yield Request(page_nav, callback=self.parse_nxt, headers = response.request.headers)


