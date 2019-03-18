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


class Hackforums(scrapy.Spider):
    name ="hackforums_crawl"
    handle_httpstatus_list = [403]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts_hackforums", host="localhost", user="root", passwd="", use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
	scraper = cfscrape.create_scraper()
	scraper = cfscrape.create_scraper(delay=10)
	'''import pdb;pdb.set_trace()
        r = scraper.get('https://hackforums.net/')
        cap_capth = '//script[@data-type="normal"]//@data-sitekey'
        sel = Selector(text=r.text)
        google_captcha = ''.join(sel.xpath(cap_capth).extract())
        data_ray = ''.join(sel.xpath('//script[@data-type="normal"]//@data-ray').extract())
        if data_ray and google_captcha:
            g_captcha =  get_googlecaptcha('https://hackforums.Inqspdr2018.net/member.php?action=login',google_captcha)
            parameters = {
                    'id': data_ray,
                    'g-recaptcha-response': g_captcha
                }
            captcha_url = 'https://hackforums.net/cdn-cgi/l/chk_captcha?' +urlencode(parameters)
            r2 = scraper.get(captcha_url)
	'''
        data = {
            'username': 'kerspdr',
            'password': 'Inqspdr2018.',
            'quick_gauth_code': '',
            'remember': 'yes',
            'submit': 'Login',
            'action': 'do_login',
            'url': ''
        }
        login_url = 'https://hackforums.net/member.php'#?action=login'
        r3 = scraper.post(login_url, data=data)
        cookies = r3.cookies.get_dict()
        cooks = r3.request.headers.get('Cookie').split(";")
        [cookies.update({cook.split("=")[0]:cook.split("=")[1]})
         for cook in cooks]
        next_url = 'https://hackforums.net/index.php'
        headers = {
            'authority': 'hackforums.net',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
	    'user-agent': r3.request.headers.get('User-Agent'),
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'referer': 'https://hackforums.net/',
        }

	yield Request(next_url,headers= headers, cookies = cookies,callback = self.parse_next)
	
    def parse_next(self, response):
        sel = Selector(response)
        links = sel.xpath(xpaths.LINKS).extract()
        links.append('forumdisplay.php?fid=259')
        for link in links:
            link = 'https://hackforums.net/' + link
            yield Request(link, callback=self.parse_urls ,headers = response.request.headers)

    def parse_urls(self, response):
        sel = Selector(response)
        link = sel.xpath(xpaths.INNERLINKS).extract()
        for forum_link in link:
            forum_link = 'https://hackforums.net/' + forum_link
            yield Request(forum_link, callback=self.parse_nxt, headers = response.request.headers)

    def parse_nxt(self, response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREADURLS).extract()
        for thread_url in thread_urls:
	    json_posts = {}
            if 'https://hackforums.net/' not in thread_urls:
                thread_url = 'https://hackforums.net/' + thread_url
            else:
                thread_url
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
                page_nav = "https://hackforums.net/" + page_nav
                yield Request(page_nav, callback=self.parse_nxt, headers = response.request.headers)


