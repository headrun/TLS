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
import selenium
from selenium import webdriver
import time
from urlparse import urljoin
QUE = utils.generate_upsert_query_posts_crawl('posts_hacklife')


class Hacklife(scrapy.Spider):
    name = "hacklife_crawl"

    def __init__(self):
        self.conn = MySQLdb.connect(
            db="posts_hacklife",
            host="localhost",
            user="root",
            use_unicode=True,
            charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def open_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-extensions")
        options.add_argument('--headless')
        driver = webdriver.Chrome(chrome_options=options)
        return driver

    def start_requests(self):
        log_in_url = 'https://hack-life.net/login/login'
	driver = self.open_driver()
	time.sleep(2)
	driver.get(log_in_url)
	time.sleep(5)
	sel = Selector(text =driver.page_source)
        token = ''.join(sel.xpath('//input[@name="_xfToken"]/@value').extract())
	cooks = cooks = driver.get_cookies()
	cookies = {}
	for cook in cooks:
	    cookies.update({cook.get('name'):cook.get('value')})
        data = {
            'login': 'inqspdr',
            'password': 'h@ck!nqspdr',
            'remember': '1',
            '_xfRedirect': '',
            '_xfToken': token,
        }
	user_agent = driver.execute_script("return navigator.userAgent;")
        headers = {
    'authority': 'hack-life.net',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': user_agent,#r2.request.headers.get('User-Agent'),
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}
	yield FormRequest(log_in_url,headers = headers,cookies= cookies,callback=self.after_login,formdata = data)
    
    def after_login(self,response):
	cooks = response.request.headers.get('Cookie').split(';')
	cookies = {}
	url = 'https://hack-life.net/forums/donations-partages.130/'
        [cookies.update({cook.split("=")[0]:cook.split("=")[1]}) for cook in cooks]
	forms_urls = response.xpath(xpaths.FORUMS).extract()
	for form in forms_urls:
	    form = urljoin(url,form)
	    yield Request(form, callback=self.parse_next, headers=response.request.headers)
        #return (Request(form, callback=self.parse_next, headers=response.request.headers) for form in forms_urls)


    def parse_next(self, response):
	url_ = 'https://hack-life.net/forums/donations-partages.130/'
        threads = response.xpath(xpaths.THREADS).extract()
        for url in threads:
	    url = urljoin(url_,url)
            json_val = {
               'sk': ''.join(re.findall('.\d+/', url)).replace('.', '').replace("/", ''),
               'post_url':  url,
               'crawl_status': 0,
               'reference_url': response.url
            }
            self.cursor.execute(QUE, json_val)

        for page_nav in set(response.xpath(xpaths.PAGENAV).extract()):
            if page_nav:
		page_nav = urljoin(url_,page_nav)
                yield Request(page_nav, callback=self.parse_next,headers=response.request.headers)



