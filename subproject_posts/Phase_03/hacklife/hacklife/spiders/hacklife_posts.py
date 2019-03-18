from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
import cfscrape
from scrapy.selector import Selector
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import xpaths
import re
import selenium
from selenium import webdriver
import time
from urlparse import urljoin
query_posts = utils.generate_upsert_query_posts('posts_hacklife')
crawl_query = utils.generate_upsert_query_authors_crawl('posts_hacklife')


class Hack_life(scrapy.Spider):
    name = "hacklife_posts"
    handle_httpstatus_list =[403]
    def __init__(self):
        self.conn = MySQLdb.connect(
            db="posts_hacklife",
            host="localhost",
            user="root",
            passwd="",
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
	driver.close()
        yield FormRequest(log_in_url,headers = headers,cookies= cookies,callback=self.after_login,formdata = data)

    def after_login(self, response):
        url_que = "select distinct(post_url) from hacklife_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread,headers = response.request.headers)

    def parse_thread(self, response):
	url_ = 'https://hack-life.net'
        if '/page-' in response.url:
            test = re.findall('/page-\d+',response.url)
            thread_url = response.url.replace(''.join(test),"")
        else:
            thread_url = response.url
        if '/page-' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'

        thread_title=response.xpath(xpaths.THREADTITLE).extract()
        try:
            category = response.xpath(xpaths.CATEGORY).extract()[1]
        except: pass
        try:
            sub_category = '["' + response.xpath(xpaths.SUBCATEGORY).extract()[2] + '"]'
        except: pass
        domain = "www.hack-life.net"
        post_title = ''
        nodes = response.xpath(xpaths.NODES)
	if u'Oops! Nous avons rencontr\xe9 quelques probl\xe8mes.' in response.body:
	    query = 'update hacklife_status set crawl_status = 5 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
        elif nodes:
            query = 'update hacklife_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
	else:
	    query = 'update hacklife_status set crawl_status = 9 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
        for node in nodes:
            post_id = ''.join(node.xpath(xpaths.POST_ID).extract()).replace('post-','')
            post_url = ''.join(node.xpath(xpaths.POST_URL).extract())
            publishtime = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
	    try:publishtime = int(publishtime) * 1000
	    except:pass
            text = '\n'.join(node.xpath(xpaths.TEXT).extract())
            text = text.replace('bbCodeBlock-expandContent','Quote ')
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            author = utils.clean_text(author.replace(u'[email\xa0protected]', ''))
            mails = node.xpath('//span[@class="__cf_email__"]//@data-cfemail').extract()
            for mail in mails:
                email = utils.decode_cloudflareEmail(mail)
                author = author.replace(mail,email)
            author_url = urljoin(url_,''.join(node.xpath(xpaths.AUTHOR_URL).extract()))
	    all_links = []
	    links = node.xpath(xpaths.LINKS).extract()
	    [all_links.append(urljoin(url_,i))for i in links]
            all_links = str(all_links)
            json_posts = {'domain': domain,
                          'crawl_type': crawl_type,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_epoch': publishtime,
                          'author': author,
                          'author_url': author_url,
                          'post_text': utils.clean_text(text),
                          'all_links': all_links,
                          'reference_url': response.url
            }
            self.cursor.execute(query_posts, json_posts)
            json_posts.update({
                'author_url': author_url,
                'all_links': all_links
            })
            meta = {'publish_epoch': publishtime}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
        page_nav = ''.join(set(response.xpath(xpaths.INNERPAGENAV).extract()))
        if page_nav:
            yield Request(page_nav, callback=self.parse_thread,headers=response.request.headers)




