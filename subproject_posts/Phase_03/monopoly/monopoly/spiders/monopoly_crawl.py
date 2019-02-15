from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
import MySQLdb
import json
import myutils
import re
import xpaths

class Monopoly(scrapy.Spider):
    name = "monopoly_posts"

    def __init__(self, *args, **kwargs):
        super(Monopoly,  self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(db="posts_monopoly",host="localhost",user="root",passwd="qwerty123" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()


    def start_requests(self):
        headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Origin': 'https://monopoly.ms',
            'Upgrade-Insecure-Requests': '1',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'https://monopoly.ms/',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        data = {
            'vb_login_username': 'inqspdr',
            'vb_login_password': '',
            's': '',
            'securitytoken': 'guest',
            'do': 'login',
            'vb_login_md5password': 'c823b3822a485d448a68ee415f5eea59',
            'vb_login_md5password_utf': 'c823b3822a485d448a68ee415f5eea59'
        }
        url = 'https://monopoly.ms/login.php?do=login'
        yield FormRequest(url, callback=self.parse_next, formdata=data)

    def parse_next(self, response):
        headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
        cooks = response.headers.getlist('Set-Cookie')
        cookies = {}
        [cookies.update({c.split(";")[0].split("=")[0]:c.split(";")[0].split("=")[1]}) for c in cooks]
        url = 'https://monopoly.ms/'
        yield Request(url, callback=self.parse_next1, headers = headers,cookies = cookies)

    def parse_next1(self, response):
        forums = response.xpath(xpaths.FORUMS).extract()
        for forum in forums:
            forum = "https://monopoly.ms/" + forum
            headers = response.request.headers
            yield Request(forum, callback=self.parse_next2, headers = headers)
        

    def parse_next2(self, response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREAD_URLS).extract()
        for thread_url in thread_urls:
            thread_url = "https://monopoly.ms/" + thread_url
            sk = re.findall('\d+',thread_url)
            query_status = myutils.generate_upsert_query_posts_crawl('posts_monopoly')
            json_posts = {'sk':sk,
                          'post_url':thread_url,
                          'crawl_status':0,
                          'reference_url':response.url
                        }
            self.cursor.execute(query_status, json_posts)
        inner_threads = sel.xpath(xpaths.INNER_THREADS).extract()
        for inner_thread in inner_threads:
            inner_thread = "https://monopoly.ms/" + inner_thread
            headers = response.request.headers
            yield Request(inner_thread, callback=self.parse_next2, headers = headers)
     