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
from urllib import urlencode
QUE = utils.generate_upsert_query_posts_crawl('xakfor')


class Xakfor(scrapy.Spider):
    name = "xakfor_thread_crawl"
    handle_httpstatus_list = [403]

    def __init__(self):
        self.conn = MySQLdb.connect(
            db="xakfor",
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

    def start_requests(self):
	session = requests.session()
	session.headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.84 Safari/537.36',
}
        scraper = cfscrape.create_scraper(sess=session)
        r = scraper.get("https://xakfor.net/forum/")
        headers = {'Accept': '*/*',
                   'Accept-Encoding': 'gzip, deflate',
                   'Connection': 'keep-alive',
                   'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.84 Safari/537.36'
                   }
	ele = Selector(text=r.text)
	cap_ = ''.join(ele.xpath('//script[@data-type="normal"]/@data-sitekey').extract())	
	import pdb;pdb.set_trace()
	from dbc_utils import get_googlecaptcha
	cap = get_googlecaptcha('https://xakfor.net/forum/', cap_)	
	bf_challenge_id = ''.join(re.findall("'bf_challenge_id', '\d+",r.text)).replace("'bf_challenge_id', '",'')
	params = (
    ('s', ''.join(ele.xpath('//input[@name="s"]/@value').extract())),
    ('id', ''.join(ele.xpath('//script[@data-type="normal"]/@data-ray').extract())),
    ('g-recaptcha-response', cap),
    ('bf_challenge_id', bf_challenge_id),
    ('bf_execution_time', '24'),
    ('bf_result_hash', '3820702122'),
)
	print urlencode(params)
	import pdb;pdb.set_trace()
	r1 = scraper.get('https://xakfor.net/cdn-cgi/l/chk_jschl?'+urlencode(params))
        request_cookies = r1.request._cookies.get_dict()
        response_cookies = r1.cookies.get_dict()
        cookies = {}  
        cookies.update(request_cookies)
        cookies.update(response_cookies)
        sel = Selector(text=r1.text)
        import pdb;pdb.set_trace()
        forms_urls = sel.xpath('//li[contains(@class,"node forum level_")]//h3[@class="nodeTitle"]/a/@href').extract()
        return (Request("https://xakfor.net/" + form,
                        callback=self.parse_next,
                        headers=headers,
                        cookies=cookies,
                        meta={'headers': headers,
                              'cookies': cookies}) for form in forms_urls)

    def parse_next(self, response):
        cookies = response.meta.get('cookies')
        headers = response.meta.get('headers')
        threads = response.xpath(
            '//div[@class="listBlock main"]//h3[@class="title"]//a[contains(@href,"threads/")]/@href').extract()
        for url in threads:
            json_val = {
                'sk': ''.join(re.findall('.\d+/', url)).replace('.', '').replace("/", ''),
                'post_url': 'https://xakfor.net/' + url,
                'crawl_status': 0,
                'reference_url': response.url,
            }
            self.cursor.execute(QUE, json_val)
        next_page = ''.join(set(response.xpath(
            '//div[@class="PageNav"]//a[contains(text()," >")]/@href').extract()))
        if next_page:
            yield Request("https://xakfor.net/" + next_page, callback=self.parse_next, headers=headers, cookies=cookies, meta={'headers': headers, 'cookies': cookies})
            


