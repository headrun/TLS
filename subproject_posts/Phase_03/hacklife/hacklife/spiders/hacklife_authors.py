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
import re
import xpaths
upsert_query_authors = utils.generate_upsert_query_authors('posts_hacklife')


class Hack_life(scrapy.Spider):
    name = "hacklife_authors"
    handle_httpstatus_list =[403]
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
        url="https://hack-life.net/"
        yield Request(url, callback=self.parse_authors, headers=headers,cookies = cookies)

    def parse_authors(self, response):
        urls = []
        que = "select DISTINCT(links) from hacklife_crawl"
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for da in data:
            urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from hacklife_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime = []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))

            publish_epoch = set(activetime)
            if url and meta:
                yield Request(url, callback=self.parse_details, headers = response.request.headers , meta={'publish_epoch':publish_epoch})

    def parse_details(self, response):
        json_data = {}
        domain = 'www.hack-life.net'
        username = ''.join(response.xpath(xpaths.USERNAME).extract())
        if username:
            query = 'update hacklife_status set crawl_status = 1 where reference_url = %(url)s'
            json_data = {'url':response.url}
            self.cursor.execute(query,json_data)
        groups = ''.join(response.xpath(xpaths.GROUPS).extract())
        lastactive = ''.join(response.xpath(xpaths.LASTACTIVE).extract())
        joindate =''.join(response.xpath(xpaths.JOINDATE).extract())
        credit =''.join(response.xpath(xpaths.CREDITS).extract()).replace('\n','').replace('\t','')
        credits = re.sub('\s\s+', '',credit)
        rank = ''.join(response.xpath(xpaths.RANK).extract())
        totalpost =''.join(response.xpath(xpaths.TOTALPOSTS).extract()).replace('\n','').replace('\t','')
        totalposts = re.sub('\s\s+', '', totalpost)
        activetimes_ =  response.meta.get('publish_epoch')
        activetimes = []
        activetimes = utils.activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        json_data.update({'user_name': username,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'author_signature':'',
                          'join_date': joindate,
                          'last_active': lastactive,
                          'total_posts': totalposts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': '',
                          'credits': credits,
                          'awards': '',
                          'rank': rank,
                          'active_time': activetimes,
                          'contact_info': '',
                          'reference_url': response.url
         })

        self.cursor.execute(upsert_query_authors, json_data)


