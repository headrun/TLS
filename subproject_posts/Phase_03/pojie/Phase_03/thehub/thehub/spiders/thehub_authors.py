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
import time
from urllib import urlencode
upsert_query_authors = utils.generate_upsert_query_authors('thehub_tor')


class Thehub(scrapy.Spider):
    name = "thehub_authors"
    start_urls=["http://thehub7xbw4dc5r2.onion"]
    
    def __init__(self):
        self.conn = MySQLdb.connect(
            db="thehub_tor",
            host="localhost",
            user="root",
            passwd="1216",
            use_unicode=True,
            charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()
    
    def parse(self, response):
        sel = Selector(response)
        name = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@name').extract())
        value = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@value').extract())
        params = (
    ('action', 'login2'),
)
        data={
name : value,
'cookielength':'',
'hash_passwrd':'',
'passwrd'       :'!nq!nq7@6',
'user': 'inqspdr',
}
        url = 'http://thehub7xbw4dc5r2.onion/index.php?' +urlencode(params)
        yield FormRequest(url,formdata=data,callback=self.parse_authors)



    def parse_authors(self, response):
        urls = []
        que = "select DISTINCT(links) from thehub_crawl"
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for da in data:
            urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from thehub_crawl where links = "%s"'%url.encode('utf8')
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
        domain = 'thehub7xbw4dc5r2.onion'
	username = ''.join(response.xpath('//div[@class="username"]//h4/text()').extract())
        if username:
            query = 'update thehub_status set crawl_status = 1 where reference_url = %(url)s'
            json_data = {'url':response.url}
            self.cursor.execute(query,json_data)
        groups = ''.join(response.xpath('//span[@class="position"]/text()').extract())
	totalposts = ''.join(response.xpath('//dt[contains(text(),"Posts: ")]//following-sibling::dd/text()').extract()).split('(')[0]
	last_active = ''.join(response.xpath('//dl[@class="noborder"]//dt[contains(text(),"Date Registered: ")]//following-sibling::dd[3]/text()').extract())
	last =  datetime.datetime.strptime(last_active,'%B %d, %Y, %H:%M:%S %p')
        lastactive = time.mktime(last.timetuple())*1000
	join_date = ''.join(response.xpath('//dl[@class="noborder"]//dt[contains(text(),"Date Registered: ")]//following-sibling::dd[1]/text()').extract())
	join = datetime.datetime.strptime(join_date,'%B %d, %Y, %H:%M:%S %p')
        joindate = time.mktime(join.timetuple())*1000
	author_sign = '\n'.join(response.xpath('//div[@class="signature"]//text()').extract()).replace('Signature:','').strip()
        activetimes_ =  response.meta.get('publish_epoch')
        activetimes = []
        activetimes = utils.activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        json_data.update({'user_name': username,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'author_signature':author_sign,
                          'join_date': joindate,
                          'last_active': lastactive,
                          'total_posts': totalposts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': '',
                          'credits': '',
                          'awards': '',
                          'rank': '',
                          'active_time': activetimes,
                          'contact_info': '',
                          'reference_url': response.url
         })
        self.cursor.execute(upsert_query_authors, json_data)
	self.conn.commit()
