
import datetime
import json
import MySQLdb
import time
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from MySQLdb import OperationalError
import scrapy
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
from kernelmode.spiders import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from elasticsearch import Elasticsearch
import hashlib
from pprint import pprint
import re
def extract_data(sel, xpath_, delim=''):
    return delim.join(sel.xpath(xpath_).extract()).strip()


def get_nodes(sel, xpath_):
    return sel.xpath(xpath_)


def extract_list_data(sel, xpath_):
    return sel.xpath(xpath_).extract()


class KernelPost(Spider):
    name = "kernel_author"
    start_urls = ["http://www.kernelmode.info/forum/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="tls_dev",
                                    passwd="hdrn!",
                                    use_unicode=True,
                                    charset='utf8')
        self.cursor = self.conn.cursor()
	self.es = Elasticsearch(['10.2.0.90:9342'])
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self):
        self.cursor.close()
        self.conn.close()

    def add_http(self, url):
        if 'http' not in url:
            url = url.replace('./', 'http://www.kernelmode.info/forum/')
        else:
            url = url.replace('./', '')
        return url

    def parse(self, response):
        sid = response.xpath('//input[@type="hidden"]/@value').extract()[0]
        data = {'username': 'kerspdr',
                'password': 'Inq2018.',
                'redirect': './ucp.php?mode=login',
                'sid': sid,
                'redirect': 'index.php',
                'login': 'Login'}

        yield FormRequest('https://www.kernelmode.info/forum/ucp.php?mode=login', callback=self.parse_login, formdata=data)

    def parse_login(self, response):
        query = 'select DISTINCT(links) from kernel_crawl;'
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for da in data:
	    url = da[0]
            meta_query = 'select auth_meta from kernel_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            publish_time = []
            thread_title = []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                publish_time.append(meta.get('publish_time', ''))
                thread_title.append(meta.get('thread_title', ''))
            publish_time = set(publish_time)
            thread_title = set(thread_title)
            meta = {'publish_time': publish_time, 'thread_title': thread_title}
            yield Request(url, callback=self.parse_author, meta=meta)

    def parse_author(self, response):
        sel = Selector(response)
        json_author ={}
        user_name = ''.join(response.xpath('//div[@class="col-md-7 col-sm-7"]//ul[@class="reset-list"]//li[contains(text(),"Username")]//span/text()').extract())

        user_name = user_name if user_name else ''
        domain = "www.kernelmode.info"
        author_signature = ''.join(response.xpath('//div[@class="signature"]//text()').extract())

        joining_date = ''.join(response.xpath('//ul[@class="recent-activity"]//li[@class="item"]//div[contains(text(),"Joined:")]/../span[@class="recent-detail"]//text()').extract()).strip()
        try:
            joindate = datetime.datetime.strptime(joining_date, '%a %b %d, %Y %I:%M %p')
            join_date = time.mktime(joindate.timetuple()) * 1000
  	except:
   	    join_date = 0
        last_active_ts = ''.join(response.xpath('//ul[@class="recent-activity"]//li[@class="item"]//span[contains(text(),"Last active:")]/../span[@class="recent-detail"]//text()').extract()).strip()
        try:
            last_active = datetime.datetime.strptime(last_active_ts, '%a %b %d, %Y %I:%M %p')
            last_active = time.mktime(last_active.timetuple()) * 1000
        except:
            last_active = 0
        total_posts = ''.join(response.xpath('//ul[@class="recent-activity"]//li[@class="item"]//span[contains(text(),"Total posts:")]/../span[@class="recent-detail"]//text()').extract()).strip()
	total_posts = re.sub(' \| Search(.*)','',total_posts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        groups = ''.join(response.xpath('//div[@class="col-md-3 col-sm-3"]//span[@class=" pos-abs r-25"]/..//small/text()').extract())
        rank = extract_data(sel, xpaths.AUTHOR_RANK)
        active_times_ = response.meta['publish_time']
        active_times = []
        thread_title = response.meta.get('thread_title', '-')
        for active_time in active_times_:
            try:
                dt = time.gmtime(int(active_time)/1000)
                count = extract_data(sel, xpaths.POSTS_COUNT).split('|')[0].replace('\t', "").encode("utf-8")
                active_time = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""\
                                 % (str(dt.tm_year),
                                    str(dt.tm_mon),
                                    str(dt.tm_wday),
                                    str(dt.tm_hour),
                                    count)
                active_times.append(active_time)
            except:
		pass
        active_times = ',  '.join(active_times)
        json_author.update({
            'username': user_name,
            'domain': domain,
            'auth_sign': author_signature,
            'join_date': join_date,
            'lastactive': last_active,
            'totalposts': total_posts,
            'fetch_time': fetch_time,
            'groups': groups,
            'reputation': '',
            'credits': '',
            'awards': '',
            'rank': rank,
            'activetimes': active_times,
            'contact_info': '',
        })
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(user_name)).hexdigest(), body=json_author)
