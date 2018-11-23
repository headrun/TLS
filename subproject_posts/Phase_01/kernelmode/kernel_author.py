
import datetime
import json
import MySQLdb
import time
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest

from kernelmode.spiders import xpaths
import utils


def extract_data(sel, xpath_, delim=''):
    return delim.join(sel.xpath(xpath_).extract()).strip()


def get_nodes(sel, xpath_):
    return sel.xpath(xpath_)


def extract_list_data(sel, xpath_):
    return sel.xpath(xpath_).extract()


class KernelPost(scrapy.Spider):
    name = "kernel_author"
    start_urls = ["http://www.kernelmode.info/forum/"]
    # handle_httpstatus_list=[404]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="",
                                    use_unicode=True,
                                    charset='utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self):
        self.conn.commit()
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

        yield FormRequest('http://www.kernelmode.info/forum/ucp.php', callback=self.parse_login, formdata=data)

    def parse_login(self, response):
        query = 'select DISTINCT(links) from kernel_crawl;'
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        urls = []
        for datum in data:
            urls.append(datum[0])
        for url in urls:
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
        user_name = extract_data(sel, xpaths.USER_NAME)

        user_name = user_name if user_name else None
        domain = "www.kernelmode.info"
        author_signature = extract_data(sel, xpaths.AUTHOR_SIGNATURE)
        if author_signature:
            author_signature = author_signature
        else:
            author_signature = ''

        joining_date = extract_data(sel, xpaths.JOINING_DATE)
        joindate = datetime.datetime.strptime(joining_date, '%a %b %d, %Y %I:%M %p')
        join_date = time.mktime(joindate.timetuple()) * 1000
        last_active_ts = extract_data(sel, xpaths.LAST_ACTIVE)
        try:
            last_active = datetime.datetime.strptime(last_active_ts, '%a %b %d, %Y %I:%M %p')
            last_active = time.mktime(last_active.timetuple()) * 1000
        except:
            last_active = 0
        total_posts = extract_data(sel, xpaths.TOTAL_POSTS).replace('\t', "").encode("utf-8")
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        groups = ','.join(sel.xpath('//div[@class="inner"]//dd/select[contains(@name,"g")]//text()').extract())
        rank = extract_data(sel, xpaths.AUTHOR_RANK)
        if not rank:
            rank = ' '

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
                active_time = ' '
                active_times.append(active_time)
        active_times = ',  '.join(active_times)
        json_author.update({
            'user_name': user_name,
            'domain': domain,
            'crawl_type': 'keep_up',
            'author_signature': author_signature,
            'join_date': join_date,
            'last_active': last_active,
            'total_posts': total_posts,
            'fetch_time': fetch_time,
            'groups': groups,
            'reputation': '',
            'credits': '',
            'awards': '',
            'rank': rank,
            'active_time': active_times,
            'contact_info': '',
            'reference_url': response.url
        })

        query_author = utils.generate_upsert_query_authors('kernel_mode')

        self.cursor.execute(query_author, json_author)
        self.conn.commit()


