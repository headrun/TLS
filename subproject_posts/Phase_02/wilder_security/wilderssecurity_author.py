import scrapy
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from scrapy.selector import Selector
from scrapy.http import Request
import datetime, csv
import time
import MySQLdb
import json
import utils

class WilderAuthors(scrapy.Spider):
    name = 'wilderssecurity_author'
    start_urls = ['http://wilderssecurity.com/']
    handle_httpstatus_list=[403]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="POSTS_WILDER",
                                    host="localhost",
                                    user="root",
                                    passwd = "root",
                                    use_unicode=True,
                                    charset="utf8mb4")
	self.cursor=self.conn.cursor()
        select_query = 'select DISTINCT(links) from wilder_crawl ;'
        self.cursor.execute(select_query)
        self.data = self.cursor.fetchall()

    def parse(self, response):
        urls = []
        for da in self.data:
            urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from wilder_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime, join_date, total_posts, author,group_ = [], [], [], [], []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch', ''))
                join_date.append(meta.get('join_date', ''))
                total_posts.append(meta.get('total_posts', ''))
                author.append(meta.get('author', '')) 
                group_.append(meta.get('groups', ''))
            publish_epoch = set(activetime)
            join_date = set(join_date)
            total_posts = set(total_posts)
            author = set(author)
            groups_ = set(group_)
            meta = {'publish_epoch':publish_epoch, 'join_date':join_date, 'total_posts':total_posts, 'author':author, 'groups_':groups_}
            if url and meta:
                yield Request(url, callback=self.parse_author, meta=meta)

    def parse_author(self, response):
        active_time = []
        json_data = {}
        count, user_name, total_posts = "", "", ""
        domain = "www.wilderssecurity.com"
        activetime_ = response.meta.get("publish_epoch")
        totalposts = response.meta.get("total_posts","")
        groups_ = response.meta.get("groups_","")
        for count in totalposts:
            total_posts = str(count)
        for activetime_1 in activetime_:
            try:
                dt = time.gmtime(int(activetime_1)/1000)
                activetime = """[ { 'year': '%s', 'month': '%s', 'dayofweek': '%s', 'hour': '%s', 'count': '%s' }]"""%(str(dt.tm_year),str(dt.tm_mon), str(dt.tm_wday), str(dt.tm_hour), count)
                active_time.append(activetime)
            except:
                active_time = "-"
        joindate = response.meta.get("join_date","")
        for joindatie_ in joindate: join_date = str(joindatie_)
        for group in groups_: groups = str(group)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        username = response.meta.get("author","")
        for un in username: user_name = str(un)

        json_data.update({'user_name': user_name,
                         'domain': domain,
                         'crawl_type': 'keep_up',
                         'author_signature': "",
                         'join_date': join_date,
                         'last_active': "",
                         'total_posts': total_posts,
                         'fetch_time':fetch_time,
                         'groups': groups,
                         'reputation': '',
                         'credits': '',
                         'awards': '',
                         'rank': '',
                         'active_time': (''.join(active_time)),
                         'contact_info': '',
                         'reference_url': response.url
        })
        upsert_query_authors = utils.generate_upsert_query_authors('wilder_security')
        self.cursor.execute(upsert_query_authors, json_data)

