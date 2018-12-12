import scrapy
import sys
reload(sys)
sys.setdefaultencoding("utf8")
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import time
import MySQLdb
import utils
import json

class HackerThread_Author(scrapy.Spider):
    name = 'hacker_threads_author'
    start_urls = ["https://www.hackerthreads.org/"]

    def __init__(self, *args, **kwargs):
	self.conn = MySQLdb.connect(db="POSTS_HACKERTHREADS",
                                    host="localhost",
                                    user="root",
                                    passwd="root",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor=self.conn.cursor()
        select_query = 'select DISTINCT(links) from hackerthreads_crawl ;'                                    
        self.cursor.execute(select_query)	
        self.data = self.cursor.fetchall()

    def parse(self, response):
        urls = []
        for data_ in self.data:
            urls.append(data_[0])
        for url in urls:
            meta_query =  'select DISTINCT(auth_meta) from hackerthreads_crawl where links = "%s"'%url.encode("utf8")
            self.cursor.execute(meta_query)		
            meta_query = self.cursor.fetchall()
            activetime, join_date, total_posts, author, group, author_signature = [], [], [], [], [], []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))
                join_date.append(meta.get('join_date',''))
                total_posts.append(meta.get('total_posts',''))
                author.append(meta.get('author',''))
                group.append(meta.get('group',''))
                author_signature.append(meta.get('author_signature',''))
            publish_epoch = set(activetime)
            join_date = set(join_date)
            total_posts = set(total_posts)
            author = set(author)
            author_signature = set(author_signature)
            groups_ = set(group)
            meta = {'publish_epoch': publish_epoch, \
                            'join_date':join_date, 'total_posts':total_posts, 'author':author,\
                            'author_signature':author_signature, 'groups_':groups_}
            if url and meta:
                yield Request(url, callback=self.parse_author, meta=meta)

    def parse_author(self, response):
        active_time = []
        json_data = {}
        count, author_sig, user_name, total_posts = "", "", "", ""
        domain = "www.hackerthreads.org"
        activetime_ = response.meta.get("publish_epoch")
        groups_ = response.meta.get("groups_","")
        total_posts = response.meta.get('total_posts','')
        for group in groups_: groups = str(group)
        author_sig = response.meta.get('author_signature','')
        for count in total_posts:
            total_posts = str(count)
        for activetime_1 in activetime_:
            try:
                dt = time.gmtime(int(activetime_1)/1000)
                activetime = """[ { 'year': '%s', 'month': '%s', 'dayofweek': '%s', \
                'hour': '%s', 'count': '%s' }]"""%(str(dt.tm_year),str(dt.tm_mon), \
                str(dt.tm_wday), str(dt.tm_hour), count)
                active_time.append(activetime)
            except:
                active_time = "-"
        joindate = response.meta.get("join_date","")
        for joindatie_ in joindate: join_date = str(joindatie_)
        for author_signa in author_sig:
            author_signature = str(author_signa)

        #for group in groups_: groups = str(group)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        username = response.meta.get("author","")
        for un in username: user_name = str(un)	

        json_data.update({'user_name': user_name,
                         'domain': domain,
                         'crawl_type': 'keep_up',
                         'author_signature': author_signature,
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
        upsert_query_authors = utils.generate_upsert_query_authors('hacker_threads')
        self.cursor.execute(upsert_query_authors, json_data)


