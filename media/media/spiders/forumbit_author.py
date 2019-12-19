import scrapy
from scrapy.spider import Spider
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from scrapy.selector import Selector
from scrapy.http import Request
import datetime,csv
import time
import MySQLdb
import json
import utils
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import re
import xpaths
upsert_query_authors = utils.generate_upsert_query_authors('posts_forumbit')


class Forummedia(scrapy.Spider):
    name="forumbit_author"
    start_urls=["https://forum.bits.media/index.php"]

    def __init__(self,*args,**kwargs):
       self.conn = MySQLdb.connect(db="posts_forumbit",host="localhost",user="root",passwd="1216",use_unicode=True,charset="utf8")
       self.cursor = self.conn.cursor()
       select_query = 'select DISTINCT(links) from forumbit_crawl;'
       self.cursor.execute(select_query)
       self.data = self.cursor.fetchall()
       dispatcher.connect(self.close_conn, signals.spider_closed)


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self,response):
        urls = []
        for da in self.data:
            urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from forumbit_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))

            publish_epoch = set(activetime)
            meta = {'publish_epoch':publish_epoch}
            if url and meta:
                yield Request(url, callback=self.parse_author,meta = meta)

    def parse_author(self, response):
        sel = Selector(response)
        domain = "www.forum.bits.media"
        json_data = {}
        user_name = ''.join(sel.xpath(xpaths.USERNAME).extract())
        username = re.sub('\s\s+', ' ', user_name)
        if user_name:
            query = 'update forumbit_status set crawl_status = 1 where reference_url = %(url)s'
            json_data = {'url':response.url}
            self.cursor.execute(query,json_data)

        totalpost = ''.join(sel.xpath(xpaths.TOTALPOSTS).extract())
        totalposts = re.sub('\s\s+', ' ', totalpost)
        activetime_ = response.meta.get("publish_epoch")
        activetime = []
        activetime = utils.activetime_str(activetime_,totalposts)

        joindates = ''.join(sel.xpath(xpaths.JOINDATE).extract())
        try:
            joindate = datetime.datetime.strptime(joindates, '%d %b %Y, %H:%M')
            join_date = time.mktime(joindate.timetuple())*1000
        except:
            joindate = datetime.datetime.strptime(joindates, ' %d %b %Y, %H:%M')
            join_date = time.mktime(joindate.timetuple())*1000

        lastactives = ''.join(sel.xpath(xpaths.LASTACTIVE).extract()).replace('\n','').replace('\t','')
        lastactive = datetime.datetime.strptime(lastactives, '%Y-%m-%dT%H:%M:%SZ')
        last_active = time.mktime(lastactive.timetuple())*1000

        groups = ''.join(sel.xpath(xpaths.GROUPS).extract())
        reputation = ''.join(sel.xpath(xpaths.REPUTATIONS).extract())
        ranks = ''.join(sel.xpath(xpaths.RANK).extract())
        rank = re.sub('\s\s+', ' ', ranks)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        json_data.update({'user_name': username,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'author_signature': '',
                          'join_date': join_date,
                          'last_active': last_active,
                          'total_posts': totalposts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': reputation,
                          'credits': '',
                          'awards': '',
                          'rank': rank,
                          'active_time': activetime,
                          'contact_info': '',
                          'reference_url': response.url
        })
        self.cursor.execute(upsert_query_authors, json_data)






