import MySQLdb
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import json
import xpaths
from MySQLdb import OperationalError
import sys
from elasticsearch import Elasticsearch
import hashlib
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
A_QUE = utils.generate_upsert_query_authors("hellbound")


class Hellbound(scrapy.Spider):
    name = "hellbound_authors"

    def __init__(self):
        self.conn ,self.cursor = self.mysql_conn()
        self.es = Elasticsearch(['10.2.0.90:9342'])
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
        conn = MySQLdb.connect(db= "hellbound", host = "localhost", user="tls_dev",passwd="hdrn!",use_unicode=True,charset="utf8mb4")
        cursor = conn.cursor()
        return conn,cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = "select distinct(links) from hellbound_authors_crawl where crawl_status = 0"
        self.cursor.execute(select_que)
	self.conn.commit()
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select DISTINCT(auth_meta) from hellbound_authors_crawl where links = "%s"'%url
            self.cursor.execute(meta_query)
	    self.conn.commit()
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))
            meta = {'publish_epoch':set(activetime)}
            yield Request(url, callback = self.parse_meta,meta = meta)

    def parse_meta(self,response):
        json_val = {}
        publish_epoch = response.meta.get('publish_epoch','')
        author_name = ''.join(response.xpath('//title/text()').extract()).replace("'s Profile | Hellbound Hackers",'')
        json_val.update({
                        'reputation':' ',
                        'domain':"www.hellboundhackers.org",
                        'username':author_name,
                        })
        author_signature = utils.clean_text(' '.join(response.xpath(xpaths.AUTHOR_SIGNATURA).extract()))
        join_date = ''.join(response.xpath(xpaths.JOINED_DATE).extract())
        join_date = utils.time_to_epoch(join_date, "%B %d %Y - %H:%M:%S") or 0
        last_active = ''.join(response.xpath(xpaths.LAST_ACTIVE).extract())
        last_active = utils.time_to_epoch(last_active, "%B %d %Y - %H:%M:%S") or 0
        total_posts = ''.join(response.xpath(xpaths.TOTAL_POSTS).extract())
        active_time = utils.activetime_str(publish_epoch,total_posts)
        fetch_time = utils.fetch_time()
        groups = ''.join(response.xpath(xpaths.GROUP).extract())
        rank = ''.join(response.xpath('//td[contains(text(),"Rank:")]/following-sibling::td//text()').extract())
        json_val.update({
                    'auth_sign': author_signature,
                    'join_date':join_date,
                    'lastactive':last_active,
                    'totalposts': total_posts,
                    'fetch_time':fetch_time,
                    'credits':" ",
                    'awards': " ",
                    'rank':rank,
                    'groups' : groups,
                    'activetimes': active_time,
                    })
        icq = ''.join(response.xpath(xpaths.ICQ).extract()).replace('Not Specified','').encode('utf8')
        msn = ''.join(response.xpath(xpaths.MSN).extract()).replace('Not Specified','').encode('utf8')
        yahoo = ''.join(response.xpath(xpaths.YAHOO).extract()).replace('Not Specified','').encode('utf8')
        email = ''.join(response.xpath(xpaths.EMAIL).extract()).replace('Not Specified','').encode('utf8')
        aim = ''.join(response.xpath(xpaths.AIM).extract()).replace('Not Specified','').encode('utf8')
        website = ''.join(response.xpath(xpaths.WEBSITE).extract() or response.xpath(xpaths.WEBSITE1).extract()).replace('Not Specified','').encode('utf8')
        contact_info = []
        if website:
            contact_info.append({"channel":"Website URL:","user_id":website})
        if icq:
            contact_info.append({"channel":"ICQ#:","user_id":icq})
        if msn:
            contact_info.append({"channel":"MSN ID:","user_id":msn})
        if yahoo:
            contact_info.append({"channel":"Yahoo ID:","user_id":yahoo})
        if email:
            contact_info.append({"channel":"Email Address:","user_id":email})
        if aim:
            contact_info.append({"channel":"AIM:","user_id":aim})
        if contact_info == []:
            contact_info.append({"channel":" ", "user_id": " "})
        json_val.update({
                    'contact_info': str(contact_info),
                    })
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(author_name)).hexdigest(), body=json_val)
	if author_name or response.url == "https://www.hellboundhackers.org/user/.html":
            UP_QUE_TO_1 = 'update hellbound_authors_crawl set crawl_status = 1 where links = "%s"'%response.url
            try:
		self.cursor.execute(UP_QUE_TO_1)
		self.conn.commit()
	    except  OperationalError as e:
                if 'MySQL server has gone away' in str(e):
                    self.conn,self.cursor = self.mysql_conn()
		    self.cursor.execute(UP_QUE_TO_1)
		    self.conn.commit()
                else:raise e()
