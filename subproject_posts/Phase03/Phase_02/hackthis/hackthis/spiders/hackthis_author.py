from urllib import urlencode
import cfscrape
import scrapy
#from scrapy.spider import Spider
import sys
from scrapy.selector import Selector
from scrapy.http import Request
import datetime,csv
import time
import MySQLdb
import json
import xpaths
import re
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib

class Hackthis(scrapy.Spider):
    name="hackthis_author"

    def __init__(self):
       self.es = Elasticsearch(['10.2.0.90:9342'])
       self.conn,self.cursor = self.mysql_conn()
       select_query = 'select DISTINCT(links) from hackthis_crawl where crawl_status =0'
       self.cursor.execute(select_query)
       self.conn.commit() 
       self.data = self.cursor.fetchall()
       dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):       
       conn = MySQLdb.connect(db="posts_hackthissite",host="localhost",user="tls_dev",passwd="hdrn!",use_unicode=True,charset="utf8")
       cursor = conn.cursor()
       return conn,cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        for url in self.data:
            url = url[0]
            meta_query = 'select DISTINCT(auth_meta) from hackthis_crawl where links = "%s"'%url.encode('utf8')
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
        json_data = {}
        reference_url = ''.join(re.sub('&sid=(.*)','',response.url))
        user_name = ''.join(sel.xpath(xpaths.USERNAME).extract())
        if user_name:
            query = 'update hackthis_status set crawl_status = 1 where reference_url = %(url)s'
            json_data = {'url':reference_url}
            self.cursor.execute(query,json_data)
        activetime_ = response.meta.get("publish_epoch")
        activetime = []
        CONTENTCOUNT = ''.join(sel.xpath(xpaths.TOTALPOSTS).extract())
        CONTENT_COUNT = CONTENTCOUNT.split('|')[0]
        total_posts =''.join( CONTENT_COUNT)
        activetime = utils.activetime_str(activetime_,total_posts)
        joindates = ''.join(sel.xpath(xpaths.JOINDATES).extract())
        try:
            joindate = datetime.datetime.strptime(joindates, '%a %b %d, %Y %H:%M %p')
            join_date = time.mktime(joindate.timetuple())*1000
        except:
            join_date = ''
        lastactives = ''.join(sel.xpath(xpaths.LASTACTIVES).extract())
        try:
            lastactive = datetime.datetime.strptime(lastactives, '%a %b %d, %Y %H:%M %p')
            last_active = time.mktime(lastactive.timetuple())*1000
        except:
	    try:
		if lastactives == ' - ':
		    last_active = 0
	    except:
		pass		

        fetch_time = utils.fetch_time()
        author_signature = ' '.join(sel.xpath(xpaths.AUTHOR_SIGNATURE).extract())
        if 'wrote:' in author_signature:
                author_signature = author_signature.replace('wrote:','wrote: Quote ')
        reputations = ''.join(sel.xpath(xpaths.REPUTATIONS).extract())
        reputation = re.sub('(.*)_','',reputations).replace('.gif','')
	groups = ','.join(sel.xpath(xpaths.GROUPS).extract())
        rank = ''.join(sel.xpath(xpaths.RANK).extract())
        email_address = ''.join(sel.xpath(xpaths.EMAILADDRESS).extract())
        website = ''.join(sel.xpath(xpaths.WEBSITES).extract())
        msnm = ''.join(sel.xpath(xpaths.MSNM).extract())
        yim = ''.join(sel.xpath(xpaths.YIM).extract())
        aim = ''.join(sel.xpath(xpaths.AIM).extract())

        contact_info = []
        if email_address:
            contact_info.append({"channel":"Email_Address","user_id":email_address})
        if website:
            contact_info.append({"channel":"Website","user_id":website})
        if msnm:
            contact_info.append({"channel":"MSNM","user_id":msnm})
        if yim:
            contact_info.append({"channel":"YIM","user_id":yim})
        if aim:
            contact_info.append({"channel":"AIM","user_id":aim})

        if contact_info == []:
            contact_info.append({"channel":"","user_id":""})

	json_data.update({'username': user_name,
                          'domain': 'www.hackthissite.org',
                          'auth_sign':  author_signature,
                          'join_date': join_date,
                          'lastactive': last_active,
                          'totalposts': total_posts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': reputation,
                          'credits': '',
                          'awards': '',
                          'rank': rank,
                          'activetimes': (''.join(activetime)),
                          'contact_info': str(contact_info),
        })

        upsert_query_authors = utils.generate_upsert_query_authors('posts_hackthissite')
        #self.cursor.execute(upsert_query_authors, json_data)
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(str(user_name)).hexdigest(), body=json_data)



