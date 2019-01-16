from dbc_utils import get_googlecaptcha
from urllib import urlencode
import cfscrape
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
import xpaths
import re
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

class Hackthis(scrapy.Spider):
    name="hackthis_author"

    def __init__(self,*args,**kwargs):
       self.conn = MySQLdb.connect(db="posts_hackthissite",host="localhost",user="root",passwd="",use_unicode=True,charset="utf8")
       self.cursor = self.conn.cursor()
       select_query = 'select DISTINCT(links) from hackthis_crawl;'
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
        domain = "http://www.hackthissite.org/"
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
            last_active = '-'

        fetch_time = utils.fetch_time()
        author_signature = ' '.join(sel.xpath(xpaths.AUTHOR_SIGNATURE).extract())
        if 'wrote:' in author_signature:
                author_signature = author_signature.replace('wrote:','wrote: Quote ')
        reputations = ''.join(sel.xpath(xpaths.REPUTATIONS).extract())
        if reputations:
            reputatin = "https://www.hackthissite.org/forums" + reputations
            reputation = reputatin.replace('./','/')
        else:
            reputation = ''
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

        json_data.update({'user_name': user_name,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'author_signature':  author_signature,
                          'join_date': join_date,
                          'last_active': last_active,
                          'total_posts': total_posts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': '',
                          'credits': '',
                          'awards': '',
                          'rank': rank,
                          'active_time': (''.join(activetime)),
                          'contact_info': str(contact_info),
                          'reference_url': reference_url
        })

        upsert_query_authors = utils.generate_upsert_query_authors('posts_hackthissite')
        self.cursor.execute(upsert_query_authors, json_data)






