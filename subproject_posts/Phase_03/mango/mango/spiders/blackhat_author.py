import scrapy
import sys
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import time
import re
import MySQLdb
import json
import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib

class BlackHat(scrapy.Spider):
   name="blackhat_author"
   handle_httpstatus_list = [404,403]
   def __init__(self):
       import pdb;pdb.set_trace()
       self.es = Elasticsearch(['10.2.0.90:9342'])
       self.conn ,self.cursor = self.mysql_conn()
       select_query = 'select DISTINCT(links) from blackhat_crawl;'
       self.cursor.execute(select_query)
       self.conn.commit()
       self.data = self.cursor.fetchall()
       self.query_authors = utils.generate_upsert_query_authors('posts_blackhat')
       dispatcher.connect(self.close_conn, signals.spider_closed)

   def mysql_conn(self):
       conn = MySQLdb.connect(db="posts_blackhat",host="localhost",user="root",passwd="",use_unicode=True,charset="utf8")
       cursor = conn.cursor()
       return conn,cursor

   def close_conn(self, spider):
       self.conn.commit()
       self.conn.close()

   def start_requests(self):
       urls = []
       for da in self.data:
           urls.append(da[0])
       for url in urls:
           meta_query = 'select DISTINCT(auth_meta) from blackhat_crawl where links = "%s"'%url.encode('utf8')
           self.cursor.execute(meta_query)
	   self.conn.commit()
           meta_query = self.cursor.fetchall()
           activetime=[]
           author_signature=[]
           for da1 in meta_query:
               meta = json.loads(da1[0])
               activetime.append(meta.get('publish_epoch',''))
               author_signature.append(meta.get('author_signature',''))
           publish_epoch = set(activetime)
           author_signature = set(author_signature)
           meta = {'publish_epoch':publish_epoch,'author_signature':author_signature}
           if url and meta:
               yield Request(url, callback=self.parse_author,meta = meta)

   def parse_author(self, response):
       sel = Selector(response)
       reference_url = response.url
       json_data = {}
       username = ''.join(sel.xpath(xpaths.USERNAME).extract())
       username = utils.clean_text(username.replace(u'[email\xa0protected]', ''))
       mails = response.xpath('//a[@class="__cf_email__"]//@data-cfemail').extract()
       for mail in mails:
           email = utils.decode_cloudflareEmail(mail)
           username = username.replace(mail,email)
       if username:
           query = 'update blackhat_crawl set crawl_status = 1 where links = %(url)s'
           json_data = {'url':reference_url}
           self.cursor.execute(query,json_data)
	   self.conn.commit()
       elif 'This member limits who may view their full profil' in response.body:
	   query = 'update blackhat_crawl set crawl_status = 9 where links = %(url)s'
	   json_data = {'url':reference_url}
 	   self.cursor.execute(query,json_data)
	   self.conn.commit()
       domain = "www.blackhatworld.com"
       activetime_ = response.meta.get("publish_epoch")
       author_signature = response.meta.get("author_signature")
       activetime = []
       CONTENT_COUNTS = ''.join(sel.xpath(xpaths.TOTALPOSTS).extract()).replace('Messages:','')
       CONTENT_COUNT = re.sub('\s\s+', '', CONTENT_COUNTS)
       total_post =''.join( CONTENT_COUNT)
       total_posts = re.sub('\s\s+', '', total_post)
       if total_posts == '':
           total_posts = ''
       activetime = utils.activetime_str(activetime_,total_posts)
       joindate = ''.join(sel.xpath(xpaths.JOINDATE).extract()).replace('Joined:','').replace('\n','').replace('\t','')

       try:
           joindates = datetime.datetime.strptime(joindate,'%b %d, %Y')
           join_date = time.mktime(joindates.timetuple())*1000
       except:
           join_date = '0'

       lastactives = ''.join(sel.xpath(xpaths.LASTACTIVE).extract())
       try:
           lastactive = datetime.datetime.strptime(lastactives,'%b %d, %Y')
           last_active = time.mktime(lastactive.timetuple())*1000
       except:
           last_active = '0'

       fetch_time = utils.fetch_time()

       groups = ''.join(sel.xpath(xpaths.GROUPS).extract())
       if groups == '':
           groups = ''
       reputations = ''.join(sel.xpath(xpaths.REPUTATIONS).extract()).replace('Positive ratings received:','').replace('\n','').replace('\t','')
       if reputations == '':
           reputations = ''
       facebook = ''.join(sel.xpath(xpaths.FACEBOOK).extract()).replace('Facebook:','').replace('\n','').replace('\t','')
       twitter = ''.join(sel.xpath(xpaths.TWITTER).extract()).replace('Twitter:','').replace('\n','').replace('\t','')
       yahooMessenger = ''.join(sel.xpath(xpaths.YAHOOMESSENGER).extract()).replace('Yahoo! Messenger:','').replace('\n','').replace('\t','')
       googletalk = ''.join(sel.xpath(xpaths.GOOGLETALK).extract()).replace('Google Talk:','').replace('\n','').replace('\t','')
       skype_id  = ''.join(sel.xpath(xpaths.SKYPEID).extract()).replace('Skype:','').replace('\n','').replace('\t','')
       aim = ''.join(sel.xpath(xpaths.AIM).extract()).replace('AIM:','').replace('\n','').replace('\t','')
       icq = ''.join(sel.xpath(xpaths.ICQ).extract()).replace('ICQ:','').replace('\n','').replace('\t','')
       contact_info = []
       if yahooMessenger:
           contact_info.append({"channel":"yahooMessenger","user_id":yahooMessenger})
       if skype_id :
           contact_info.append({"channel":"skype_id","user_id":skype_id})
       if googletalk:
           contact_info.append({"channel":"googletalk","user_id":googletalk})
       if facebook:
           contact_info.append({"channel":"facebook","user_id":facebook})
       if twitter:
           contact_info.append({"channel":"twitter","user_id":twitter})
       if aim:
           contact_info.append({"channel":"aim","user_id":aim})
       if icq:
           contact_info.append({"channel":"icq","user_id":icq})
       if contact_info == [] :
           contact_info.append({"channel":" ","user_id":""})

       json_data.update({'username': username,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'auth_sign':str(list(author_signature)),
                          'join_date': join_date,
                          'lastactive': last_active,
                          'totalposts': total_posts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': reputations,
                          'credits': '',
                          'awards': '',
                          'rank': '',
                          'activetimes': ''.join(activetime),
                          'contactinfo': str(contact_info),
                          'reference_url': reference_url
        })
       self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(username).hexdigest(), body=json_data)
