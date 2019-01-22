import scrapy
from scrapy.spider import Spider
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
from scrapy.selector import Selector
from scrapy.http import Request
import datetime,csv
import time
import re
import MySQLdb
import json
import utils
import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
import logging

class BlackHat(scrapy.Spider):
   name="blackhat_author"
   start_urls=["https://www.blackhatworld.com/forums/"]
   handle_httpstatus_list = [403,503]
   def __init__(self,*args,**kwargs):
       self.conn = MySQLdb.connect(db="posts_blackhat",host="localhost",user="root",passwd="1216",use_unicode=True,charset="utf8")
       self.cursor = self.conn.cursor()
       select_query = 'select DISTINCT(links) from blackhat_crawl;'
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
           meta_query = 'select DISTINCT(auth_meta) from blackhat_crawl where links = "%s"'%url.encode('utf8')
           self.cursor.execute(meta_query)
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

       if username == '':
           username = ''
       if username:
           query = 'update blackhat_status set crawl_status = 1 where reference_url = %(url)s'
           json_data = {'url':reference_url}
           self.cursor.execute(query,json_data)
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

       json_data.update({'user_name': username,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'author_signature':author_signature,
                          'join_date': join_date,
                          'last_active': last_active,
                          'total_posts': total_posts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': reputations,
                          'credits': '',
                          'awards': '',
                          'rank': '',
                          'active_time': (''.join(activetime)),
                          'contact_info': str(contact_info),
                          'reference_url': reference_url
        })
       upsert_query_authors = utils.generate_upsert_query_authors('posts_blackhat')
       self.cursor.execute(upsert_query_authors, json_data)




