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
from selenium import webdriver
import selenium
from selenium.webdriver.support.wait import WebDriverWait
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

class BlackHat(scrapy.Spider):
   name="blackhat_author"
   start_urls=["https://www.blackhatworld.com/forums/"]
   handle_httpstatus_list = [403,503]
   def __init__(self,*args,**kwargs):
       self.conn = MySQLdb.connect(db="posts_blackhat",host="localhost",user="root",passwd="",use_unicode=True,charset="utf8")
       self.cursor = self.conn.cursor()
       select_query = 'select DISTINCT(links) from blackhat_crawl;'
       self.cursor.execute(select_query)
       self.data = self.cursor.fetchall()
       self.driver = open_driver()
       dispatcher.connect(self.close_conn, signals.spider_closed)

   def close_conn(self, spider):
       try:
           self.driver.quit()
       except Exception as exe:
           pass
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
       if "[email" in response.body and "protected]" in response.body:
           self.driver.get(response.url)
           time.sleep(1)
           WebDriverWait(self.driver, 10)
           reference_url = response.url
           sel = Selector(text = self.driver.page_source)
       else:
           sel = Selector(response)
           reference_url = response.url
       json_data = {}
       username = ''.join(sel.xpath(xpaths.USERNAME).extract())
       if username == '':
           username = ''
       domain = "www.blackhatworld.com"
       activetime_ = response.meta.get("publish_epoch")
       author_signature = response.meta.get("author_signature")
       activetime = []
       CONTENT_COUNT = ''.join(sel.xpath(xpaths.TOTALPOSTS).extract()).replace('Messages:','')
       for activetime_i in activetime_:
           try:
               dt = time.gmtime(int(activetime_i)/1000)
               activetime_i = """[ { "year": "%d","month": "%d", "dayofweek": "%d", "hour": "%d", "count": "%s" }]"""%\
                      (dt.tm_year,dt.tm_mon,dt.tm_wday,dt.tm_hour,CONTENT_COUNT)
               activetime.append(activetime_i)
           except:
               activetime.append('-')
       total_posts =''.join( CONTENT_COUNT)
       if total_posts == '':
           total_posts = ''

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

       fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000

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

def open_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--incognito")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument('--headless')
    driver = webdriver.Chrome(chrome_options=options)
    return  driver


