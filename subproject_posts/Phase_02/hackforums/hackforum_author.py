import sys
reload(sys)
sys.setdefaultencoding('utf8')
import json
import re
import datetime
import time
import MySQLdb
import scrapy
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import xpaths

class Hackforums(scrapy.Spider):
     name ="hackforums_author"

     def __init__(self, *args, **kwargs):
         super(Hackforums, self).__init__(*args, **kwargs)
         self.conn = MySQLdb.connect(db="posts_hackforums", host="localhost", user="root", passwd="", use_unicode = True, charset = 'utf8')
         self.cursor = self.conn.cursor()
         dispatcher.connect(self.close_conn, signals.spider_closed)

     def close_conn(self, spider):
         self.conn.commit()
         self.conn.close()

     def start_requests(self):
         url = "https://hackforums.net/"
         time.sleep(3)
         yield Request(url, callback=self.parse, meta={'proxy':'http://71.138.16.17:30421'})

     def parse(self, response):
         sel = Selector(response)
         headers = {
         'authority': 'hackforums.net',
         'pragma': 'no-cache',
         'cache-control': 'no-cache',
         'origin': 'https://hackforums.net',
         'upgrade-insecure-requests': '1',
         'content-type': 'application/x-www-form-urlencoded',
         'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
         'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,/;q=0.8',
         'referer': 'https://hackforums.net/member.php?action=login',
         'accept-encoding': 'gzip, deflate, br',
         'accept-language': 'en-US,en;q=0.9',
         }
         data = {
         'username': 'kerspdr',
         'password': 'Inqspdr2018.',
         'quick_gauth_code': '',
         'submit': 'Login',
         'action': 'do_login',
         'url': 'https://hackforums.net/'
         }

         url_form = "https://hackforums.net/member.php"
         time.sleep(5)
         yield FormRequest(url_form, callback=self.parse_author, formdata=data, meta={'proxy':'http://71.138.16.17:30421','wait': 2}, dont_filter=True)

     def parse_author(self, response):
         urls = []
         que = "select DISTINCT(links) from hackforum_crawl"
         self.cursor.execute(que)
         data = self.cursor.fetchall()
         for da in data:
             urls.append(da[0])
         for url in urls:
             meta_query = 'select DISTINCT(auth_meta) from hackforum_crawl where links = "%s"'%url.encode('utf8')
             self.cursor.execute(meta_query)
             meta_query = self.cursor.fetchall()
             activetime = []
             for da1 in meta_query:
                 meta = json.loads(da1[0])
                 activetime.append(meta.get('publish_epoch',''))

             publish_epoch = set(activetime)
             if url and meta:
                yield Request(url, callback=self.parse_details, meta = {'proxy':'http://71.138.16.17:30421','publish_epoch':publish_epoch,'wait': 2})

     def parse_details(self, response):
         sel = Selector(response)
         json_data = {}
         domain = 'www.hackforums.net'
         username = ''.join(sel.xpath(xpaths.USERNAME).extract())
         if username:
             query = 'update blackhat_status set crawl_status = 1 where reference_url = %(url)s'
             json_data = {'url':reference_url}
             self.cursor.execute(quey,json_data)

         author_signature = '  '.join(sel.xpath(xpaths.AUTHOR_SIGNATURE).extract())
         if 'mycode_quote' in author_signature:
             author_signature = author_signature.replace('mycode_quote', 'Quote ')
         joindates = ''.join(sel.xpath(xpaths.JOINED_DATE).extract())
         try:
             joindate = datetime.datetime.strptime(joindates, '%m-%d-%Y')
             join_date = time.mktime(joindate.timetuple())*1000
         except:
             try:
                 if '{1}' in joindates:
                     joindate = datetime.datetime.now() - datetime.timedelta(1)
                     join_date = int((datetime.datetime.now() - datetime.timedelta(1)).strftime("%s")) * (1000)
             except:
                 join_date = ''
         lastactives = ''.join(sel.xpath(xpaths.LASTACTIVES).extract())
         if '(Hidden)' in lastactives:
             lastactive = '(Hidden)'
         else:
             try:
                 last_active =  datetime.datetime.strptime(lastactives,'%m-%d-%Y, %I:%M %p')
                 lastactive =  time.mktime(last_active.timetuple())*1000
             except:
                 if '{1}' in lastactives:
                     try:
                         lastactive_yes = ''.join(lastactives.replace('{1}',datetime.datetime.now().strftime('%m-%d-%Y')))
                         last_active = datetime.datetime.strptime(lastactive_yes, '%m-%d-%Y, %I:%M %p')
                         lastactive =  time.mktime(last_active.timetuple())*1000
                     except:pass
         total_post = ''.join(sel.xpath(xpaths.TOTALPOSTS).extract()).split('(')[0].replace('\t',"")
         total_posts = re.sub('\s\s+', '',total_post)
         fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
         awards = ''.join(sel.xpath(xpaths.AWARDS).extract()).replace('Awards:','')
         try:
             ranks = ''.join(sel.xpath(xpaths.GROUPS).extract()[0])
             rank = re.sub('\s\s+', '',ranks)
         except: pass
         activetimes_ =  response.meta.get('publish_epoch')
         activetimes = []
         for activetime in activetimes_:
             try:
                 dt = time.gmtime(int(activetime)/1000)
                 counts = ''.join(sel.xpath(xpaths.TOTALPOSTS).extract()).split('(')[0].replace('\t',"")
                 count = re.sub('\s\s+', '',counts)
                 activetime ="""[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),count)
                 activetimes.append(activetime)
             except:
                 activetime = ' '
                 activetimes.append(activetime)
         json_data.update({'user_name': username,
                          'domain': domain,
                          'crawl_type': 'keep_up',
                          'author_signature':author_signature,
                          'join_date': join_date,
                          'last_active': lastactive,
                          'total_posts': total_posts,
                          'fetch_time': fetch_time,
                          'groups': '',
                          'reputation': '',
                          'credits': '',
                          'awards': awards,
                          'rank': utils.clean_text(rank),
                          'active_time': (''.join(activetimes)),
                          'contact_info': '',
                          'reference_url': response.url
         })
         upsert_query_authors = utils.generate_upsert_query_authors('posts_hackforums')
         self.cursor.execute(upsert_query_authors, json_data)

