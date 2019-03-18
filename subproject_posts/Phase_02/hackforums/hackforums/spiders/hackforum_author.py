from dbc_utils import get_googlecaptcha
from urllib import urlencode
import cfscrape
import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
import scrapy
import MySQLdb
import time
import datetime
import re
import json
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
from scrapy.selector import Selector
import tls_utils as utils
upsert_query_authors = utils.generate_upsert_query_authors('posts_hackforums')


class Hackforums(scrapy.Spider):
    name = "hackforums_author"

    def __init__(self):
	self.conn,self.cursor = self.mysql_conn()
	self.count = 0
	self.que_val = utils.que_filename('hackforums_author')
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
	conn = MySQLdb.connect(db="posts_hackforums",host="localhost",user="root", passwd="", use_unicode=True, charset='utf8')
	cursor = conn.cursor()
	return conn,cursor

    def close_conn(self, spider):
        filename = self.que_val.name.split('/')[-1]
        self.que_to_mysql(filename)
        self.conn.commit()
        self.conn.close()

    def que_to_mysql(self,filename):
        conn,cursor = self.mysql_conn()
        utils.quefile_to_mysql(filename,upsert_query_authors,conn,cursor)
        cursor.close()
        conn.close()

    def start_requests(self):
        scraper = cfscrape.create_scraper()
        '''r = scraper.get('https://hackforums.net/member.php?action=login')
        cap_capth = '//script[@data-type="normal"]//@data-sitekey'
        sel = Selector(text=r.text)
        google_captcha = ''.join(sel.xpath(cap_capth).extract())
        data_ray = ''.join(sel.xpath('//script[@data-type="normal"]//@data-ray').extract())
        if data_ray and google_captcha:
            g_captcha =  get_googlecaptcha('https://hackforums.net/member.php?action=login',google_captcha)
            parameters = {
                    'id': data_ray,
                    'g-recaptcha-response': g_captcha
                }
            captcha_url = 'https://hackforums.net/cdn-cgi/l/chk_captcha?' +urlencode(parameters)
            r2 = scraper.get(captcha_url)
	'''
        data = {
            'username': 'kerspdr',
            'password': 'Inqspdr2018.',
            'quick_gauth_code': '',
            'remember': 'yes',
            'submit': 'Login',
            'action': 'do_login',
            'url': ''
        }
        login_url = 'https://hackforums.net/member.php?action=login'
        r3 = scraper.post(login_url, data=data)
        cookies = r3.cookies.get_dict()
        cooks = r3.request.headers.get('Cookie').split(";")
        [cookies.update({cook.split("=")[0]:cook.split("=")[1]})
         for cook in cooks]
        next_url = 'https://hackforums.net/showthread.php?tid=5926452'
        headers = {
            'authority': 'hackforums.net',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': r3.request.headers.get('User-Agent'),
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'referer': 'https://hackforums.net/',
        }
        urls = []
        que = "select DISTINCT(links) from hackforum_crawl where crawl_status = 0"
        self.cursor.execute(que)
	self.conn.commit()
        data = self.cursor.fetchall()
        for da in data:
            urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from hackforum_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
	    self.conn.commit()
            meta_query = self.cursor.fetchall()
            activetime = []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))

            publish_epoch = set(activetime)
            if url and meta:
                yield Request(url, callback=self.parse_details, headers = headers,cookies= cookies, meta = {'publish_epoch':publish_epoch})

    def parse_details(self, response):
	 if "The member you specified is either invalid or doesn't exist." in response.body:
	     query = 'update hackforum_crawl set crawl_status = 9 where links = %(url)s'
	     json_data_ = {'url':response.url}
	     self.cursor.execute(query,json_data_)
	     self.conn.commit()
	     return None

         sel = Selector(response)
         json_data = {}
         domain = 'www.hackforums.net'
         username = ''.join(sel.xpath(xpaths.USERNAME).extract())
	 reference_url = response.url
         if username:
             query = 'update hackforum_crawl set crawl_status = 1 where links = %(url)s'
             json_data_ = {'url':reference_url}
             self.cursor.execute(query,json_data_)
	     self.conn.commit()
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
                 join_date = 0
         lastactives = ''.join(sel.xpath(xpaths.LASTACTIVES).extract())
         if '(Hidden)' in lastactives:
             lastactive = 0
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
                 activetime ="""[{ "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),count)
                 activetimes.append(activetime)
             except:
		 pass
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
         self.count += 1
         if self.count > 1000:
             self.count = 0
             filename = self.que_val.name.split('/')[-1]
             self.que_val.close()
             self.que_to_mysql(filename)
             self.que_val = utils.que_filename('hackforums_author')
             self.que_val.write(json.dumps(json_data) + '\n')
         else:
             self.que_val.write(json.dumps(json_data) + '\n')
