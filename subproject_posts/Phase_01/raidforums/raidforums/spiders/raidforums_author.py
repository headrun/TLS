from scrapy.http import Request
import datetime
import scrapy
import time
import re
import MySQLdb
import json
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from ast import literal_eval
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib

HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Encoding': 'gzip,deflate',
           'Accept-Language': 'en',
           'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
           }


class Raidforums(scrapy.Spider):
    name = 'raidforums_author'

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="posts", host="localhost",
                                    user="root", passwd="qwe123", use_unicode=True, charset="utf8")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url = 'https://raidforums.com/'
        yield Request(url, self.parse, headers=HEADERS)

    def parse(self, response):
        cookie = literal_eval(json.loads(json.dumps(
            str(response.headers)))).get('Set-Cookie', [])
        cookies = {}
        for i in cookie:
            data = i.split(';')[0]
            if data:
                try:
                    key, val = data.split('=', 1)
                except:
                    continue
                cookies.update({key.strip(): val.strip()})
        cookies_input = {
            'RFLovesYou_mybb[lastactive]': str(int(cookies.get('RFLovesYou_mybb[lastactive]', '')) + 283),
            'RFLovesYou_mybb[lastvisit]': cookies.get('RFLovesYou_mybb[lastvisit]', ''),
            'RFLovesYou_sid': cookies.get('RFLovesYou_sid', ''),
            '__cfduid': cookies.get('__cfduid', '')
        }
        url = 'https://raidforums.com/index.php'
        yield Request(url, callback=self.parse_login, cookies=cookies_input, meta={'cook': cookies})

    def parse_login(self, response):
        cook = response.meta.get('cook', {})
        cookie = literal_eval(json.loads(json.dumps(
            str(response.headers)))).get('Set-Cookie', [])
        sid_cook = {}
        for i in cookie:
            data = i.split(';')[0]
            if data:
                try:
                    key, val = data.split('=', 1)
                except:
                    continue
                sid_cook.update({key.strip(): val.strip()})
        cookies = {'RFLovesYou_mybbuser': '121487493_xD4bBSh4So7MiiyrsEMCooopeXwMmGt0bOJi2FZ1dWKQZc0wMr',
                   'RFLovesYou_loginattempts': '1',
                   'RFLovesYou_mybb[lastactive]': str(int(cook.get('RFLovesYou_mybb[lastactive]', '')) + 628),
                   'RFLovesYou_mybb[lastvisit]': cook.get('RFLovesYou_mybb[lastvisit]', ''),
                   'RFLovesYou_sid': sid_cook.get('RFLovesYou_sid', ''),
                   '__cfduid': cook.get('__cfduid', '')}
        url = 'https://raidforums.com/User-Harrington'
        yield Request(url, self.parse_user,  cookies=cookies, meta={'cook': cookies})

        # fetch urls from DB using __init__ method
    def parse_user(self, response):
	select_qry = 'select DISTINCT(links) from raidforums_author_crawl where crawl_status = 0'
        self.cursor.execute(select_qry) 
	self.conn.commit()
        data = self.cursor.fetchall()
        cookies = response.meta.get('cook', {})
        for url in data:
	    url = ''.join(url)
            meta_query = 'select DISTINCT(auth_meta) from raidforums_author_crawl where links = "%s" ' % url.encode('utf8')
            self.cursor.execute(meta_query)
	    self.conn.commit()
            meta_query = self.cursor.fetchall()
            commant_date = []
            thread_type = []
            thread_Topics = []
            for da1 in meta_query:
                meta = json.loads(da1[0])
                commant_date.append(meta.get('publish_time', ''))
                thread_type.append(meta.get('thread_type', ''))
                thread_Topics.append(meta.get('thread_Topics', ''))
            commant_date = set(commant_date)
            thread_type = set(thread_type)
            thread_Topics = set(thread_Topics)
            author_meta = {'commant_date': commant_date, 'thread_type': ', '.join(thread_type).replace(
                '\n', ''), 'thread_Topics': ', '.join(thread_Topics).replace('\n', '')}
            yield Request(url, callback=self.parse_author,  cookies=cookies, meta=author_meta)

    def parse_author(self, response):
        json_data = {}
        others_social,skype_id,xbox,myspace,playstatio,facebook, google, credits,keybase,soundcloud,steamcommunity,twitter,youtube,flickr,instagram,last,linkedin  ="","","","","","","","","","","","","","","","",""
        user = ''.join(response.xpath(
            '//div[contains(@class, "user-name")]//span/text()').extract())
        if response.status != 200 or 302:
            status_code_update = 'update raidforums_author_crawl set crawl_status = 1 where links = "%(url)s"' %({'url':response.url})
            self.cursor.execute(status_code_update)
	    self.conn.commit()
        else:
            status_code_update = 'update raidforums_author_crawl set crawl_status = 9 where links = "%s"' %({'url':response.url})
	    self.cursor.execute(status_code_update)
            self.conn.commit()

        group = ''.join(response.xpath(
            '//span[@class="largetext protitlemain"]//span//@id | //span[@class="largetext protitlemain"]/strong/text()').extract())
        group = group.replace('[', '').replace(']', '')

        # Group details in required format
        # data in both tags or in string format so need to Heard coad
        if 'o-tag' in group:
            group = 'Owner'
        elif 'm-tag' in group:
            group = 'Moderator'
        elif 'mp-tag' in group:
            group = 'Moderator+'
        elif 'a-tag' in group:
            group = 'Admin'
        elif 'd-tag'in group or 'top-d' in group:
            group = 'Top donator'
        elif group == '':
            group = 'Member'
        username = ''.join(response.xpath('//div[@class="profile__user-basic-info"]//span//text()').extract())

        join = ''.join(response.xpath('//div[@class="profile__short-info"]//td[@class="trow1"]/span[contains(text(),"Joined:")]//../..//preceding-sibling::td/text()').extract())

        # Join date to epoch time convertion
        join_datee = datetime.datetime.strptime(join, '%B %d, %Y')
        join_date = time.mktime(join_datee.timetuple()) * 1000
        reputation = ''.join(response.xpath('//div[@class="d-table-cell w-100"]//span[contains(text(),"Reputation")]//../strong[@class="reputation_positive"]//text()').extract()) or 'Null'

        # last active into epoch time convertion
        try:
            last_active_xpath = response.xpath('//div[@class="profile__online-status"]//span//@title').extract()
            last_activee = datetime.datetime.strptime(last_active_xpath, '%B %d, %Y')
            last_active = (time.mktime(last_activee.timetuple()) * 1000)
        except:
            try:
                last_active_xpath = response.xpath('//div[@class="profile__online-status"]/text()').extract()[1]
                last_active = re.findall('(.*)Visit:', last_active_xpath)
                last = datetime.datetime.strptime(last_active, '%B %d, %Y')
                last_active = time.mktime(last.timetuple()) * 1000
            except:
                last_active = 0
        total_posts_xpath = response.xpath('//div[@class="d-table-cell w-100"]/span[contains(text(),"Total Posts:")]/../text()').extract()[1]
        total_posts = re.findall('\d+',total_posts_xpath)[0]
        credits = ''

        rank = ''.join(response.xpath(
            '//div[@class="proboxes"]/span/img/@src').extract()) or 'none'
        gmail_id = ''.join(response.xpath(
            '//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Google Hangouts ID:")]/../..//preceding-sibling::td/text()').extract())
        gmail = ''.join(response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Skype ID:")]/../following-sibling::td/a//text()').extract())
        if "protected]" in gmail:
            mail_2 = response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Skype ID:")]/../..//preceding-sibling::td//span/@data-cfemail').extract()
            email_1 = [utils.decode_cloudflareEmail(id_) for id_ in mail_2]
            gmail_id = gmail.split(u'[email\xc2\xa0protected]')[0] + email_1[0]
        skype_id = ''.join(response.xpath(
            '//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Skype ID:")]/../..//preceding-sibling::td/a/text()').extract())
        skype = ''.join(response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Google Hangouts ID:")]/../..//preceding-sibling::td/a//text()').extract())
        if "protected]" in skype:
            mails_1 = response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Google Hangouts ID:")]/../..//preceding-sibling::td/a/@data-cfemail').extract()
            email = [utils.decode_cloudflareEmail(id_) for id_ in mails_1]
            skype_id = skype.split(u'[email\xc2\xa0protected]')[0] + email[0]
        social = response.xpath(
            '//table[@class="tborder"]//a[@target="_blank"]/@href').extract()
        others_social = []

        # contact details in str(DICT format)
        for i in social:
            if "facebook" in i:
                facebook = i
            elif "google" in i:
                google = i
            elif "keybase" in i:
                keybase = i
            elif "soundcloud" in i:
                soundcloud = i
            elif "steamcommunity" in i:
                steamcommunity = i
            elif "twitter" in i:
                twitter = i
            elif "youtube" in i:
                youtube = i
            elif "flickr" in i:
                flickr = i
            elif "instagram" in i:
                instagram = i
            elif "last.fm"in i:
                last = i
            elif ".linkedin" in i:
                linkedin = i
            elif "/myspace.com" in i:
                myspace = i
            elif ".playstation" in i:
                playstatio = i
            elif "xbox.com" in i:
                xbox = i
            else:
                others_social.append(i)
        try:
            if not facebook:
                facebook = ''
        except:
            facebook = ''
        try:
            if not google:
                google = ''
        except:
            google = ''
        try:
            if not keybase:
                keybase = ''
        except:
            keybase = ''
        try:
            if not soundcloud:
                soundcloud = ''
        except:
            soundcloud = ''
        try:
            if not steamcommunity:
                steamcommunity = ''
        except:
            steamcommunity = ''
        try:
            if not twitter:
                twitter = ''
        except:
            twitter = ''
        try:
            if not youtube:
                youtube = ''
        except:
            youtube = ''
        try:
            if not flickr:
                flickr = ''
        except:
            flickr = ''
        try:
            if not instagram:
                instagram = ''
        except:
            instagram = ''
        try:
            if not last:
                last = ''
        except:
            last = ''
        try:
            if not linkedin:
                linkedin = ''
        except:
            linkedin = ''
        try:
            if not myspace:
                myspace = ''
        except:
            myspace = ''
        try:
            if not playstatio:
                playstatio = ''
        except:
            playstatio = ''
        try:
            if not skype_id:
                skype_id = ""
        except:
            skype_id = ""
        try:
            if not gmail_id:
                gmail_id = ""
        except:
            gmail_id = ""

        try:
            if not xbox:
                xbox = ''
        except:
            xbox = ''
        others_social = ', '.join(others_social)
        try:
            if not others_social:
                others_social= ""
        except:
            others_social = ""
        contact_info = '''[{"channel":"Skype ID:","user_id":"%s"}, {"channel":"Google Hangouts ID:", "user_id": "%s"},{"channel":"facebook", "user_id":"%s"},{"channel":"plus.google", "user_id":"%s"},{"channel":"keybase", "user_id":"%s"},{"channel":"soundcloud", "user_id":"%s"}{"channel":"steamcommunity", "user_id":"%s"},{"channel":"twitter", "user_id":"%s"},{"channel":"youtube", "user_id":"%s"}, {"channel":"flickr", "user_id":"%s"},{"channel":"instagram", "user_id":"%s"},{"channel":"last", "user_id":"%s"},{"channel":"linkedin", "user_id":"%s"},{"channel":"myspace", "user_id":"%s"},{"channel":"playstation", "user_id":"%s"}{"channel":"xbox", "user_id":"%s"}{"channel":"Other social networking links", "user_id":"%s"}]''' % (
            skype_id, gmail_id, facebook, google, keybase, soundcloud, steamcommunity, twitter, youtube, flickr, instagram, last, linkedin, myspace, playstatio, xbox, others_social)
        topic = response.meta.get('thread_type', '')
        thread_title = response.meta.get('thread_Topics', '-')
        activetimes_ = response.meta.get('commant_date', '-')
        activetimes = []

        # date in active time to str( dict format)
        for activetime in activetimes_:
            try:
                active_count = re.findall(
                    '\d+,\d+', total_posts) or re.findall('\d+', total_posts)
                try:
                    active_count = ''.join(active_count[0])
                except:
                    active_count = 'none'
                dt = time.gmtime(int(activetime) / 1000)
                activetime = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]""" % (
                    str(dt.tm_year), str(dt.tm_mon), str(dt.tm_wday), str(dt.tm_hour), active_count)
                activetimes.append(activetime)
            except:
                activetime = ''
        # writing data to DB
        Fetch_Time = int(round(time.time() * 1000))
        author_signature = ''

        awards = ', '.join([e.replace('>>', '').replace('\n', '').strip() for e in response.xpath('//tr//td[@class="trow1"]//div//a//img//@src | //tr//td[@class="trow2"]//div//a//img//@src').extract() if e.replace('>>', '').replace('\n', '').strip()])
        json_data.update({'username': username,
                'domain': 'www.raidforums.com',
                'auth_sign': author_signature,
                'join_date': join_date,
                'lastactive': last_active,
                'totalposts': total_posts,
                'fetch_time': Fetch_Time,
                'groups': group,
                'reputation': reputation,
                'credits': credits,
                'awards': awards,
                'rank' :rank,
                'activetimes': (''.join(activetimes)),
                'contact_info':contact_info,
        })
	self.es.index(index="forum_author", doc_type='post', id=hashlib.md5(username).hexdigest(), body=json_data)
	

