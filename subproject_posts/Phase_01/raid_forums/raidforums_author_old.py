from scrapy.http import Request
import csv
import datetime
import calendar
import scrapy
import time
import re
import MySQLdb
import datetime
import json
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import logging
from pyvirtualdisplay import Display
from selenium import webdriver
import unicodedata
from ast import literal_eval
import utils

HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
           'Accept-Encoding': 'gzip,deflate',
           'Accept-Language': 'en',
           'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0.3325.181 Safari/537.36',
           }


class Raidforums(scrapy.Spider):
    name = 'raidforums_author_old'
    log_file_name = 'raidforums_%s.log' % str(
        datetime.datetime.now()).replace(' ', '')
    logging.basicConfig(filename=log_file_name, level=logging.DEBUG)
    # no need start urls

    def __init__(self, limit='50', *args, **kwargs):
        self.conn = MySQLdb.connect(db="posts_raidforums", host="localhost",
                                    user="root", passwd="", use_unicode=True, charset="utf8")
        self.cursor = self.conn.cursor()
        self.urls = []
        self.limit = limit
        super(scrapy.Spider, self).__init__(*args, **kwargs)
        dispatcher.connect(self.close_conn, signals.spider_closed)
        select_qry = 'select DISTINCT(links) from raidforums_crawl ;'
        self.cursor.execute(select_qry)
        self.data = self.cursor.fetchall()
        for da in self.data:
            self.urls.append(da[0])

    # closing DB connection
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
        yield Request(url, callback=self.parse_login, headers=HEADERS, cookies=cookies_input, meta={'cook': cookies})

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
        yield Request(url, self.parse_user, headers=HEADERS, cookies=cookies, meta={'cook': cookies})

        # fetch urls from DB using __init__ method
    def parse_user(self, response):
        cookies = response.meta.get('cook', {})
        for url in self.urls:
            meta_query = 'select DISTINCT(auth_meta) from raidforums_crawl where links = "%s" ' % url.encode(
                'utf8')
            self.cursor.execute(meta_query)
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
            yield Request(url, callback=self.parse_author, headers=HEADERS, cookies=cookies, meta=author_meta)

    def parse_author(self, response):
        json_data = {}
        others_social,skype_id,xbox,myspace,playstatio,facebook, google, credits,keybase,soundcloud,steamcommunity,twitter,youtube,flickr,instagram,last,linkedin  ="","","","","","","","","","","","","","","","",""
        user = ''.join(response.xpath(
            '//div[contains(@class, "user-name")]//span/text()').extract())
        if response.status != 200 or 302:
            status_code_update = 'update raidforums_crawl set status_code = 1 where links = "%s"' % MySQLdb.escape_string(
                response.url)
            self.cursor.execute(status_code_update)
        else:
            status_code_update = 'update raidforums_crawl set status_code = 9 where links = "%s"' % MySQLdb.escape_string(
                response.url)

        logging.info("url:%s", response.url)
        logging.info("status_code:%s", response.status)
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

        username = ''.join(response.xpath('//span[@class="largetext protitlemain"]//span/text()').extract()) or \
                   ''.join(response.xpath('//span[@class="largetext protitlemain"]//strong/img/@original-title').extract())or \
                   ''.join(response.xpath(
                       '//span[@class="largetext protitlemain"]//span//text()').extract())

        join_date_xpath = '//div[@id="3-content"]//td[@class="trow1"]/strong[contains(text(),"Joined:")]//../..//preceding-sibling::td/text()'
        join_date = ''.join(response.xpath(join_date_xpath).extract())

        # Join date to epoch time convertion
        try:
            join_date = datetime.datetime.strptime(join_date, '%m-%d-%Y')
            join_date = time.mktime(join_date.timetuple()) * 1000
        except:
            try:
                join_date = datetime.datetime.strptime(join_date, '%d-%m-%Y')
                join_date = time.mktime(join_date.timetuple()) * 1000
            except:
                pass
        last_active_xpath = '//div[@id="3-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Last Visit")]//../..//preceding-sibling::td//text()'
        last_active = ''.join(response.xpath('//div[@id="3-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Last Visit")]//../..//preceding-sibling::td//span//@title').extract(
        )) or ''.join(response.xpath(last_active_xpath).extract())
        last_active = ''.join(re.findall(
            '\d+-\d+-\d+, \d+:\d+ \w+', last_active))
        reputation_xpath = '//div[@id="3-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Reputation:")]//../..//preceding-sibling::td//strong//text()'
        try:
            reputation = response.xpath(reputation_xpath).extract()[-1]
        except:
            reputation = ''.join(response.xpath(reputation_xpath).extract())

        # last active into epoch time convertion
        try:
            last_active = datetime.datetime.strptime(
                last_active, '%m-%d-%Y, %I:%M %p')
            last_active = (time.mktime(last_active.timetuple()) * 1000)
        except:
            try:
                last_active = datetime.datetime.strptime(
                    last_active, '%d-%m-%Y, %I:%M %p') or datetime.datetime.strptime(last_active, '%m-%d-%Y')
                last_active = time.mktime(last_active.timetuple()) * 1000
            except:
                last_active = ''.join(response.xpath(
                    last_active_xpath).extract())
                if 'Hidden' not in last_active:
                    last_active = ''.join(response.xpath(
                        '//div[@id="3-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Last Visit")]//../..//preceding-sibling::td//span/@title').extract())
                    if last_active:
                        try:
                            date_ = datetime.datetime.strptime(last_active, '%d-%m-%Y, %I:%M %p')  
                        except:
                            date_ =  datetime.datetime.strptime(last_active, '%m-%d-%Y')
                        last_active = time.mktime(date_.timetuple()) * 1000
                    else:
                        last_active = 0
        total_posts_xpath = '//div[@id="3-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Total Posts:")]//../..//preceding-sibling::td/text()'
        total_posts = ''.join(response.xpath(total_posts_xpath).extract())
        credits = ''.join(response.xpath(
            '//div[@id="3-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Credits")]//../..//preceding-sibling::td/a/text()').extract())

        rank = ''.join(response.xpath(
            '//div[@class="proboxes"]/span/img/@src').extract()) or 'none'
        gmail_id = ''.join(response.xpath(
            '//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Google Hangouts ID:")]/../..//preceding-sibling::td/text()').extract())
        gmail = ''.join(response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Skype ID:")]/../following-sibling::td/a//text()').extract())
        if "protected]" in gmail:
            mail_2 = response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Skype ID:")]/../..//preceding-sibling::td//span/@data-cfemail').extract()
            email_1 = [utils.decode_cloudflareEmail(id_) for id_ in mail_2]
            gmail_id = gmail.split('[email\xc2\xa0protected]')[0] + email_1[0]
        skype_id = ''.join(response.xpath(
            '//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Skype ID:")]/../..//preceding-sibling::td/a/text()').extract())
        skype = ''.join(response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Google Hangouts ID:")]/../..//preceding-sibling::td/a//text()').extract())
        if "protected]" in skype:
            mails_1 = response.xpath('//div[@id="2-content"]//td[contains(@class,"trow")]/strong[contains(text(),"Google Hangouts ID:")]/../..//preceding-sibling::td/a/@data-cfemail').extract()
            email = [utils.decode_cloudflareEmail(id_) for id_ in mails_1]
            skype_id = skype.split('[email\xc2\xa0protected]')[0] + email[0]
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
                facebook = 'none'
        except:
            facebook = 'none'
        try:
            if not google:
                google = 'none'
        except:
            google = 'none'
        try:
            if not keybase:
                keybase = 'none'
        except:
            keybase = 'none'
        try:
            if not soundcloud:
                soundcloud = 'none'
        except:
            soundcloud = 'none'
        try:
            if not steamcommunity:
                steamcommunity = 'none'
        except:
            steamcommunity = 'none'
        try:
            if not twitter:
                twitter = 'none'
        except:
            twitter = 'none'
        try:
            if not youtube:
                youtube = 'none'
        except:
            youtube = 'none'
        try:
            if not flickr:
                flickr = 'none'
        except:
            flickr = 'none'
        try:
            if not instagram:
                instagram = 'none'
        except:
            instagram = 'none'
        try:
            if not last:
                last = 'none'
        except:
            last = 'none'
        try:
            if not linkedin:
                linkedin = 'none'
        except:
            linkedin = 'none'
        try:
            if not myspace:
                myspace = 'none'
        except:
            myspace = 'none'
        try:
            if not playstatio:
                playstatio = 'none'
        except:
            playstatio = 'none'
        try:
            if not skype_id:
                skype_id = "none"
        except:
            skype_id = "none"
        try:
            if not gmail_id:
                gmail_id = "none"
        except:
            gmail_id = "none"

        try:
            if not xbox:
                xbox = 'none'
        except:
            xbox = 'none'
        others_social = ', '.join(others_social)
        try:
            if not others_social:
                others_social= "none"
        except:
            others_social = "none"
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
                activetime = 'none'
        # writing data to DB
        Fetch_Time = int(round(time.time() * 1000))
        author_signature = ""
        author_signature = ' '.join(response.xpath('//td/strong[contains(text(), "Signature")]/../../following-sibling::tr//td//img//@src | //td/strong[contains(text(), "Signature")]/../../following-sibling::tr//td//text() | //td/strong[contains(text(), "Signature")]/../../following-sibling::tr//td//a/@href').extract())
        if """protected]""" in author_signature:
            mails  = response.xpath('//td/strong[contains(text(), "Signature")]/../../following-sibling::tr//td//@data-cfemail').extract()
            email_id=[utils.decode_cloudflareEmail(id_) for id_ in mails]
            if len(mails) > 1:
                author_signature =  author_signature.split('[email\xc2\xa0protected]')[0] + email_id[0] + ' ' + author_signature.split('[email\xc2\xa0protected]')[1] + ' ' + email_id[1] + ' ' + author_signature.split('[email\xc2\xa0protected]')[2]
            else:
                autho_signature = author_signature.split('[email\xc2\xa0protected]')
                if len(autho_signature) >1:
                    author_signature =  author_signature.split('[email\xc2\xa0protected]')[0] + email_id[0]  + " " + author_signature.split('[email\xc2\xa0protected]')[1]
                else:
                    author_signature =   author_signature.split('[email\xc2\xa0protected]')[0] + email_id[0] + ' '

        awards = ', '.join([e.replace('>>', '').replace('\n', '').strip() for e in response.xpath(
            '//strong[contains(text(), "awards")]/..//../../tr//td[contains(@class, "trow")]//following-sibling::span/../text()').extract() if e.replace('>>', '').replace('\n', '').strip()])
        json_data.update({'user_name': username,
                'domain': 'www.raidforums.com',
                'crawl_type': 'keep_up',
                'author_signature': author_signature,
                'join_date': join_date,
                'last_active': last_active,
                'total_posts': total_posts,
                'fetch_time': Fetch_Time,
                'groups': group,
                'reputation': reputation,
                'credits': credits,
                'awards': awards,
                'rank' :rank,
                'active_time': (''.join(activetimes)),
                'contact_info':contact_info,
                'reference_url': response.url
        })
        upsert_query_authors = utils.generate_upsert_query_authors('posts_raidforums')
        self.cursor.execute(upsert_query_authors, json_data)

