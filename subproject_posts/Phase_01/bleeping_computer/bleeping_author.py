import datetime
import json
import MySQLdb
import re
import time
#import sys
#reload(sys)
#sys.setdefaultencoding('utf-8')
from datetime import date
from datetime import timedelta
import scrapy
from scrapy.spiders import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.http import FormRequest
import utils


class BleepingSpider(Spider):
    name = 'bleeping_author'

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="123",
                                    use_unicode=True,
                                    charset="utf8")

        self.cursor=self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        data = {
            'auth_key': '880ea6a14ea49e853634fbdc5015a024',
            'referer': 'https://www.bleepingcomputer.com/forums/index.php',
            'ips_username': 'inqspdr',
            'ips_password': 'Inq2018.',
            'rememberMe': '1',
        }
        url = 'https://www.bleepingcomputer.com/forums/index.php?app=core&module=global&section=login&do=process'
        yield FormRequest(url, callback=self.parse_next, formdata=data)

    def parse_next(self,response):
        start_query = 'select DISTINCT(links) from bleeping_authors_crawl'
        self.cursor.execute(start_query)
        data = self.cursor.fetchall()
        urls = [datum[0] for datum in data]
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from  bleeping_authors_crawl where links = %(url)s'
            val = {'url':url}
            self.cursor.execute(meta_query,val)
            meta_query = self.cursor.fetchall()
            publish_time = []
            thread_title = []
            for data in meta_query:
                meta = eval(data[0])
                thread_title.append(meta.get('thread_title', ''))
                publish_time.append(meta.get('publish_time', ''))
            publish_time  = set(publish_time)
            thread_title = ', '.join(set(thread_title))
            author_meta = {'publish_time':publish_time,'thread_title':thread_title}
            if author_meta and url:
                yield FormRequest(url, callback=self.parse_author,meta = author_meta)

    def parse_author(self, response):
        json_data = {}
        thread_title = response.meta.get('thread_title','')
        sel = Selector(response)
        user_name = ''.join(sel.xpath('//h1//span[@class="fn nickname"]//text()').extract())
        domain = "www.bleepingcomputer.com"
        if user_name:
            up_que_1 = "update bleeping_authors_crawl set crawl_status = 1 where links = %(url)s"
            up_que_val ={'url':response.url}
            self.cursor.execute(up_que_1,up_que_val)
        json_data.update({'user_name': user_name,
                          'domain': domain,
                          'crawl_type': 'keep_up'})
        activetimes_ = response.meta.get('publish_time')
        activetimes = []
        try:
            total_posts = int(''.join(response.xpath('//li[@class="clear clearfix"]//span[contains(text(),"Active Posts")]/../span[@class="row_data"]/text()').extract()).replace(',',''))
        except:
            total_posts = 0
            print 'vdnsjjj'
        for activetime in activetimes_:
            try:
                dt = time.gmtime(activetime/1000)
                activetime ="""[ { 'year': '%s','month': '%s', 'dayofweek': '%s', 'hour': '%s', 'count': '%s' }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),total_posts)
            except:
                activetime = '-'
            activetimes.append(activetime)
        joindate = ''.join(response.xpath('//div[@id="user_info_cell"]/text()').extract())
        joindate = ''.join(re.findall('\w+, \d\d:\d\d \wM',joindate)) or ''.join(re.findall('\d\d \w+ \d\d\d\d',joindate))
        lastactive = ''.join(response.xpath('//span[contains(text(),"Last Active")]//text()').extract()).replace("Last Active ",'')
        try:
            joindate = datetime.datetime.strptime(joindate,'%d %b %Y')
            joindate = time.mktime(joindate.timetuple())*1000
        except:
            try:
                if 'esterday' in joindate:
                    joindate_ = date.today() - timedelta(1)
                    joindate_ = joindate_.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',joindate))
                    joindate = datetime.datetime.strptime(joindate_,'%b %d %Y %I:%M %p')
                    joindate = time.mktime(joindate.timetuple())*1000
                elif 'oday' in joindate:
                    joindate_ = date.today()
                    joindate_ = joindate_.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',joindate))
                    joindate = datetime.datetime.strptime(joindate_,'%b %d %Y %I:%M %p')
                    joindate = time.mktime(joindate.timetuple())*1000
            except:
                pass
        author_signature = ""
        try:
            lastactive = datetime.datetime.strptime(lastactive,'%b %d %Y %I:%M %p')
            lastactive =  time.mktime(lastactive.timetuple())*1000
        except:
            if 'esterday' in lastactive:
                yesterday = date.today() - timedelta(1)
                yesterday =  yesterday.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',lastactive))
                lastactive = datetime.datetime.strptime(yesterday,'%b %d %Y %I:%M %p')
                lastactive =  time.mktime(lastactive.timetuple())*1000
            elif 'oday' in lastactive:
                yesterday = date.today()
                yesterday = yesterday.strftime('%b %d %Y')+' '+''.join(re.findall('\d\d:\d\d \wM',lastactive))
                lastactive = datetime.datetime.strptime(yesterday,'%b %d %Y %I:%M %p')
                lastactive =  time.mktime(lastactive.timetuple())*1000
            else:
                lastactive = 00
        author_signature = ''.join(sel.xpath('//div[@class="signature"]/a//strong//text() | //div[@class="signature"]/a//following-sibling::text() | //div[@class="signature"]//span//text() | //div[@class="signature"]//text() | //div[@class="signature"]//p//img//@src | //div[@class="signature"]//img//@src | //div[@class="signature"]//a/@href').extract()).replace('#','')
        if not "smile.png" in author_signature:
            author_signature = ''.join(sel.xpath('//div[@class="signature"]/a//strong//text() | //div[@class="signature"]/a//following-sibling::text() | //div[@class="signature"]//span//text() | //div[@class="signature"]//text() | //div[@class="signature"]//p//img//@src | //div[@class="signature"]//a/@href').extract()).encode('utf-8').replace('#','')

        groups = ''.join(response.xpath('//li[@class="clear clearfix"]//span[contains(text(),"Group")]/..//span[@class="row_data"]//text()').extract())
        fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000

        json_data.update({'author_signature': author_signature,
                          'join_date': joindate,
                          'last_active': lastactive,
                          'total_posts': total_posts,
                          'fetch_time': fetch_epoch,
                          'groups': groups,
                          'reputation': "None",
                          'credits': "None",
                          'awards': "None",
                          'rank': "None",
                          'active_time': ''.join(activetimes),
                          'contact_info': "None",
                          'reference_url': response.url
        })
        upsert_query_authors = utils.generate_upsert_query_authors('bleeping_computer')
        self.cursor.execute(upsert_query_authors, json_data)

