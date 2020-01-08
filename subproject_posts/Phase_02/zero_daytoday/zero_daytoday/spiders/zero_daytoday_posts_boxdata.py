import time
import datetime
import MySQLdb
import sys
import re
import requests
import json
import scrapy
import md5
import  hashlib
import utils
from zero_daytoday_xpaths import *
from zero_daytoday_browse import *
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request, FormRequest
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
query_posts_boxdata = utils.generate_upsert_query_posts_boxdata('0_daytoday')
auth_que = utils.generate_upsert_query_authors_data_crawl('0_daytoday')

class formus(Spider):
    name = '0_daytoday_posts_boxdata'
    #start_urls = ['http://mvfjfugdwgc5uwho.onion/']
    handle_httpstatus_list = [403]


    def __init__(self, *args, **kwargs):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="qwe123", db="0_daytoday", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def add_http(self, url):
        if 'http' not in url:
            url = 'http://mvfjfugdwgc5uwho.onion%s'%url
        return url

    def start_requests(self):
        url = "http://mvfjfugdwgc5uwho.onion/"
        time.sleep(3)
        yield Request(url, callback=self.parse_req)


    def parse_req(self, response):
        sel = Selector(response)
        #cookies = {
        #    'PHPSESSID': 'vad5eagpoj3je0entf7tva1880',
        #}

        headers = {
            'Host': 'mvfjfugdwgc5uwho.onion',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:45.0) Gecko/20100101 Firefox/45.0',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'http://mvfjfugdwgc5uwho.onion/',
            'Connection': 'keep-alive',
        }

        data = {
          'agree': 'Yes, I agree'
        }
        time.sleep(3)
        yield FormRequest('http://mvfjfugdwgc5uwho.onion/', callback=self.parse_next, headers=headers, formdata=data, dont_filter=True)

    def parse_next(self, response):
        url_que = "select distinct(post_url) from 0_daytoday_browse"
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_all_pages_links)

    def parse_all_pages_links(self, response):
	sel = Selector(response)
	meta = response.headers
        domain = "mvfjfugdwgc5uwho.onion"
	thread_url = response.url
	thread_id = response.url
	thread_id =  ''.join(re.findall('description/\d+',thread_id)).replace('description/', '')
	#print thread_id
        reference_url = response.url
        full_title =''.join(response.xpath('//div[@class="exploit_title"]//h1/text()').extract()).replace('Report Error','')
        category = ''.join(sel.xpath(CATEGORY).extract())
        date_add = ''.join(sel.xpath(DATE_ADD).extract())
        if date_add:
            year = ''.join(re.findall('-\d+(.*)',date_add)).replace('-','')
            if year > '2011':
                month_year = ''.join(re.findall('-(.*)',date_add)).replace('-','_')
            else:
                import pdb;pdb.set_trace()
                return None
      
        platform = ''.join(sel.xpath(PLATFORM).extract())
        price = ''.join(sel.xpath(PRICE).extract())
	if price == "free":
	    price = "0 BTC"
        description = ''.join(sel.xpath(DESCRIPTION).extract())
        CVE = ''.join(sel.xpath(CVE_XPATH).extract())
        abuses =  ''.join(sel.xpath(ABUSES).extract())
        comments = ''.join(sel.xpath(COMMENTS).extract())
        views =  ''.join(sel.xpath(VIEWS).extract())
        rel_releases =  ''.join(sel.xpath(REL_REALEASES).extract())
        risks = ''.join(sel.xpath(RISKS).extract())
        if risks:
            risks = risks
            if 'http' not in risks:
                risks =  self.add_http(risks)
        else:
            risks = ''

        verified = ''.join(sel.xpath(VERIFIED).extract())
        if verified:
            verified = verified
            if 'http' not in verified:
                verified = self.add_http(verified)
        else:
            verified = ''

        vendor_link = ''.join(sel.xpath(VENDOR_LINK).extract()).replace('\t','').replace('\n','').replace('\r','')
        if vendor_link:
            vendor_link = vendor_link
            if 'http' not in  vendor_link:
                vendor_link = 'https:' + vendor_link
        else:
            vendor_link = ''
        affected_ver = ''.join(sel.xpath(AFFECTED_VER).extract())
        tested_on =  ''.join(sel.xpath(TESTED_ON).extract()).replace('\t','').replace('\n','').replace('\r','')
        tags = ''.join(sel.xpath(TAGS).extract()).encode('ascii', 'ignore')
        video_proof = ''.join(sel.xpath(VEDIO_PROOF).extract())
	fetch_time = round((time.time()- time.timezone)*1000)
        if video_proof:
            video_proof = video_proof
            if  'http' not in video_proof:
                video_proof =  self.add_http(video_proof)
        else:
            video_proof = ''
        author_link = response.xpath(AUTHOR_LINK).extract()
        for author_url in author_link:
            if 'http' not in author_url:
                author_url = self.add_http(author_url)
	json_posts_boxdata = {}
        json_posts_boxdata.update({'domain' : domain,
                          'thread_url' : thread_url,
			  'thread_id' : thread_id,
			  'full_title' : full_title,
                          'category' : category,
			  'date_add' : date_add,
			  'platform' : platform,
			  'price' : price,
			  'description' : description,
			  'CVE' : CVE,
			  'abuses' : abuses,
			  'comments' : comments,
			  'views' : views,
			  'rel_releases' : rel_releases,
			  'risks' : risks,
			  'verified' : verified,
			  'vendor_link' : vendor_link,
			  'affected_ver' : affected_ver,
			  'tested_on' : tested_on,
			  'tags' : tags,
			  'video_proof' : video_proof,
			  'fetch_time':fetch_time,
              		  'reference_url' : reference_url,
	})
	#self.cursor.execute(query_posts_boxdata, json_posts_boxdata)
	#self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(thread_id)).hexdigest(), body=json_posts_boxdata)
        self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(thread_id)).hexdigest(), body=json_posts_boxdata)
        json_author = {}
        json_author.update({
            'links' : author_url,
            'crawl_status': 0,
            'thread_id' : thread_id
            })
        self.cursor.execute(auth_que, json_author)
	self.conn.commit()

