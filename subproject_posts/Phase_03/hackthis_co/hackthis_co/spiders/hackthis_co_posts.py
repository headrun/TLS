# -*- coding: utf-8 -*-
import scrapy
from scrapy import Spider
from scrapy.selector import Selector
import time
from scrapy.http import Request
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
import json
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import unicodedata
from pprint import pprint
#import xpaths


class HackThisiCo(Spider):
    name = "hackthis_co_posts"

    def __init__(self, *args, **kwargs):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="posts_hackthis_co",host="localhost",user="tls_dev",passwd="hdrn!" , use_unicode = True , charset = 'utf8')
        self.crawl_query = utils.generate_upsert_query_authors_crawl('posts_hackthis_co')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url_que = "select distinct(post_url) from hackthis_co_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread)


    def parse_thread(self,response):
	if response.url == 'https://www.hackthis.co.uk/forum/':
	    return
        domain = "hackthis.co.uk"
        thread_url = response.url
        thread_title = ''.join(response.xpath('//h1[@itemprop="name"]/text()').extract()) or 'Null'
        category = ''.join(response.xpath('//div[@class="col span_18 forum-main"]//a/text()').extract()[0]) or 'Null'
        sub_category = ''.join(response.xpath('//div[@class="col span_18 forum-main"]//a/text()').extract()[1]) or 'Null'
	sub_categoryurl = ''.join(response.xpath('//div[@class="col span_18 forum-main"]//a/@href').extract()[1])
	if sub_categoryurl:
	    sub_category_url = 'https://www.hackthis.co.uk/forum' + sub_categoryurl
	if sub_categoryurl == '':
	    sub_category_url = 'Null'
        #nodes = response.xpath('//li[@class="row clr  "]')
	nodes = response.xpath('//li[contains(@class,"row clr   ")] | //li[@class="row clr  "]')
        if nodes:
            query = 'update hackthis_co_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
	x = 0
        for node in nodes:
	    x = x+1
            post_id = ''.join(node.xpath('.//@data-id').extract()) or 'Null'
            author = ''.join(node.xpath('.//a[@itemprop="author"]/text() | .//div[@class="col span_5 post_header"]//a/text()').extract()).strip() or 'Null'
            author_url = ''.join(node.xpath('.//a[@itemprop="author"]/@href | .//div[@class="col span_5 post_header"]//a/@href').extract())
            if "http" not in author_url:
                authorurl = "https://www.hackthis.co.uk" + author_url
	    if author_url == '':
		authorurl = 'Null'
            publish_time = ''.join(node.xpath('.//time[@itemprop="datePublished"]/@datetime | .//li[@class="highlight"]//@datetime ').extract()) or 'Null'
            publish_epoch = utils.time_to_epoch(publish_time, '%Y-%m-%dT%H:%M:%S+00:00')
	    if publish_epoch:
		month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
	    else:
		import pdb;pdb.set_trace()

            fetch_epoch = utils.fetch_time()
            texts = ''.join(node.xpath('.//div[@class="post_body"]//text() | .//div[@class="post_body"]//img[@class="bbcode_img"]/@alt | .//div[@class="bbcode-youtube"]//iframe/@src ').extract()) or 'Null'
            text_sign = ''.join(node.xpath('.//div[@class="post_signature"]//text() | .//div[@class="post_signature"]//img[@class="bbcode_img"]/@alt ').extract())
            text = texts.replace(text_sign,'')
            linkes = node.xpath('.//div[@class="post_body"]//img[@class="bbcode_img"][not(contains(@src,"gif"))]/@src | .//div[@class="post_body"]//a[@class="bbcode_url"]/@href | .//div[@class="bbcode-youtube"]//iframe/@src | .//div[@class="post_body"]//a[not(contains(@href,"gif"))]/@href').extract()
            link_ = node.xpath('.//div[@class="post_signature"]//a[@class="bbcode_url"]/@href | .//div[@class="post_signature"]//a[not(contains(@href,"gif"))]/@href').extract()
            links_ = list(set(linkes) ^ set(link_))
            link = []
            for Link in links_:
                if 'http:' not in Link and 'https:' not in Link and 'mailto:' not in Link:
                    Link = "https://www.hackthis.co.uk" + Link
                    link.append(Link)
                else:
                    link.append(Link)
            links = ', '.join(link)
	    if links == '':
		links = 'Null'
	    if link_ == []:
		links = 'Null'
            query_posts = utils.generate_upsert_query_posts('posts_hackthis_co')
	    author_data = {
                'name':author,
                'url':authorurl
                }
	    post = {
		'cache_link':'',
		'author':json.dumps(author_data),
		'section':category,
		'language':'english',
		'require_login':'false',
		'sub_section':sub_category,
		'sub_section_url':sub_category_url,
		'post_id':post_id,
		'post_title':'Null',
		'ord_in_thread':x,
		'post_url':'Null',
		'post_text':utils.clean_text(text).replace('\n',''),
		'thread_title':thread_title,
		'thread_url':thread_url
		}
            json_posts = {
			  'record_id':'Null',
			  'hostname':'www.hackthis.co.uk',
			  'domain': domain,
			  'sub_type':'openweb',
			  'type':"forum",
			  'author':json.dumps(author_data),
			  'title':thread_title,
			  'text':utils.clean_text(text).replace('\n',''),
			  'url':'Null',
			  'original_url':'Null',
			  'fetch_time':fetch_epoch,
			  'publish_time':publish_epoch,
			  'link.url':links,
			  'post':post
            }
            #query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(domain+post_id).hexdigest(), body=json_posts, request_timeout=30)

            if author_url:
                # Write data into forums_crawl table
                meta = {'publish_epoch': publish_epoch}
                json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
                }

                self.cursor.execute(self.crawl_query, json_crawl)
                self.conn.commit()

        pagenav = ''.join(response.xpath('//li[@class="right"]//a[@rel="next"]/@href').extract())
        if pagenav:
            page = re.sub('\?page=\d+','',response.url)+pagenav
            yield Request(page,callback=self.parse_thread)
