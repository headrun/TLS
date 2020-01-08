'''
   Onion site using tor 0_daytoday_posts_comments..
'''
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
from zero_daytoday_browse import *
from scrapy.selector import Selector
from scrapy.spiders import Spider
from scrapy.http import Request, FormRequest
from scrapy import signals
from zero_daytoday_xpaths import*
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
query_posts = utils.generate_upsert_query_posts('0_daytoday')
authors_meta = utils.generate_upsert_query_authors_meta('0_daytoday')



class formus(Spider):
    name = '0_daytoday_posts'
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
        url_que = "select distinct(post_url) from 0_daytoday_browse "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_all_pages_links)

    def parse_all_pages_links(self, response):
	sel = Selector(response)
        meta = response.headers
	thread_url = response.url
	domain = 'mvfjfugdwgc5uwho.onion'
	crawl_type = 'keepup'
	thread_title = ''.join(response.xpath('//div[@class="exploit_title"]//h1/text()').extract()).replace('Report Error','')
        author_link = sel.xpath(AUTHOR_LINK).extract()
        for author_url in author_link:
            if 'http' not in author_url:
                author_url = self.add_http(author_url)

	nodes=sel.xpath('//div[@class="support_message"]')
        if nodes:
            query = 'update 0_daytoday_browse set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
	for node in nodes:
            comment_author_name = ''.join(node.xpath(COMMENT_AUTHOR_NAME).extract())
            comment_text = ''.join(node.xpath(COMMENT_TEXT).extract())
            publish_time_1 = ''.join(node.xpath(COMMENT_PUBLISH_TIME).extract())
            sk = hashlib.md5((comment_author_name + publish_time_1 + comment_text[:20]).encode())
            sk_id = sk.hexdigest()
            try:
		comment_publish_time = datetime.datetime.strptime((publish_time_1), '%d-%m-%Y, %H:%M ')
	    except:
		comment_publish_time = datetime.datetime.strptime((publish_time_1), '%d-%m--%Y, %H:%M ')
            comment_publish_time = time.mktime(comment_publish_time.timetuple())*1000
	    if comment_publish_time:
                 year = time.strftime("%Y", time.localtime(int(comment_publish_time/1000)))
                 if year > '2011' :
		     month_year = time.strftime("%m_%Y", time.localtime(int(comment_publish_time/1000)))
                 else:
                     continue
	    fetch_time =  (round(time.time()*1000))
	    json_posts = {}
            json_posts.update({'domain' : domain,
                          'category' : '',
                          'sub_category' : '',
                          'thread_title' : thread_title,
                          'post_title'  :comment_text,
                          'thread_url' : thread_url,
                          'post_id' : '',
                          'post_url' : '',
                          'publish_time' :  '',
                          'fetch_time' : fetch_time,
                          'author_url' : '',
                          'text' : '', #text
                          'links' : '',
                          'author' : comment_author_name,
                          'comment_publish_time' : comment_publish_time,
                          'reference_url' : response.url,
                          'sk_id' : sk_id
            })
	    #try:
	    self.es.index(index="forum_posts_"+month_year, doc_type='post', id=sk_id, body=json_posts)
	    #except:
		#import pdb;pdb.set_trace()

            meta = json.dumps({'time' : comment_publish_time})
            json_author_meta = {}
            json_author_meta.update({
                'auth_meta' : meta,
		'sk_id' : sk_id,
                'links' : author_url
            })
            self.cursor.execute(authors_meta, json_author_meta)





