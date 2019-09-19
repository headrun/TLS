import scrapy
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import Request 
import datetime
import requests
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
from urllib import urlencode
from datetime import date, timedelta
from elasticsearch import Elasticsearch
import hashlib
from onionwebsites.scripts import *
from onionwebsites import utils
crawl_query = utils.generate_upsert_query_authors_crawl('paypal')


class Paypal(scrapy.Spider):
    name = "paypal_posts"
    start_urls = ["http://flkcpcprcfouwj33.onion/"]
    es = Elasticsearch(['10.2.0.90:9342'])

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user="tls_dev",passwd="hdrn!" , use_unicode = True , charset = 'utf8mb4')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        url_que = "select distinct(post_url) from paypal_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_next)

    def parse_next(self, response):
        domain = "flkcpcprcfouwj33.onion"
        category = response.xpath('//ul[@class="crumbs"]//li//a/text()').extract()[0]
        sub_category = response.xpath('//ul[@class="crumbs"]//li//a/text()').extract()[1].encode('utf8')
        thread_title = response.xpath('//ul[@class="crumbs"]//li//a/text()').extract()[2]
        thread_url = response.url
        nodes = response.xpath('//div[@id="brdmain"]//div[contains(@id,"p")]')
        for node in nodes:
            author = ''.join(node.xpath('.//dt//strong//a/text()').extract())
            author_url = ''.join(node.xpath('.//dt//strong//a/@href').extract())
            if author_url:
                author_url = 'http://flkcpcprcfouwj33.onion/' + author_url
            post_url = ''.join(node.xpath('.//h2//a/@href').extract())
            if post_url:
                post_url = 'http://flkcpcprcfouwj33.onion/' + post_url
            id = ''.join(re.findall('#p\d+', post_url))
            post_id = id.replace('#p','')
            date_ = ''.join(node.xpath('.//h2//a/text()').extract()).encode('utf8').replace('Today',datetime.datetime.now().strftime('%Y-%m-%d')).replace('Yesterday',(datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))
            try:
                publish_date = datetime.datetime.strptime(date_,'%Y-%m-%d %H:%M:%S')
                publish_epoch = time.mktime(publish_date.timetuple())*1000
            except:
		pass
            text = '\n'.join(node.xpath('.//div[@class="postmsg"]//text() | .//div[@class="postmsg"]//p[@class="postedit"]//text() | .//div[@class="postmsg"]//p//img/@alt | .//div[@class="postmsg"]//div[@class="quotebox"]/@class').extract()).replace('quotebox','quote')

            links = node.xpath('.//div[@class="postmsg"]//p//a/@href').extract()
            all_links = ', \n'.join(links)
            json_posts = {'domain': domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'fetch_time': int(datetime.datetime.now().strftime("%s")) * 1000,
                          'post_title' : '',
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'author': author,
                          'author_url': author_url,
                          'text': utils.clean_text(text),
                          'links': all_links,
            }
	    sk = hashlib.md5(domain + json_posts['post_id']).hexdigest()
	    query={"query":{"match":{"_id":sk}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
                self.es.index(index='forum_posts',doc_type='post', id=sk, body=json_posts)
	    else:
		data_doc = res['hits']['hits'][0]
		if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    self.es.index(index='forum_posts',doc_type='post', id=sk, body=json_posts)

            auth_meta = {'publish_epoch': publish_epoch}
            json_posts.update({
                    'post_id': post_id,
                    'auth_meta': json.dumps(auth_meta),
                    'crawl_status':0,
                    'links': author_url
                    })
            self.cursor.execute(crawl_query, json_posts)
            self.conn.commit()
        inner_nav = ''.join(response.xpath('//p[@class="pagelink conl"]//a[@rel="next"]//@href').extract())
        if inner_nav:
            inner_nav = "http://flkcpcprcfouwj33.onion/" + inner_nav
            yield Request(inner_nav, callback=self.parse_next)

