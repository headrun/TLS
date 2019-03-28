# -*- coding: utf-8 -*-
import scrapy
from scrapy.spider import Spider
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
#import xpaths


class HackThisiCo(scrapy.Spider):
    name = "hackthis_co_posts"

    def __init__(self, *args, **kwargs):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="posts_hackthis_co",host="localhost",user="root",passwd="1216" , use_unicode = True , charset = 'utf8')
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
        domain = "www.hackthis.co.uk"
        thread_url = response.url
        thread_title = ''.join(response.xpath('//h1[@itemprop="name"]/text()').extract())
        category = ''.join(response.xpath('//div[@class="col span_18 forum-main"]//a/text()').extract()[0])
        sub_category = '["' + ''.join(response.xpath('//div[@class="col span_18 forum-main"]//a/text()').extract()[1])+ '"]'
        nodes = response.xpath('//li[@class="row clr  "]')
        if nodes:
            query = 'update hackthis_co_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
        for node in nodes:
            post_id = ''.join(node.xpath('.//@data-id').extract())
            author = ''.join(node.xpath('.//a[@itemprop="author"]/text() | .//div[@class="col span_5 post_header"]//a/text()').extract()).strip()
            author_url = ''.join(node.xpath('.//a[@itemprop="author"]/@href | .//div[@class="col span_5 post_header"]//a/@href').extract())
            if "http" not in author_url:
                authorurl = "https://www.hackthis.co.uk" + author_url
            publish_time = ''.join(node.xpath('.//time[@itemprop="datePublished"]/@datetime | .//li[@class="highlight"]//@datetime ').extract())
            publish_epoch = utils.time_to_epoch(publish_time, '%Y-%m-%dT%H:%M:%S+00:00')
            fetch_epoch = utils.fetch_time()
            texts = ''.join(node.xpath('.//div[@class="post_body"]//text() | .//div[@class="post_body"]//img[@class="bbcode_img"]/@alt | .//div[@class="bbcode-youtube"]//iframe/@src ').extract())
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
            all_links = str(link)
            query_posts = utils.generate_upsert_query_posts('posts_hackthis_co')
            json_posts = {'domain': domain,
                          'crawl_type': '',
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : '',
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': '',
                          'publish_epoch': publish_epoch,
                          'fetch_epoch': fetch_epoch,
                          'author': author,
                          'author_url': authorurl,
                          'post_text': utils.clean_text(text),
                          'all_links': all_links,
                          'reference_url': response.url
            }
            query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
                self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)

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
