from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
from scrapy.utils.response import open_in_browser
from onionwebsites.scripts import *
from onionwebsites import utils
import re
import time
from pprint import pprint
from elasticsearch import Elasticsearch
import hashlib
es = Elasticsearch(['10.2.0.90:9342'])
import requests
from scrapy.http import FormRequest
from urlparse import urljoin
import time


class Altcoin(scrapy.Spider):
    name = "altcoin"
    start_urls = ['http://altcoinexpx3on26hpsbu4b5ipojqetyla677xva66jnidyxhrxrizqd.onion.ws/']
    custom_settings = {
	'COOKIES_ENABLED':False,
	'DOWNLOADER_MIDDLEWARES':{
		'onionwebsites.middlewares.Proxy_Rotation_Middleware': 440
				}
		}
	

    def parse(self,response):
        urls = response.xpath('//div[@id="content"]//ul[@class="categories"]//li[@class="row clearfix"]//h2[@class="title"]//a[@itemprop="url"]/@href | \
		//div[@id="content"]//ul[@class="categories"]//li[@class="row clearfix"]//span[@class="category-children-item pull-left"]//a/@href').extract()
        for url in urls:
            yield Request(urljoin('http://altcoinexpx3on26hpsbu4b5ipojqetyla677xva66jnidyxhrxrizqd.onion.ws/',url),callback = self.next_page)

    def next_page(self,response):
        urls = response.xpath('//div[@id="content"]//ul[@component="category"]//li[@class="row clearfix category-item unread"]//h2[@class="title"]//a[@itemprop="url"]/@href').extract()
        for url in urls:
            yield Request(urljoin('http://altcoinexpx3on26hpsbu4b5ipojqetyla677xva66jnidyxhrxrizqd.onion.ws/',url),callback = self.meta_page)

    def meta_page(self,response):
        nodes = response.xpath('//ul[@class="posts"]//li[@component="post"]')
        thread_title = ''.join(response.xpath('//h1[@class="hidden-xs"]//span[@class="topic-title"]//text()').extract()).strip()
        catagery = response.xpath('//ol[@class="breadcrumb"]//li[@itemscope="itemscope"]//span[@itemprop="title"]//text()').extract()[1].strip()
        sub_catagery = response.xpath('//ol[@class="breadcrumb"]//li[@itemscope="itemscope"]//span[@itemprop="title"]//text()').extract()[2].strip()
        for node in nodes:
            author = ''.join(node.xpath('./@data-username').extract()).strip()
            author_url = 'http://altcoinexpx3on26hpsbu4b5ipojqetyla677xva66jnidyxhrxrizqd.onion.ws/user/'+author
            publish_epoch = ''.join(node.xpath('./@data-timestamp').extract())
            text = clean_text('\n'.join(node.xpath('.//div[@class="content"]//text()').extract()).strip())
            post_id = ''.join(node.xpath('./@data-pid').extract()).strip()
            post_url = ''
            fetch_time = round((time.time()- time.timezone)*1000)
            links = node.xpath('.//div[@class="content"]//a[@rel="nofollow"]/@href').extract()
            all_links = []
            for link in links:
                if link:
                    try:
                        if link[:2] == '//':
                            all_links.append('http:'+link)
                        else:
                            all_links.append(link)
                    except:
                        all_links.append(link)
            doc = {
		    'thread_url':response.url,
                    'thread_title':thread_title,
                    'category':catagery,
                    'sub_category':sub_catagery,
                    'author':author,
                    'author_url':author_url,
                    'publish_time':publish_epoch,
                    'text':clean_text(text),
                    'post_id':post_id,
                    'post_url':'',
                    'fetch_time':fetch_time,
                    'links':', '.join(all_links),
                    'domain':'altcoinexpx3on26hpsbu4b5ipojqetyla677xva66jnidyxhrxrizqd.onion.ws',
                    }
            sk = hashlib.md5('altcoinexpx3on26hpsbu4b5ipojqetyla677xva66jnidyxhrxrizqd.onion.ws'+post_id).hexdigest()
            query={"query":{"match":{"_id":sk}}}
            res = es.search(body=query)
            if res['hits']['hits'] == []:
                es.index(index="forum_posts", doc_type='post', id=sk, body=doc)
	    else:
		data_doc = res['hits']['hits'][0]
                if (doc['links'] != data_doc['_source']['links']) or (doc['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=doc)
