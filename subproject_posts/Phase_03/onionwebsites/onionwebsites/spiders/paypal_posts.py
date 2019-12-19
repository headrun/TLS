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
from onionwebsites.utils import *
crawl_query = generate_upsert_query_authors_crawl('paypal')

def clean_spchar_in_text(self, text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    #text = unicodedata.normalize('NFKD', text.decode('utf8'))
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
    text = re.sub(r'(\n\s*)', '\n', text)
    return text

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
        try:category = response.xpath('//ul[@class="crumbs"]//li//a/text()').extract()[0] or 'Null'
        except:pass
        try:sub_category = response.xpath('//ul[@class="crumbs"]//li//a/text()').extract()[1].encode('utf8') or 'Null'
        except:pass
        sub_category_url = response.xpath('//ul[@class="crumbs"]//li//a/@href').extract()[1].encode('utf8') or 'Null'
        if sub_category_url:
            sub_category_url = 'http://flkcpcprcfouwj33.onion/' + sub_category_url
        thread_title = response.xpath('//ul[@class="crumbs"]//li//a/text()').extract()[2] or 'Null'
        thread_url = response.url
        nodes = response.xpath('//div[@id="brdmain"]//div[contains(@id,"p")]')
        #if nodes:
            #query = 'update paypal_status set crawl_status = 1 where post_url = %(url)s'
            #json_data={'url':response.url}
            #self.cursor.execute(query,json_data)
        inner_nav = ''.join(response.xpath('//p[@class="pagelink conl"]//a[@rel="next"]//@href').extract())
        if inner_nav:
           inner_nav = "http://flkcpcprcfouwj33.onion/" + inner_nav
           yield Request(inner_nav, callback=self.parse_next)
        if inner_nav:
            if '&p' in thread_url:
                test = re.findall('(.*)&p',thread_url)
                thread_url = ''.join(test)
            else:
                thread_url = thread_url
        for node in nodes:
            author = ''.join(node.xpath('.//dt//strong//a/text()').extract()) or 'Null'
            authorurl = ''.join(node.xpath('.//dt//strong//a/@href').extract())
            if authorurl:
                author_url = 'http://flkcpcprcfouwj33.onion/' + authorurl
            post_url = ''.join(node.xpath('.//h2//a/@href').extract())
            if post_url:
                post_url = 'http://flkcpcprcfouwj33.onion/' + post_url
            ord_in_thread = ''.join(node.xpath('.//h2//span//span[@class="conr"]//text()').extract()).replace('#','')
            id = ''.join(re.findall('#p\d+', post_url)) or 'Null'
            post_id = id.replace('#p','')
            date_ = ''.join(node.xpath('.//h2//a/text()').extract()).encode('utf8').replace('Today',datetime.datetime.now().strftime('%Y-%m-%d')).replace('Yesterday',(datetime.datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')) or 'Null'
            try:
                publish_date = datetime.datetime.strptime(date_,'%Y-%m-%d %H:%M:%S')
                publish_epoch = time.mktime(publish_date.timetuple())*1000
            except:
		pass
	    if publish_epoch:
		month_year = get_index(publish_epoch)
	    else:
		import pdb;pdb.set_trace()

            text = ''.join(node.xpath('.//div[@class="postmsg"]//text() | .//div[@class="postmsg"]//p[@class="postedit"]//text() | .//div[@class="postmsg"]//p//img/@alt | .//div[@class="postmsg"]//div[@class="quotebox"]/@class').extract()).replace('quotebox','quote').replace('\t','').replace('\n\n','')
            text = clean_spchar_in_text(self,text)

            links = node.xpath('.//div[@class="postmsg"]//p//a/@href').extract()
            all_links = ', \n'.join(links)
            if all_links == '':
                all_links = 'Null'
            if links == []:
                all_links = 'Null'
            post = {
                'cache_link':'',
                'section':category,
                'language':'english',
                'require_login':'false',
                'sub_section':sub_category,
                'sub_section_url':sub_category_url,
                'post_id':''.join(post_id),
                'post_title':'Null',
                'ord_in_thread':ord_in_thread,
                'post_url':post_url,
                'post_text':text.replace('\n',''),
                'thread_title':thread_title,
                'thread_url':thread_url
                }
            author_data = {
                'name':author,
                'url':author_url
                }
            json_posts = {
                        'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                        'hostname': 'http://flkcpcprcfouwj33.onion/',
                        'domain' : domain,
                        'sub_type':'openweb',
                        'type':'forum',
                        'author':json.dumps(author_data),
                        'title':thread_title,
                        'text':text.replace('\n',''),
                        'url':post_url,
                        'original_url':post_url,
                        'fetch_time':(round(time.time()*1000)),
                        'publish_time' : publish_epoch,
                        'link_url' : links,
                        'post':post
            }
	    sk = hashlib.md5(str(post_url.encode('utf8'))).hexdigest()
	    #query={"query":{"match":{"_id":sk}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
            self.es.index(index=month_year, doc_type='post', id=sk, body=json_posts)
	    '''else:
		data_doc = res['hits']['hits'][0]
		if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    self.es.index(index='forum_posts',doc_type='post', id=sk, body=json_posts)'''

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

