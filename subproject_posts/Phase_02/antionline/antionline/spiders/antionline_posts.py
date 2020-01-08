import datetime
import time
import sys
import json
import re
import scrapy
from scrapy.selector import Selector
from scrapy.spiders import BaseSpider
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import MySQLdb
import unicodedata
from antionline_xpaths import *
import sys
from datetime import timedelta
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
from pprint import pprint
query_posts = utils.generate_upsert_query_posts('antionline')
auth_que = utils.generate_upsert_query_authors_crawl('antionline')


class Formus(BaseSpider):
    name = 'antionline_posts'
    start_urls = ['http://www.antionline.com/']
    handle_httpstatus_list = [403]
    
    def __init__(self):
        self.conn ,self.cursor = self.mysql_conn()
        self.es = Elasticsearch(['10.2.0.90:9342'])
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
	conn = MySQLdb.connect(host="localhost", user="tls_dev", passwd="hdrn!", db="tls_phase_2", charset="utf8mb4", use_unicode=True)
	cursor = conn.cursor()
	return conn,cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def add_http(self, url):
        if 'http' not in url:
            url = 'http://www.antionline.com/%s'%url
        return url

    def parse(self,response):
        url_que = "select distinct(post_url) from antionline_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_all_pages_links)

    def parse_all_pages_links(self, response):
        if 'page' in response.url:
            crawl_type = "catchup"
            test = re.findall(r'page\d+', response.url)
            thread_url = response.url.replace(''.join(test), "")#.replace('&do=findComment','')
            thread_url = utils.clean_url(thread_url)
        else:
            crawl_type = "keepup"
            thread_url = response.url
            thread_url = utils.clean_url(thread_url)
	
        sel = Selector(response)
        #thread_url = response.url
        domain = 'antionline.com'
        try:category = sel.xpath('//li[@class="navbit"]//a//text()').extract()[1] or 'Null'
        except:pass
        try:subcategory = ''.join(sel.xpath('//li[@class="navbit"]//a//text()').extract()[2]) or 'Null'
        except:pass
	sub_category_url = sel.xpath('//li[@class="navbit"]//a/@href').extract()[2] or 'Null'
	if sub_category_url:
	    sub_section_url = 'http://www.antionline.com/' + sub_category_url
        thread_title = ''.join(sel.xpath(THREAD_TITLE).extract()) or 'Null'
        crawl_type = ''
        nodes = sel.xpath(NODES)
        if nodes:
            query = 'update antionline_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
        '''nav_click = set(sel.xpath('//span//a[@rel="next"]//@href').extract())
        for post_nav_click in nav_click:
            if post_nav_click:
		try:
		    post_url_ = ''.join(nodes[-1].xpath(POST_URL).extract())
                    test_id = hashlib.md5(str(post_url_.encode('utf8'))).hexdigest()
		    query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		    res = es.search(index="forum_posts", body={"query": query})
		    if res['hits']['hits']==[]:
			post_nav_click = self.add_http(post_nav_click)
                	yield Request(post_nav_click, callback=self.parse_all_pages_links)
		except:pass'''

        for node in nodes:
            if "-email-account" in response.url:
                pass
            author_name = ''.join(node.xpath(AUTHOR_NAME).extract()) or 'Null'
            author_link = ''.join(node.xpath(AUTHOR_LINK).extract())
            if author_link:
                author_link = "http://www.antionline.com/" + author_link
	    if author_link == '':
		author_link = 'Null'
            post_urls = ''.join(node.xpath(POST_URL).extract()) or 'Null'
	    if post_urls:
		post_url = "http://www.antionline.com/" + post_urls
	    if post_urls == '':
		post_urls = 'Null'
		post_url = post_urls
	    ord_in_thread = ''.join(node.xpath('.//span[@class="nodecontrols"]//a[contains(@id,"postcount")]/@name').extract())
            #postid  = post_url.split('=1#post')[-1] ## try in re without indexing
            postid = ''.join(re.findall(r'#post\d+', post_url)).replace('#post', '') or 'Null'

            post_title = ''.join(node.xpath(POST_TITLE).extract()).replace('\n', '').replace('\t', '').replace('\r', '') or 'Null'

            publish_time = ' '.join(node.xpath(PUBLISH_TIME).extract()).encode('ascii', 'ignore') or 'Null'
            publish_time = re.sub(r'\w+ \d+\w+,', ''.join(re.findall(r'\w+ \d+', publish_time)), publish_time).replace('Yesterday,',(datetime.datetime.now() - timedelta(days=1)).strftime('%d %B %Y -')).replace('Today,',datetime.datetime.now().strftime('%d %B %Y -'))
            publish_time = datetime.datetime.strptime((publish_time), '%B %d %Y, %H:%M %p')
            publish_time = time.mktime(publish_time.timetuple())*1000
	    if publish_time:
                year = time.strftime("%Y", time.localtime(int(publish_time/1000)))
                if year > '2011':
		    month_year = time.strftime("%m_%Y", time.localtime(int(publish_time/1000)))
                else:
                    continue
            fetch_time = (round(time.time()*1000))

            POST_TEXT = './/blockquote[@class="postcontent restore "]//text()|.//blockquote[@class="postcontent restore "]//img//@title | .//div[@class="bbcode_quote_container"]//@class' #|.//h2[@class="title icon"]//img//@alt ' #| .//div[@class="bbcode_postedby"]//img[not(contains (@alt , "View Post"))]//@alt'

            text = ' '.join(node.xpath(POST_TEXT).extract()) or 'Null'
            text = utils.clean_text(text)
            if  'bbcode_quote_container' in text:
                text = text.replace('bbcode_quote_container', 'Quote ')

            Links = node.xpath(LINK).extract()
            Link = []
            for link_ in Links:
                if 'http' not in link_: link_ = 'https:'+ link_
                if not '.gif.' in link_:
                    Link.append(link_)
	    links = ', '.join(Link)
	    if links == '':
		links = 'Null'
	    if Links == []:
		Links = 'Null'
		links = Links
            #links = str(Link)
            #if "[]" in links: links = ""
	    author = {
                'name':author_name,
                'url':author_link
                }
	    post = {
                'cache_link': '',
		'author':json.dumps(author),
                'section':category,
                'language': "english",
                'require_login':"false",
                'sub_section':subcategory,
                'sub_section_url':sub_section_url,
                'post_id':postid,
                'post_title':post_title,
                'ord_in_thread':ord_in_thread,
                'post_url':post_url,
                'post_text':utils.clean_text(text),
                'thread_title':thread_title,
                'thread_url':thread_url
                }
	    json_posts = {
			'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                        'hostname': 'www.antionline.com',
                        'domain' : domain,
                        'sub_type':'openweb',
                        'type':'forum',
                        'author':json.dumps(author),
                        'title':thread_title,
                        'text':utils.clean_text(text),
                        'url':post_url,
                        'original_url':post_url,
                        'fetch_time':fetch_time,
                        'publish_time' : publish_time,
                        'link.url' : links,
                        'post':post
            }
	    #query={"query":{"match":{"_id":hashlib.md5(str(post_url.encode('utf8'))).hexdigest()}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
	    index = "forum_posts_" + month_year
            self.es.index(index=index, doc_type='post', id=hashlib.md5(str(post_url.encode('utf8'))).hexdigest(), body=json_posts)
	    #else:
		#data_doc = res['hits']['hits'][0]
                #if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    #self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url.encode('utf8'))).hexdigest(), body=json_posts)
	    if author_link == 'Null':
		continue
	    meta = json.dumps({'time' : publish_time})
            json_author = {}
            json_author.update({
                'post_id' : postid,
                'auth_meta' : meta,
                'crawl_status':0,
                'links' : author_link,
                })
            self.cursor.execute(auth_que, json_author)
	    self.conn.commit()
	nav_click = set(sel.xpath('//span//a[@rel="next"]//@href').extract())
        for post_nav_click in nav_click:
            if post_nav_click:
                post_nav_click = self.add_http(post_nav_click)
                yield Request(post_nav_click, callback=self.parse_all_pages_links)

