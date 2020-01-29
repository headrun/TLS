import hashlib
import random 
from elasticsearch import Elasticsearch
from scrapy.http import FormRequest
import cfscrape
import time
import datetime
import json
import re
import unicodedata
from scrapy.selector import Selector
from scrapy.spiders import BaseSpider
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from pprint import pprint 
import MySQLdb
from binrev_xpaths import *
#import utils
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
auth_que = utils.generate_upsert_query_authors_crawl('binrev')


class formus(BaseSpider):
    name = 'binrev_posts'
    handle_httpstatus_list = [407]

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user="tls_dev", passwd="hdrn!", db="binrev", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
	self.es = Elasticsearch(['10.2.0.90:9342'])
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()



    def add_http(self, url):
        '''
          Adding http link
        '''
        if 'http' not in url:
            url = 'http://www.binrev.com/%s'%url
        return url 
    '''
    def start_requests_(self):
        yield Request('http://www.binrev.com/forums/index.php?/login/',callback =self.login_page1)

    def login_page1(self,response):
        csrfKey = ''.join(response.xpath('//input[@name="csrfKey"]/@value').extract())
        data = {
                'login__standard_submitted': '1',
'csrfKey': csrfKey,
'auth': 'saikrishna',
'password': 'ammananna1@',
'remember_me': '0',
'remember_me_checkbox': '1',
'signin_anonymous': '0',
}
	yield FormRequest('http://www.binrev.com/forums/index.php?/login/', callback = self.login_page,formdata = data)
	'''
    def start_requests(self):#login_page(self, response):
        url_que = "select distinct(post_url) from binrev_browse where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_all_pages_links)

    def parse_all_pages_links(self, response):
 	if '&page=' in response.url:
            crawl_type = "catchup"
            test = re.findall(r'&page=\d+', response.url)
            thread_url = response.url.replace(''.join(test), "").replace('&do=findComment', '')
            thread_url =  utils.clean_url(thread_url)
        else:
            crawl_type = "keepup"
            thread_url = response.url
            thread_url =  utils.clean_url(thread_url)

 	sel = Selector(response)
        domain = 'binrev.com'
        category = sel.xpath('//span[@itemprop="name"]/text()').extract()[1].strip() or 'Null'
        subcategory = sel.xpath('//span[@itemprop="name"]/text()').extract()[2].strip() or 'Null'
	sub_category_url = sel.xpath('//a[@itemprop="item"]/@href').extract()[2] or 'Null'
        thread_title = ''.join(sel.xpath(THREAD_TITLE).extract()) or 'Null'
	crawl_type = ''
        post_title = 'Null'
        nodes = sel.xpath(NODES)
	if nodes:
            query = 'update binrev_browse set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)	
        '''nav_click = ''.join(set(sel.xpath('//li[@class="ipsPagination_next"]//a//@href').extract()))
        if nav_click:
	    try:
		post_url_ = ''.join(nodes[-1].xpath(POST_URL).extract())
	 	test_id = hashlib.md5(str(post_url_)).hexdigest()
                query = {'query_string': {'use_dis_max': 'true', 'query': '_id : {0}'.format(test_id)}}
                res = self.es.search(index="forum_posts", body={"query": query})
                if res['hits']['hits'] == []:
		    post_nav_click = self.add_http(nav_click)
                    yield Request(post_nav_click, callback=self.parse_all_pages_links)
	    except:pass'''
	x = 0
        for node in nodes:
	    x = x+1
            author_name = ''.join(node.xpath(AUTHOR_NAME).extract()) or 'Null'
            author_link = ''.join(node.xpath(AUTHOR_LINK).extract()) or 'Null'
            post_url = ''.join(node.xpath(POST_URL).extract()) or 'Null'
            postid = post_url.split('=')[-1] ## try in re without indexing
            publish_time = ''.join(node.xpath(PUBLISH_TIME).extract()) or 'Null'
            publish_time = datetime.datetime.strptime((publish_time), '%m/%d/%Y %H:%M %p') ## need to check later
            publish_time = time.mktime(publish_time.timetuple())*1000
	    if publish_time:
                year = time.strftime("%Y", time.localtime(int(publish_time/1000)))
                if year > '2011':
		    month_year = time.strftime("%m_%Y", time.localtime(int(publish_time/1000)))
                else:
                    continue
	    else:
		import pdb;pdb.set_trace()

            fetch_time = (round(time.time()*1000))
            TEXT_NEW = './/div[@data-role="commentContent"]//p//text() | .//div[@data-role="commentContent"]//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//img//@alt | .//div[@class="ipsEmbeddedVideo"]//@src | .//blockquote[@class="ipsQuote"]/p/text() |.//blockquote[@class="ipsQuote"]/@class | .//div[@data-role="commentContent"]//blockquote/@data-ipsquote-username | .//div[@data-role="commentContent"]//blockquote/@data-ipsquote-timestamp'
            text = node.xpath(TEXT_NEW).extract() or 'Null'#.strip().encode('ascii','ignore').decode('utf8')
            text = utils.clean_text(' '.join(text))
            text = text.replace('ipsQuote', 'Quote')
            TEXT_A = './/div[@data-role="commentContent"]//blockquote/@data-ipsquote-username'
            text_a = node.xpath(TEXT_A).extract()
            TEXT_T = './/div[@data-role="commentContent"]//blockquote/@data-ipsquote-timestamp'
            text_t = node.xpath(TEXT_T).extract()
            for ta, tt in zip(text_a, text_t):
                z = ta+' '+tt
                z1 = tt+' '+ta
                try:
                    tz = time.strftime("On %d/%m/%Y at %I:%M %p, ", time.localtime(int(tt)))
                except:
                    pass
                zz = tz+ta+' Said: '
                text = text.replace(z, zz).replace(z1, zz)
            text = text.replace('Quote \nQuote', 'Quote \n')
            if not text_t and text_a:
                for auth in text_a:
                    z = 'Quote '+auth
                    text = text.replace(z, z + ' Said: ')
            ext = ''.join(re.findall(r'\d+ \w+ ago, \w+ said:', text))
            ext1 = ''.join(re.findall(r'On \d+-\d+-\d+ at \d+:\d+ \wM, \w+ said:', text)) 
            ext2 = ''.join(re.findall(r'On \d+/\d+/\d+ at \d+:\d+ \wM, \w+ said:', text))
            ext3 = ''.join(re.findall(r'On \d+/\d+/\d+ at \d+:\d+ \wM,  said:', text))
            ext4 = ''.join(re.findall(r'On \w+ \d+, \d+ at \d+:\d+ \wM, \w+ said:', text))
            text = text.replace(ext, '').replace(ext1, '').replace(ext2, '').replace(ext3, '').replace(ext4, '')
            Links = node.xpath(LINK).extract()
            Link = []
            for link_ in Links:
                if 'http' not in link_: link_ = 'https:'+ link_
                if not 'emoticons' in link_:
                    Link.append(link_)
	    links = ', '.join(Link)
	    if links == '':
	        links = 'Null'
	    if Links == []:
	        Links = 'Null'
		links = Links
            #links = str(Link)
	    author_data = {
                'name':author_name,
                'url':author_link
                }
              
	    post = {
		'cache_link':'',
		'author':json.dumps(author_data),
		'section':category,
		'language':'english',
		'require_login':'false',
		'sub_section':subcategory,
		'sub_section_url':sub_category_url,
		'post_id':postid,
		'post_title':post_title,
		'ord_in_thread':x,
		'post_url':post_url,
		'post_text':utils.clean_text(text),
		'thread_title':thread_title,
		'thread_url':thread_url
		}
  	    json_posts = {
			  'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
			  'hostname':'www.binrev.com',
			  'domain' : domain,
			  'sub_type':'openweb',
			  'type':'forum',
			  'author':json.dumps(author_data),
			  'title':thread_title,
			  'text':utils.clean_text(text),
			  'url':post_url,
			  'original_url':post_url,
			  'fetch_time':fetch_time,
                          'publish_time' : publish_time,
                          'link.url' : links,
			  'post':post
            }
	    #query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            #res = self.es.search(body=query)
	    #d_test = res['hits']['hits']
            #try:
		#if 'post_id' not in d_test[0].get("_source").keys():
		    #self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts) 
	    #except:
		#if d_test ==[]:
		    #self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
	    #if res['hits']['hits'] == []:
	    self.es.index(index="forum_posts_" + month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
	    #else:
		#data_doc = res['hits']['hits'][0]
		#if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    #self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
	    
	    #self.cursor.execute(query_posts, json_posts)
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
	nav_click = ''.join(set(sel.xpath('//li[@class="ipsPagination_next"]//a//@href').extract()))
	if nav_click:
            post_nav_click = self.add_http(nav_click)
            yield Request(post_nav_click, callback=self.parse_all_pages_links)
        


       



