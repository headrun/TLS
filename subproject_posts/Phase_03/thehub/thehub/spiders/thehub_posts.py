#encoding: utf-8
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request, FormRequest
import datetime
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
from elasticsearch import Elasticsearch
import hashlib
import tls_utils as utils
from urllib import urlencode
crawl_query = utils.generate_upsert_query_authors_crawl('thehub_tor')
from thehub.items import Posts_item


class Thehub(scrapy.Spider):
    name = "thehub_posts"
    start_urls = ["http://thehub7xbw4dc5r2.onion/"]

    def __init__(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="posts",host="localhost",user="root",passwd="qwe123" , use_unicode = True , charset = 'utf8mb4')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        sel = Selector(response)
        name = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@name').extract())
        value = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@value').extract())
        params = (
    ('action', 'login2'),
)
        data={
name : value,
'cookielength':'',
'hash_passwrd':'',
'passwrd'       :'!nq!nq7@6',
'user': 'inqspdr',
}
        url = 'http://thehub7xbw4dc5r2.onion/index.php?' +urlencode(params)
        yield FormRequest(url,formdata=data,callback=self.after_login)


    def after_login(self, response):
        url_que = "select distinct(post_url) from thehub_status where crawl_status = 0"
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
	    #url = ['http://thehub7xbw4dc5r2.onion/index.php?topic=4035.0']
            yield Request(url[0], callback = self.parse_thread)#, errback=self.pte)

    def pte(self, response):
	pass
	#import pdb;pdb.set_trace()

    def parse_thread(self, response):
	if "The topic or board you are looking for appears to be either missing or off limits to you." in response.body:
            up_que = 'update thehub_status set crawl_status = 9 where post_url = %(url)s'
            self.cursor.execute(up_que,{'url':response.request.url})
            self.conn.commit()
	    return
	sel = Selector(response)
	domain = "thehub7xbw4dc5r2.onion"
	categories = sel.xpath('//div[@id="main_content_section"]//div[@class="navigate_section"]//a//span//text()').extract()
	sub = categories[len(categories)/2:]
 	category = sub[1] 
	sub_category = sub[2:-1]
	if sub_category == []:
	    sub_category = sub[2:]
	sub_cat_urls = sel.xpath('//div[@id="main_content_section"]//div[@class="navigate_section"]//a/@href').extract()
	cat_urls = sub_cat_urls[len(sub_cat_urls)/2:]
	sub_category_url = cat_urls[2:-1]
	if sub_category_url == []:
	    sub_category_url = cat_urls[2:]
	thread_url = response.url
	thread_title = ''.join(response.xpath('//h3[@class="catbg"]/text()').extract()).split('(')[0].replace('\n','').replace('\t','') or 'Null'
	nodes = sel.xpath('//div[@id="forumposts"]//div[contains(@class,"windowbg")]')
	if nodes:
	    up_que = 'update thehub_status set crawl_status = 1 where post_url = %(url)s'
	    self.cursor.execute(up_que,{'url':response.request.url})
	    self.conn.commit()

	ord_in_thread = 0
	for node in nodes:
	    ord_in_thread = ord_in_thread+1	
	    post_title= ''.join(node.xpath('.//h5[contains(@id,"subject_")]//text()').extract()).replace('\n','').replace('\t','') or 'Null'
	    post_url =  ''.join(node.xpath('.//h5[contains(@id,"subject_")]//a/@href').extract())
	    if post_url == '':
		post_url = 'Null'
	    post_id = ''.join(re.sub('(.*)#msg','',post_url))
	    if post_id == '':
		post_id = 'Null'
	    text = ''.join(node.xpath('.//div[@class="post"]//div[@class="inner"]/text() | .//div[@class="post"]//div[contains(@class,"quote")]//a/text() | .//div[@class="post"]//text()| .//div[@class="post"]//img[@class="smiley"]//@alt | .//div[@class="smalltext modified"]//em/text()| .//blockquote/@class').extract()).replace('bbc_standard_quote','Quote').replace('bbc_alternate_quote','Quote').replace('\n', '').strip()
	    author = ''.join(node.xpath('.//div[@class="poster"]//h4//text()').extract()).replace('\n','').replace('\t','') or 'Null'
	    author_url = ''.join(node.xpath('.//div[@class="poster"]//h4//@href').extract())
	    if author_url == '':
		author_url = 'Null'
	    publish = ''.join(node.xpath('.//div[@class="smalltext"]//strong//following-sibling::text()').extract())
	    published = re.sub('\xbb','',publish).strip()
	    try:
	        publish_date = datetime.datetime.strptime(published,'%B %d, %Y, %H:%M:%S %p')
	        publish_epoch = time.mktime(publish_date.timetuple())*1000
	        if publish_epoch:
		    month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
	    except:
		publish_epoch = 0
	    links = node.xpath('.//div[@class="post"]//div[contains(@class,"quote")]//a/@href').extract()
	    all_links = ', '.join(links)
	    if all_links == '':
		all_links = 'Null'
	    if links == []:
		all_links = 'Null'
	    author_data = {
			'name':author,
			'url':author_url
			}
	    post = {
		'cache_link':'',
		'section':category,
		'language':'english',
		'require_login':'true',
		'sub_section':', '.join(sub_category),
		'sub_section_url':', '.join(sub_category_url),
		'post_id':post_id,
		'post_title':post_title,
		'ord_in_thread':ord_in_thread,
		'post_url':post_url,
		'post_text':utils.clean_text(text),
		'thread_title':thread_title,
		'thread_url':thread_url
		}
	    json_posts = {'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
			  'hostname':'thehub7xbw4dc5r2.onion',
			  'domain': domain,
			  'sub_type':'openweb',
			  'type':'forum',
			  'author':json.dumps(author_data),
			  'title':thread_title,
			  'text':utils.clean_text(text),
			  'url':post_url,
			  'original_url':post_url,
			  'fetch_time':int(datetime.datetime.now().strftime("%s")) * 1000,
			  'publish_time':publish_epoch,
			  'link_url':all_links,
			  'post':post
			  }
	    #yield obj
	    sk = hashlib.md5(str(post_url)).hexdigest()
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts) 
	    #db data
	    meta = {'publish_epoch': publish_epoch}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
	    self.conn.commit()
	page_nav = sel.xpath('//div[@class="pagelinks floatleft"]//a[@class="navPages"]/@href').extract()
        for page in page_nav:
            yield Request(page, callback=self.parse_thread)
