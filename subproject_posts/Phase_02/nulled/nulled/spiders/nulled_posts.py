import  nulled_xpath
import datetime
from datetime import timedelta
import scrapy
import time
import re
import json
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from dbc_utils import get_googlecaptcha
import cfscrape
import requests
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
import hashlib
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
thread_que = utils.generate_upsert_query_posts('nulled')
a_que = utils.generate_upsert_query_authors_crawl('nulled')

class nulled(scrapy.Spider):
    name = "nulled_posts"
    handle_httpstatus_list = [403]

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db= "nulled", host ="localhost", user="root", passwd="qwe123", use_unicode=True,  charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        scraper = cfscrape.create_scraper()
        r = scraper.get("https://www.nulled.to/")#index.php?app=core&module=global&section=login")
        request_cookies = r.request._cookies.get_dict()
        response_cookies = r.cookies.get_dict()
        cookies = {}
        cookies.update(request_cookies)
        cookies.update(response_cookies)
        headers = {
    'authority': 'www.nulled.to',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': r.request.headers.get('User-Agent'),
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'referer': 'https://www.nulled.to/forum/110-feedback-and-suggestions/',
}

    	user_login = '123'  #.join(sel.xpath('//a[@id="user_link"]/@href').extract())
	if user_login:
		len_que = 'select count(*) from nulled_threads_crawl'
	        self.cursor.execute(len_que)
		self.conn.commit()
		total_links = self.cursor.fetchall()
		total_links = int(total_links[0][0])
		for i in range(1,total_links/5000+2):
                    select_que = "select distinct(post_url) from nulled_threads_crawl where crawl_status = 0 limit {0},{1}".format((i-1)*5000,5000)
                    self.cursor.execute(select_que)
		    self.conn.commit()
                    data = self.cursor.fetchall()
                    for url in data:
			meta = {'Crawl_type':'keep up','headers':headers,'update url':url[0]}
                        yield Request(url[0].replace("'",''),callback = self.parse_thread,headers=headers,meta = meta)#cookies = cookies, meta=meta)

    def parse_thread(self,response):
	fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        reference_url = response.request.url
        headers = response.meta.get('headers')
        a_val = {}
        nodes = response.xpath(nulled_xpath.node_xpath)
        try:category = response.xpath(nulled_xpath.category_xpath).extract()[1].encode('utf8')
        except:pass
        try:subcategory = '#<>#'.join(response.xpath(nulled_xpath.subcategory_xpath).extract()[1:]).encode('utf8').split('#<>#')
        except:pass
        try:sub_category_url = response.xpath('//ol[@class="breadcrumb top ipsList_inline left"]//a//@href').extract()[1:]
        except:pass
        threadtitle = ''.join(response.xpath(nulled_xpath.threadtitle_xpath).extract()).replace('\n','').replace('\t','')
        thread_url = ''.join(re.findall('(.*)page-\d+',reference_url)) or reference_url
        crawl_type = response.meta.get('Crawl_type','')

        next_page = ''.join(response.xpath('//div[@class="topic_controls"]//li[@class="next"]//a/@href').extract())
        if next_page:
	    try:
		post_url_ = ''.join(set(nodes[-1].xpath(nulled_xpath.posturl_xpath).extract()))
		test_id = hashlib.md5(post_url_.encode('utf8')).hexdigest()
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		res = es.search(index="forum_posts", body={"query": query})
		if res['hits']['hits']==[]:
                    meta = {'Crawl_type':'catch up','headers':headers}
                    yield Request(next_page,callback = self.parse_thread,headers = headers,meta=meta)
	    except:pass

        for node in nodes:
            postid = ''.join(set(node.xpath(nulled_xpath.postid_xpath).extract()))
            post_url = ''.join(set(node.xpath(nulled_xpath.posturl_xpath).extract()))
            ord_in_thread = ''.join(set(node.xpath(nulled_xpath.ord_in_thread).extract())).replace('#','').strip()
            publish_time_ = ''.join(set(node.xpath(nulled_xpath.publishtime_xpath).extract()))\
                    .replace('Today,',datetime.datetime.now().strftime('%d %B %Y -'))\
                    .replace('Yesterday,',(datetime.datetime.now() - timedelta(days=1)).strftime('%d %B %Y -'))
            try:publish_time_ = datetime.datetime.strptime(publish_time_, '%d %B %Y - %I:%M %p')
            except:pass
            publish_epoch =time.mktime(publish_time_.timetuple())*1000
	    if publish_epoch:
		month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            author  = ''.join(set(node.xpath(nulled_xpath.author_xpath).extract()))
            text = ' '.join(node.xpath(nulled_xpath.text_xpath).extract()).replace('citation',' Quote ')
            t_text = node.xpath('.//div[@itemprop="commentText"]//blockquote/@data-time').extract()
            a_text = node.xpath('.//div[@itemprop="commentText"]//blockquote/@data-author').extract()
            for t,a in zip(t_text,a_text):
                z = a+' '+t
                try:
                    z1 = 'Quote '+a+time.strftime(" on %d %b %Y - %I:%M %p, said:", time.localtime(int(t)))
                    text = text.replace(z,z1)
                except:pass
            links = '#<>#'.join(node.xpath(nulled_xpath.links_xpath).extract()).encode('utf8').split('#<>#')
            author_link = ''.join(set(node.xpath('.//div[@class="post_username"]//span[@itemprop="name"]//../@href').extract() or node.xpath('.//div[@class="author_info clearfix"]//ul[@class="basic_info"]//a[@itemprop="url"]/@href').extract()))
            all_links = '"'+str(links).replace('/gateway.php','https://www.nulled.to/gateway.php')+'"'
            author_data= {
                   'name':author,
                   'url':author_link
                   }
            post_data = {
			'cache_link':'',
			'author': json.dumps(author_data),
			'section':category,
			'language':'english',
			'require_login':'false',
			'sub_section':subcategory,
			'sub_section_url':sub_category_url,
			'post_id':postid,
			'post_title':'',
			'ord_in_thread':int(ord_in_thread),
			'post_url':post_url,
			'post_text':utils.clean_text(text).replace('\n', ''),
			'thread_title':threadtitle,
			'thread_url':thread_url
			} 
            json_posts = {
			  'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
			  'hostname':'nulled.to',
			  'domain': 'nulled.to',
			  'sub_type':'openweb',
			  'type':'forum',
			  'author':json.dumps(author_data),
			  'title':threadtitle,
			  'text':utils.clean_text(text).replace('\n', ''),
			  'url':post_url,
			  'original_url':post_url,
			  'fetch_time':fetch_time,
			  'publish_time':publish_epoch,
			  'link.url':all_links,
			  'post':post_data
            		}
            json_posts.update({
                          'domain': 'nulled.to',
                          'thread_url': thread_url,
                          'category': category,
                          'sub_category': str(subcategory),
                          'thread_title': threadtitle,
                          'post_title': '',
                          'author_url': author_link,
                          'all_links':  all_links,
                          'post_id': postid,
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'fetch_time': fetch_time,
                          'author': author,
                          'text': utils.clean_text(text),
                          })
	    self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(post_url.encode('utf8')).hexdigest(), body=json_posts)
            a_meta = json.dumps({
                'PublishTime': publish_epoch,
                'ThreadTitle': threadtitle
                })
            if author_link:
                a_val = ({
                'post_id' : postid,
                'links' : author_link,
                'auth_meta' : a_meta
                })
                self.cursor.execute(a_que,a_val)
		self.conn.commit()

        #status_code_update = 'update nulled_threads_crawl set crawl_status = 1 where post_url like "%{}%"'.format(reference_url)
        if nodes and crawl_type == 'keep up':
	    status_code_update = 'update nulled_threads_crawl set crawl_status = 1 where post_url like "%{}%"'.format(response.meta.get('update url').encode('utf8'))
            self.cursor.execute(status_code_update)
	    self.conn.commit()
