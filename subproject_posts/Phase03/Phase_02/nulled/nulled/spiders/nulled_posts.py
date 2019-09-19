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
        self.conn = MySQLdb.connect(db= "nulled", host ="localhost", user="tls_dev",passwd="hdrn!",use_unicode=True,  charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        scraper = cfscrape.create_scraper()
	import pdb;pdb.set_trace()
        r = scraper.get("https://www.nulled.to")
        request_cookies = r.request._cookies.get_dict()
        response_cookies = r.cookies.get_dict()
        cookies = {}
        cookies.update(request_cookies)
        cookies.update(response_cookies)
        '''sel = Selector(text=r.text)
        auth_key = ''.join(set(sel.xpath('//form//input[@name="auth_key"]/@value').extract()))
        google_captcha = ''.join(set(sel.xpath('//div[@class="g-recaptcha"]/@data-sitekey').extract()))
        g_captcha = get_googlecaptcha('https://www.nulled.to/index.php?app=core&module=global&section=login', google_captcha)
        data = {
          'auth_key': auth_key,
          'referer': 'https://www.nulled.to/index.php?app=core&module=global&section=login',
          'ips_username': 'inqspdr2',
          'ips_password': 'lolw4@123~',
          'g-recaptcha-response': g_captcha,
          'rememberMe': '1'
        }
        if g_captcha and len(g_captcha)>5:
            login_url = 'https://www.nulled.to/index.php?app=core&module=global&section=login&do=process'
            r2 = scraper.post(login_url,data = data)
            request_cookies_ = r2.request._cookies.get_dict()
            response_cookies_ = r2.cookies.get_dict()
            cookies_ = {}
            cookies_.update(request_cookies_)
            cookies_.update(response_cookies_)
	'''
        headers = {
    'authority': 'www.nulled.to',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': r.request.headers.get('User-Agent'),
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'referer': 'https://www.nulled.to/forum/110-feedback-and-suggestions/',
}
    	#    sel =  Selector(text=r2.text)
    	#    user_login = ''.join(sel.xpath('//a[@id="user_link"]/@href').extract())

        user_login = 'no need log in'
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
                    meta = {'Crawl_type':'keep up','headers':headers}
                    for url in data:
                        yield Request(url[0].replace("'",''),callback = self.parse_thread,headers=headers,cookies = cookies, meta=meta)

    def parse_thread(self,response):
	fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        reference_url = response.url
        headers = response.meta.get('headers')
        json_posts = {}
        a_val = {}
        nodes = response.xpath(nulled_xpath.node_xpath)
        try:category = response.xpath(nulled_xpath.category_xpath).extract()[1].encode('utf8')
        except:pass
        try:subcategory = '#<>#'.join(response.xpath(nulled_xpath.subcategory_xpath).extract()[1:]).encode('utf8').split('#<>#')
        except:pass
        threadtitle = ''.join(response.xpath(nulled_xpath.threadtitle_xpath).extract()).replace('\n','').replace('\t','')
        thread_url = ''.join(re.findall('(.*)page-\d+',reference_url)) or reference_url
        crawl_type = response.meta.get('Crawl_type','')

        next_page = ''.join(response.xpath('//div[@class="topic_controls"]//li[@class="next"]//a/@href').extract())
        if next_page:
	    try:
		post_url_ = ''.join(set(nodes[-1].xpath(nulled_xpath.posturl_xpath).extract()))
		test_id = hashlib.md5(str(post_url_)).hexdigest()
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		res = es.search(index="forum_posts", body={"query": query})
		if res['hits']['hits']==[]:
                    meta = {'Crawl_type':'catch up','headers':headers}
                    yield Request(next_page,callback = self.parse_thread,headers = headers,meta=meta)
	    except:pass

        for node in nodes:
            postid = ''.join(set(node.xpath(nulled_xpath.postid_xpath).extract()))
            post_url = ''.join(set(node.xpath(nulled_xpath.posturl_xpath).extract()))
            publish_time_ = ''.join(set(node.xpath(nulled_xpath.publishtime_xpath).extract()))\
                    .replace('Today,',datetime.datetime.now().strftime('%d %B %Y -'))\
                    .replace('Yesterday,',(datetime.datetime.now() - timedelta(days=1)).strftime('%d %B %Y -'))
            try:publish_time_ = datetime.datetime.strptime(publish_time_, '%d %B %Y - %I:%M %p')
            except:pass
            publishtime =time.mktime(publish_time_.timetuple())*1000
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
            json_posts.update({
                          'domain': 'www.nulled.to',
                          'thread_url': thread_url,
                          'category': category,
                          'sub_category': str(subcategory),
                          'thread_title': threadtitle,
                          'post_title': '',
                          'author_url': author_link,
                          'links':  all_links,
                          'post_id': postid,
                          'post_url': post_url,
                          'publish_time': publishtime,
                          'fetch_time': fetch_time,
                          'author': author,
                          'text': utils.clean_text(text),
                          })
            query={"query":{"match":{"_id":hashlib.md5(post_url.encode('utf8')).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
		self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(post_url.encode('utf8')).hexdigest(), body=json_posts)
            a_meta = json.dumps({
                'PublishTime': publishtime,
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

        status_code_update = 'update nulled_threads_crawl set crawl_status = 1 where post_url like "%{}%"'.format(reference_url)
        if nodes and crawl_type == 'keep up':
            self.cursor.execute(status_code_update)
	    self.conn.commit()
