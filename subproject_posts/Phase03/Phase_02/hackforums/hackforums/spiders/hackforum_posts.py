from dbc_utils import get_googlecaptcha
from urllib import urlencode
import cfscrape
import sys
import json
import re
import datetime
import time
import MySQLdb
import scrapy
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import xpaths
sys.path.append('/home/epictions/tls_scripts/tls_utils')
from scrapy.selector import Selector
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib

crawl_query = utils.generate_upsert_query_authors_crawl('posts_hackforums')
query_posts = utils.generate_upsert_query_posts('posts_hackforums')

class Hackforums(scrapy.Spider):
    name ="hackforums_posts"
    handle_httpstatus_list = [403,410]

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
	self.conn,self.cursor = self.mysql_conn()
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
        conn = MySQLdb.connect(db="posts_hackforums", host="localhost", user="tls_dev", passwd="hdrn!", use_unicode=True, charset='utf8')
        cursor = conn.cursor()
	return conn,cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        scraper = cfscrape.create_scraper(delay=10)
        r = scraper.get('https://hackforums.net')#/member.php?action=login')
	'''
        cap_capth = '//script[@data-type="normal"]//@data-sitekey'
        sel = Selector(text=r.text)
        google_captcha = ''.join(sel.xpath(cap_capth).extract())
        data_ray = ''.join(sel.xpath('//script[@data-type="normal"]//@data-ray').extract())
	if data_ray and google_captcha:
	    g_captcha =  get_googlecaptcha('https://hackforums.net/member.php?action=login',google_captcha)
            parameters = {
	            'id': data_ray,
        	    'g-recaptcha-response': g_captcha
	        }
            captcha_url = 'https://hackforums.net/cdn-cgi/l/chk_captcha?' +urlencode(parameters)
            r2 = scraper.get(captcha_url)
	'''
        data = {
            'username': 'blackstorm52@protonmail.com',
            'password': 'Iwanttos33forums',
            'quick_gauth_code': '',
            'remember': 'yes',
            'submit': 'Login',
            'action': 'do_login',
            'url': ''
	}
        login_url = 'https://hackforums.net/member.php'#?action=login'
        r3 = scraper.post(login_url, data=data)
        cookies = r3.cookies.get_dict()
        cooks = r3.request.headers.get('Cookie').split(";")
        [cookies.update({cook.split("=")[0]:cook.split("=")[1]})
         for cook in cooks]
        #next_url = 'https://hackforums.net/showthread.php?tid=5926452'
        headers = {
            'authority': 'hackforums.net',
            'pragma': 'no-cache',
            'cache-control': 'no-cache',
            'upgrade-insecure-requests': '1',
            'user-agent': r3.request.headers.get('User-Agent'),
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'referer': 'https://hackforums.net/',
        }
	url_que = "select distinct(post_url) from hackforum_status where crawl_status = 0 "
        self.cursor.execute(url_que)
	self.conn.commit()
        data = self.cursor.fetchall()
        for url in data:
	    meta = {'Crawl_type':'keep up'}
            yield Request(url[0], callback=self.parse_thread, headers=headers, cookies=cookies,meta = meta)


    def parse_thread(self, response):
	headers = response.request.headers
        sel = Selector(response)
        if '&page=' in response.url:
            test = re.findall('&page=\d+',response.url)
            thread_url = response.url.replace(''.join(test),"")
        else:
            thread_url = response.url
        if '&page=' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'
        domain = 'www.hackforums.net'
        thread_title = ''.join(sel.xpath(xpaths.THREADTITLE).extract()).replace('Quick Reply','')
        post_title = ''
        try:category = sel.xpath(xpaths.CATEGORY).extract()[1]
        except:pass
        try:sub_category = ''.join(sel.xpath(xpaths.SUBCATEGORY).extract()[2])
        except: pass
        nodes = sel.xpath(xpaths.NODES)
        if nodes:
            query = 'update hackforum_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
	    self.conn.commit()
	elif 'The specified thread does not exist.' in response.body or \
		'is no longer available on this server and there is no forwarding address.' in response.body:
	    query = 'update hackforum_status set crawl_status = 9 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
            self.conn.commit()
	    #return None
	page_nav = ''.join(set(sel.xpath(xpaths.INNERPAGENAV).extract()))
        if page_nav:
	    try:
	    	text_case = ''.join(nodes[-1].xpath(xpaths.POSTID).extract()).replace('post_url_','')
		test_id = hashlib.md5(str(text_case)).hexdigest()
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		res = es.search(index="forum_posts", body={"query": query})
		if res['hits']['hits']==[]:
                    meta = {'Crawl_type':'catch up'}
                    page = "https://hackforums.net/" + page_nav
                    yield Request(page,callback = self.parse_thread, headers = headers, meta=meta)
	    except:pass

        for node in nodes:
            post_url = ''.join(node.xpath(xpaths.POSTURL).extract())
            post_id = ''.join(node.xpath(xpaths.POSTID).extract()).replace('post_url_','')
            publishs = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
            publish = publishs.split('M')[0] + 'M'
            publish_time = publish.replace(''.join(re.findall('\d+ minutes ago',publish) or re.findall('\d+ hours ago',publish)),'')
            try:
                publishdate = datetime.datetime.strptime(publish_time, '%m-%d-%Y, %I:%M %p')
            except:
                try:
                    publishdate = datetime.datetime.strptime(publish_time, '%m-%d-%Y, %I:%M %p ')
                except:
                    if '{1}' in publish:
                        try:
                            publis = ''.join(publish.replace('{1}',datetime.datetime.now().strftime('%m-%d-%Y')))
                            publishdate = datetime.datetime.strptime(publis, '%m-%d-%Y, %I:%M %p')
                        except:pass

            publish_epoch = time.mktime(publishdate.timetuple())*1000
            Text = '\n'.join(node.xpath(xpaths.POST_TEXT).extract())
            post_text = Text
            post_text = post_text.replace('mycode_quote', 'Quote ').replace('Quote Quote:','Quote:')

            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            author_url = ''.join(node.xpath(xpaths.AUTHOR_URLS).extract())
            links = node.xpath(xpaths.LINKS).extract()
            if '.gif' in links:
                links = node.xpath(xpaths.GIF_LINKS).extract()

            all_links = links
	    fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            json_posts = {'domain': domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'fetch_time': fetch_epoch,
                          'author': author,
                          'author_url': author_url,
                          'text': "{0}".format(utils.clean_text(post_text)),
                          'links': ', '.join(all_links)
            }

            json_posts.update({
                'author_url': author_url,
                'all_links': ', '.join(all_links)
            })
	    query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
 	        self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
	    else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)

            meta = {'publish_epoch': publish_epoch}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
             }
            self.cursor.execute(crawl_query, json_crawl)
	    self.conn.commit()

