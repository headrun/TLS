import sys
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import json
import xpaths
import MySQLdb
import time
import re
from MySQLdb import OperationalError
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
from pprint import pprint 
POST_QUERY = utils.generate_upsert_query_posts('hellbound')
AUTHOR_CRAWL_QUERY = utils.generate_upsert_query_authors_crawl('hellbound')


class Hellbound(scrapy.Spider):
    name = "hellbound_posts"
    handle_httpstatus_list = range(400,505)

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
	self.conn, self.cursor = self.mysql_conn()
        dispatcher.connect(self.close_conn, signals.spider_closed)
    
    def mysql_conn(self):
        conn = MySQLdb.connect(db= "hellbound", host = "localhost", user="tls_dev", passwd="hdrn!",use_unicode=True,charset="utf8mb4")
        cursor = conn.cursor()
        return conn,cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = "select distinct(post_url) from hellbound_threads_crawl where crawl_status = 0 "
        try:
	    self.cursor.execute(select_que)
	    self.conn.commit()
            data = self.cursor.fetchall()
	except MySQLdb.OperationalError as e:
            if 'MySQL server has gone away' in str(e):
                self.conn,self.cursor = self.mysql_conn()
		self.cursor.execute(select_que)
		self.conn.commit()
		data = self.cursor.fetchall()
	    else: raise e()
        meta = {'crawl_type':'keep up'}
        for url in data:
	    yield Request(url[0], callback = self.parse_meta,meta = {'crawl_type':'keep up','thread_url':url[0]})

    def parse_meta(self, response):
	if response.status in range(400,599):
            UP_QUE_TO_9 = 'update hellbound_threads_crawl set crawl_status= 9 where post_url = "%s"'%response.url
            self.cursor.execute(UP_QUE_TO_9)
	    self.conn.commit()
        else:
	    json_values = {}
            thread_url = response.meta.get('thread_url','')
            category = ''.join(response.xpath(xpaths.CATEGORY).extract()[1]) or 'Null'
            sub_category = ''.join(response.xpath(xpaths.SUB_CATEGORY).extract()[2]) or 'Null'
	    sub_categoryurl = response.xpath('//h1[@class="smallalt"]//a/@href').extract()[2]
	    if sub_categoryurl:
		sub_category_url = 'https://www.hellboundhackers.org/forum/' + sub_categoryurl
	    if sub_categoryurl == '':
		sub_categoryurl = 'Null'
		sub_category_url = sub_categoryurl
            try:thread_title = response.xpath(xpaths.THREAD_TITLE).extract()[0].replace('RE: ','') or 'Null'
            except:thread_title = ''.join(response.xpath('//title/text()').extract()).replace('Thread |  Hellbound Hackers','') or 'Null'
            fetch_epoch = utils.fetch_time()
            reference_url = response.url
            '''json_values.update({
			'hostname':"www.hellboundhackers.org",	
                        'domain':"hellboundhackers.org",
			'sub_type':'openweb',
			'type':'forum',
                        })'''
            #POST_URL is not availible in site.
            publish_times = response.xpath(xpaths.PUBLISH_TIME).extract()
            post_titles = response.xpath(xpaths.POST_TITLE).extract()
            authors_links = response.xpath(xpaths.AITHORS_LINKS).extract()
            author_name_nodes = response.xpath(xpaths.AUTHOR_NAME_NODES)
            text_nodes = response.xpath(xpaths.TEXT_NODES)
            post_ids = response.xpath(xpaths.POST_IDS).extract()
            #NEXT_PAGE REQUEST
            try:
                next_page = response.xpath(xpaths.NEXT_PAGE).extract_first().replace('./','https://www.hellboundhackers.org/forum/')
		'''post_id_ = post_ids[-1].replace('post_','')
        	test_id = hashlib.md5(str(post_id_)).hexdigest()
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(post_ids[-1])}}
		res = es.search(index="forum_posts", body={"query": query})
		if res['hits']['hits']==[]:'''
	        yield Request(next_page, callback = self.parse_meta,meta = {'crawl_type':'catch up','thread_url':thread_url})
            except:pass
	    x = 0
            all_zip = zip(author_name_nodes, publish_times, post_titles, authors_links, text_nodes, post_ids)
            for author_name_mode, publish_time, post_titles, a_link, text_node, post_id in all_zip:
		x = x+1
                author = ''.join(author_name_mode.xpath('.//text()').extract()) or 'Null'
                publish_epoch = utils.time_to_epoch(publish_time,"Posted on %d-%m-%y %H:%M" )
		if publish_epoch:
                    year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                    if year > '2011':
		        month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                    else:
                        continue
		else:
		    import pdb;pdb.set_trace()

                a_link = a_link.replace('../../','https://www.hellboundhackers.org/')
	        if a_link == '':
		    a_link = 'Null'
		post_id = post_id.replace('post_','') or 'Null'
		author_data = {
			'name':author,
			'url':a_link
			}
                hr = text_node.xpath('.//hr')
                if hr:
                    post_text = text_node.xpath(xpaths.TEXT_HR).extract()
                    all_links_ = text_node.xpath(xpaths.ALL_LINKS_HR).extract()
                else:
                    post_text = text_node.xpath(xpaths.TEXT).extract()
                    all_links_ = text_node.xpath(xpaths.ALL_LINKS_NO_HR).extract()
                post_text = utils.clean_text(' '.join(post_text))
		if post_text == '':
		    post_text = 'Null'
                all_links = ', '.join(set(all_links_)).replace('../user/','https://www.hellboundhackers.org/user/')
		if all_links == '':
		    all_links = 'Null'
		if all_links_ == []:
		    all_links = 'Null'
		post = {
			'cache_link':'',
			'author':json.dumps(author_data),
			'section':category,
			'language':'english',
			'require_login':'false',
			'sub_section':sub_category,
			'sub_section_url':sub_category_url,
			'post_id':post_id,
			'post_title':post_titles,
			'ord_in_thread':x,
			'post_url':'Null',
			'post_text':post_text.replace('\n', ''),
			'thread_title':thread_title,
			'thread_url':thread_url
			}
                json_values.update({
		 	    'record_id' : 'Null',
			    'hostname':"www.hellboundhackers.org",
                            'domain':"hellboundhackers.org",
                            'sub_type':'openweb',
                            'type':'forum',
			    'author':json.dumps(author_data),
			    'title':thread_title,
                            'text':post_text.replace('\n',''),
                            'url':'Null',
			    'original_url':'Null',
			    'fetch_time':fetch_epoch,
			    'publish_time':publish_epoch,
			    'link.url':all_links,
			    'post':post
                            })
		#query={"query":{"match":{"_id":hashlib.md5(str(post_id)).hexdigest()}}}
                #res = self.es.search(body=query)
                #if res['hits']['hits'] == []:
	      	self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(post_id)).hexdigest(), body=json_values, request_timeout=30)
		#else:
		    #data_doc = res['hits']['hits'][0]
                    #if (json_values['links'] != data_doc['_source']['links']) or (json_values['text'] != data_doc['_source']['text']):
			#self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_id)).hexdigest(), body=json_values)

		a_meta = {'publish_epoch':publish_epoch}
                json_author = {}
                json_author.update({
                            'post_id':post_id.replace('post_',''),
                            'auth_meta': json.dumps(a_meta),
                            'links': a_link,
                            'crawl_status': 0
                            })
                try:
		    self.cursor.execute(AUTHOR_CRAWL_QUERY,json_author)
		    self.conn.commit()
                except  MySQLdb.OperationalError as e:
                    if 'MySQL server has gone away' in str(e):
                        self.conn,self.cursor = self.mysql_conn()
			self.cursor.execute(AUTHOR_CRAWL_QUERY,json_author)
			self.conn.commit()
                    else:raise e()
            if post_ids:
                UP_QUE_TO_1 = 'update hellbound_threads_crawl set crawl_status= 1 where post_url = "%s"'%response.url
                try:
		    self.cursor.execute(UP_QUE_TO_1)
		    self.conn.commit()
                except  MySQLdb.OperationalError as e:
                    if 'MySQL server has gone away' in str(e):
                        self.conn,self.cursor = self.mysql_conn()
	           	self.cursor.execute(UP_QUE_TO_1)
			self.conn.commit()
                    else:raise e()
	
	

