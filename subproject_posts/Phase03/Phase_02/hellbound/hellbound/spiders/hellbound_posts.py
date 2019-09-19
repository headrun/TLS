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
            category = ''.join(response.xpath(xpaths.CATEGORY).extract()[1])
            sub_category = ''.join(response.xpath(xpaths.SUB_CATEGORY).extract()[2])
            try:thread_title = response.xpath(xpaths.THREAD_TITLE).extract()[0].replace('RE: ','')
            except:thread_title = ''.join(response.xpath('//title/text()').extract()).replace('Thread |  Hellbound Hackers','')
            fetch_epoch = utils.fetch_time()
            reference_url = response.url
            json_values.update({
                        'domain':"www.hellboundhackers.org",
                        'category':category,
                        'sub_category': sub_category,
                        'thread_title':thread_title,
                        'post_url':'',
                        'thread_url': thread_url,
                        'fetch_time':utils.fetch_time(),
                        })
            #POST_URL is not availible in site.
            publish_times = response.xpath(xpaths.PUBLISH_TIME).extract()
            post_titles = response.xpath(xpaths.POST_TITLE).extract()
            authors_links = response.xpath(xpaths.AITHORS_LINKS).extract()
            author_name_nodes = response.xpath(xpaths.AUTHOR_NAME_NODES)
            text_nodes = response.xpath(xpaths.TEXT_NODES)
            post_ids = response.xpath(xpaths.POST_IDS).extract()
            #NEXT_PAGE REQUEST
            try:
                next_page = response.xpath(xpaths.NEXT_PAGE).extract()[0].replace('./','https://www.hellboundhackers.org/forum/')
		post_id_ = post_ids[-1].replace('post_','')
        	test_id = hashlib.md5(str(post_id_)).hexdigest()
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(post_ids[-1])}}
		res = es.search(index="forum_posts", body={"query": query})
		if res['hits']['hits']==[]:
	            yield Request(next_page, callback = self.parse_meta,meta = {'crawl_type':'catch up','thread_url':thread_url})
            except:pass
            all_zip = zip(author_name_nodes, publish_times, post_titles, authors_links, text_nodes, post_ids)
            for author_name_mode, publish_time, post_titles, a_link, text_node, post_id in all_zip:
                author = ''.join(author_name_mode.xpath('.//text()').extract())
                publish_epoch = utils.time_to_epoch(publish_time,"Posted on %d-%m-%y %H:%M" )
                a_link = a_link.replace('../../','https://www.hellboundhackers.org/')
		post_id = post_id.replace('post_','')
                json_values.update({
                            'author': author,
                            'author_url': a_link,
                            'publish_time': publish_epoch,
                            'post_title': post_titles,
                            'post_id':post_id
                            })
                hr = text_node.xpath('.//hr')
                if hr:
                    post_text = text_node.xpath(xpaths.TEXT_HR).extract()
                    all_links = text_node.xpath(xpaths.ALL_LINKS_HR).extract()
                else:
                    post_text = text_node.xpath(xpaths.TEXT).extract()
                    all_links = text_node.xpath(xpaths.ALL_LINKS_NO_HR).extract()
                post_text = utils.clean_text(' '.join(post_text))
                all_links = ', '.join(set(all_links)).replace('../user/','https://www.hellboundhackers.org/user/')
                json_values.update({
                            'text':post_text,
                            'links':all_links
                            })
		query={"query":{"match":{"_id":hashlib.md5(str(post_id)).hexdigest()}}}
                res = self.es.search(body=query)
                if res['hits']['hits'] == []:
	      	    self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_id)).hexdigest(), body=json_values)
		else:
		    data_doc = res['hits']['hits'][0]
                    if (json_values['links'] != data_doc['_source']['links']) or (json_values['text'] != data_doc['_source']['text']):
			self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_id)).hexdigest(), body=json_values)

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
	

