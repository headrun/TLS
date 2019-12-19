import scrapy
from elasticsearch import Elasticsearch
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import json
import xpaths
import MySQLdb
import sys
import hashlib
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import re
import time
import tls_utils as utils
POST_QUERY = utils.generate_upsert_query_posts('bbs')
AUTHOR_CRAWL_QUERY = utils.generate_upsert_query_authors_crawl('bbs')


class Bbs_posts(Spider):
    name = "bbs_posts"
    def __init__(self):
        self.conn = MySQLdb.connect(db= "posts",host="localhost",user="tls_dev",passwd="hdrn!" , use_unicode = True , charset = 'utf8')
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = "select distinct(post_url) from bbs_crawl where crawl_status = 0"
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        meta = {'crawl_type':'keep up'}
        for url in data:
            url = url[0]
            yield Request(url, callback = self.parse_meta, meta = {'crawl_type':'keep up','thread_url':url})

    def parse_meta(self, response):
        crawl_type = response.meta.get('crawl_type','')
        category = ''.join(response.xpath(xpaths.CATEGORY).extract()).strip()
        sub_category = ''.join(response.xpath(xpaths.SUB_CATEGORY).extract()).strip() or 'Null'
        sub_categoryurl = ''.join(response.xpath(xpaths.SUBCATEGORY_URL).extract()).strip()
        if sub_categoryurl:
            sub_category_url = 'https://bbs.pediy.com/' + sub_categoryurl
        if sub_categoryurl == '':
            sub_category_url = 'Null'
        thread_title = ''.join(response.xpath(xpaths.THREAD_TITLE).extract()).strip()
        thread_url = response.meta.get('thread_url')
        thread_url = response.meta.get('thread_url','')
        nodes = response.xpath(xpaths.NODES)
        ord_in_thread = 0
        for node in nodes:
            ord_in_thread = ord_in_thread+1
            publish_epoch = utils.time_to_epoch(''.join(node.xpath(xpaths.PUBLISH_EPOCH).extract()).strip(),'%Y-%m-%d %H:%M')
            if publish_epoch:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            else:
                import pdb;pdb.set_trace()
            author_name = ''.join(node.xpath(xpaths.AUTHOR).extract())
            author_url = 'https://bbs.pediy.com/'+''.join(node.xpath(xpaths.AUTHOR_URL).extract())
            post_id = ''.join(node.xpath('.//@data-pid').extract())
            post_text = ' '.join(node.xpath(xpaths.POST_TEXT).extract()).strip() 
            posturl = ''.join(node.xpath(xpaths.POST_URL).extract())
            if "http" not in posturl: posturl = 'https://bbs.pediy.com/' + posturl
            links = node.xpath(xpaths.ALL_LINKS).extract()
            fetch_epoch = utils.fetch_time()
            all_links = []
            for i in links:
                if 'http'not in i: i = 'https://bbs.pediy.com/'+i
                all_links.append(i)
            author_data = {
                'name':author_name,
                'url':author_url
                }
            post = {
                'cache_link':'',
                'author':json.dumps(author_data),
                'section':category,
                'language':'chinese',
                'require_login':'false',
                'sub_section':sub_category,
                'sub_section_url':sub_category_url,
                'post_id':post_id,
                'post_title':'Null',
                'ord_in_thread':ord_in_thread,
                'post_url':posturl,
                'post_text':utils.clean_text(post_text).replace('\n',''),
                'thread_title':thread_title,
                'thread_url':thread_url
                }
            json_posts = {
                          'record_id':'Null',
                          'hostname':'bbs.pediy.com',
                          'domain': 'bbs.pediy.com',
                          'sub_type':'openweb',
                          'type':"forum",
                          'author':json.dumps(author_data),
                          'title':thread_title,
                          'text':utils.clean_text(post_text).replace('\n',''),
                          'url':posturl,
                          'original_url':posturl,
                          'fetch_time':fetch_epoch,
                          'publish_time':publish_epoch,
                          'link.url':links,
                          'post':post
            }
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(post_id).hexdigest(), body=json_posts, request_timeout=30)

            if author_url:
                json_crawl = {
                            'post_id':post_id,
                            'auth_meta': json.dumps({'publish_epoch':publish_epoch}),
                            'links': author_url,
                            'crawl_status': 0
                            }
                self.cursor.execute(AUTHOR_CRAWL_QUERY,json_crawl)
                self.conn.commit()
        #NEXT_PAGE REQUEST
        try:
            next_page = 'https://bbs.pediy.com/' + response.xpath(xpaths.NEXT_PAGE).extract()[0]
            yield Request(next_page, callback = self.parse_meta,meta = {'crawl_type':'catch up','thread_url':thread_url})
        except:pass
        #for post data not commats 
        '''if 'keep up'== crawl_type:
            post_id = ''.join(re.findall('thread-\d+',response.url)).replace('thread-','')
            text = ' '.join(response.xpath(xpaths.TEXT).extract()).strip()
            auth = ''.join(response.xpath(xpaths.A_NAME).extract()).strip()
            auth_url = 'https://bbs.pediy.com/'+''.join(response.xpath(xpaths.A_URL).extract())
            publish = ''.join(response.xpath(xpaths.P_EPOCH).extract())
            publish_epoch = utils.time_to_epoch(publish,'%Y-%m-%d %H:%M')
            if publish_epoch:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            else:
                import pdb;pdb.set_trace()
            links = response.xpath(xpaths.ALL_LINKS1).extract() 
            author_data = {
                'name':author_name,
                'url':author_url
                }
            all_links = []
            for j in links:
                if j:
                    if 'http'not in j:
                        j = 'https://bbs.pediy.com/'+j
                    all_links.append(j)              
            post = {
                'cache_link':'',
                'author':json.dumps(author_data),
                'section':category,
                'language':'chinese',
                'require_login':'false',
                'sub_section':sub_category,
                'sub_section_url':sub_category_url,
                'post_id':post_id,
                'post_title':'Null',
                'ord_in_thread':ord_in_thread,
                'post_url':'Null',
                'post_text':utils.clean_text(post_text).replace('\n',''),
                'thread_title':thread_title,
                'thread_url':thread_url
                }
            json_posts = {
                          'record_id':'Null',
                          'hostname':'bbs.pediy.com',
                          'domain': 'bbs.pediy.com',
                          'sub_type':'openweb',
                          'type':"forum",
                          'author':json.dumps(author_data),
                          'title':thread_title,
                          'text':utils.clean_text(post_text).replace('\n',''),
                          'url':'Null',
                          'original_url':'Null',
                          'fetch_time':fetch_epoch,
                          'publish_time':publish_epoch,
                          'link.url':all_links,
                          'post':post
            }
            import pdb;pdb.set_trace()
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(domain+post_id).hexdigest(), body=json_posts, request_timeout=30)
            a_meta = {'publish_epoch':publish_epoch}
            if auth_url:
                json_author.update({
                            'post_id':post_id,
                            'auth_meta': json.dumps(a_meta),
                            'links': author_url,
                            'crawl_status': 0
                            })
                self.cursor.execute(AUTHOR_CRAWL_QUERY,json_author)
            if nodes or thread_title:
                UP_QUE_TO_1 = 'update bbs_crawl set crawl_status = 1 where post_url = "%s"'%response.url
                self.cursor.execute(UP_QUE_TO_1)'''

