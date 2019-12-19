import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
import datetime
import time
import json
import re
import MySQLdb
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import xpaths
from elasticsearch import Elasticsearch
import hashlib
crawl_query = utils.generate_upsert_query_authors_crawl('posts_blackhat')
query_posts = utils.generate_upsert_query_posts('posts_blackhat')

class BlackHat(scrapy.Spider):
    name = "blackhat_thread"
    handle_httpstatus_list = [403]
    start_urls = ["https://www.blackhatworld.com/forums/"]

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn,self.cursor = self.mysql_conn()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
	conn = MySQLdb.connect(db="posts_blackhat",host="localhost",user="tls_dev",passwd="hdrn!" , use_unicode = True , charset = 'utf8')
	cursor = conn.cursor()
	return conn,cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
	len_que = 'select count(*) from blackhat_status'
        self.cursor.execute(len_que)
	self.conn.commit()
        total_links = self.cursor.fetchall()
        total_links = int(total_links[0][0])
        for i in range(1,total_links/500+2):
            que = 'select distinct(post_url) from blackhat_status where crawl_status = 0 limit {0},{1}'.format((i-1)*500,500)
            self.cursor.execute(que)
            data = self.cursor.fetchall()
            for url in data:
                yield Request(url[0], callback = self.parse_thread)

    def parse_thread(self, response):
        sel = Selector(response)
        reference_url = response.url
        domain = "www.blackhatworld.com"
        if '/page-' not in reference_url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'
        if '/page-' in reference_url:
            test = re.findall('/page-\d+',reference_url)
            thread_url = reference_url.replace(''.join(test),"")
        else:
            thread_url = reference_url
        thread_title = ''.join(sel.xpath(xpaths.THREADTITLE).extract())
        try:
	    category = ''.join(sel.xpath(xpaths.CATEGORY).extract()[1])
	except: pass
        try:
	    sub_category = ''.join(sel.xpath(xpaths.SUBCATEGORY).extract()[2])
	except: pass
        post_title = ' '
        nodes=sel.xpath(xpaths.NODES)
        if nodes:
            query = 'update blackhat_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
	if response.status not in (200,301):
	    query = 'update blackhat_status set crawl_status = 9 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)

        page_nav = ''.join(set(sel.xpath(xpaths.PAGENAV).extract()))
        if page_nav:
            try:
                text_case = ''.join(nodes[-1].xpath(xpaths.POSTID).extract()).replace('post_url_','')
                test_id = hashlib.md5(str(text_case)).hexdigest()
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		res = es.search(index="forum_posts", body={"query": query})
		if res['hits']['hits']==[]:    
	            yield Request(page_nav,callback = self.parse_thread)
            except:pass

        for node in nodes:
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            authorurl =  ''.join(node.xpath(xpaths.AUTHORURL).extract())
            if authorurl == '':
                author_url = authorurl
            else:
                author_url = "https://www.blackhatworld.com/"  + authorurl
            post_url = "https://www.blackhatworld.com/" + ''.join(node.xpath(xpaths.POSTURL).extract())
            postid = ''.join(node.xpath(xpaths.POSTID).extract())
            post_id =  re.findall('\d+',postid)
            publishtimes = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
            publish_epoch = utils.time_to_epoch(publishtimes, '%b %d, %Y')
            post_texts = '\n'.join(node.xpath(xpaths.POST_TEXT).extract())
            post_text = utils.clean_text(post_texts.replace(u'[email\xa0protected]', ''))
            mails = node.xpath('//a[@class="__cf_email__"]/@data-cfemail').extract()
            for mail in mails:
                email = utils.decode_cloudflareEmail(mail)
                post_text = post_texts.replace(mail,email)
            if 'quoteContainer' in post_text:
                post_text = post_text.replace('quoteContainer' ,'Quote ')
            fetch_epoch = utils.fetch_time()
            author_signature = '\n'.join(node.xpath(xpaths.AUTHOR_SIGNATURE).extract())
            Links = node.xpath(xpaths.LINKS).extract()
            links = []
            for Link in Links:
                if 'http:' not in Link and 'https:' not in Link:
                    al_links = "https://www.blackhatworld.com/" + Link
                    links.append(al_links)
                else:
                    links.append(Link)

            all_links = links
            json_posts = {'domain': domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': ''.join(post_id),
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'fetch_time': fetch_epoch,
                          'author': author,
                          'author_url': author_url,
                          'text': utils.clean_text(post_text),
                          'links':', '.join(all_links)
            }
            json_posts.update({
                'author_url': author_url,
                'links': ', '.join(all_links)
            })
	    query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            res = self.es.search(body=query)
	    if res['hits']['hits'] == []:
          	self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
	    else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)

            meta = {'publish_epoch': publish_epoch, 'author_signature':utils.clean_text(author_signature)}
            json_crawl = {}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
	    self.conn.commit()
        pagenav = set(sel.xpath(xpaths.PAGENAV).extract())
        for page in pagenav:
            if page:
                page = "https://www.blackhatworld.com/" + page
                yield Request(page, callback = self.parse_thread)




