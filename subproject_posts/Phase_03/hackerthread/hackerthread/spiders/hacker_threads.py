import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import re
import datetime
import time
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import json
import sys
from pprint import pprint
import unicodedata
from hacker_threads_xpaths import *
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
crawl_query = utils.generate_upsert_query_authors_crawl('hacker_threads')

def clean_spchar_in_text(self, text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    #text = unicodedata.normalize('NFKD', text.decode('utf8')).encode('utf8')
    text = unicodedata.normalize('NFKD', text.decode('utf8'))
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
    #text = re.sub(r'(\n\s*)', '\n', text).encode('utf-8').encode('ascii', 'ignore')
    text = re.sub(r'(\n\s*)', '\n', text)
    return text

class Hacker_threads(scrapy.Spider):
    name = "hacker_threads"
    start_urls = ["https://www.hackerthreads.org/"]

    def __init__(self, *args, **kwargs):
        super(Hacker_threads, self).__init__(*args, **kwargs)
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.query = utils.generate_upsert_query_posts('hacker_threads')
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="qwe123",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def add_http(self, url):
        if 'http' not in url:
            url = url.replace('./', 'https://www.hackerthreads.org/')
        else:
            url = url.replace('./', '')
        return url

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
	self.cursor.execute("select COUNT(*) from hackerthreads_status ")
	total_links = self.cursor.fetchall()
        #total_links = int(total_links[0][0])
        #for i in range(1,total_links/500+2):
        que = 'select distinct(post_url) from hackerthreads_status where crawl_status = 0' 
        self.cursor.execute(que)
	data = self.cursor.fetchall()
        for url in data:
	    yield Request(url[0], callback = self.parse_meta)

    def parse_meta(self, response):
        sel = Selector(response)
        url =response.url
        url_ = url.split('-')
	json_posts = {}
        thread_url = response.url
        domain = "www.hackerthreads.org"
        category = sel.xpath(CATEGORY).extract()[1] or 'Null'
        subcategory = ''.join(sel.xpath(CATEGORY).extract()[2]) or 'Null'
	sub_category_url = 'https://www.hackerthreads.org'+ ''.join(response.xpath('//a[@itemprop="url"]//@href')[2].extract()).replace('.','') or 'Null'
        thread_title = ''.join(sel.xpath(THREAD_TITLE).extract()) or 'Null'
        '''json_posts.update({'domain': domain,
                            'thread_url': thread_url,
                            'thread_title' : thread_title
        })'''
        nodes = sel.xpath('//div[contains(@class, "post has-profile bg")]')
	if nodes:
            query = 'update hackerthreads_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.request.url}
            self.cursor.execute(query,json_data)
	    self.conn.commit()

	else:
	    query = 'update hackerthreads_status set crawl_status = 9 where post_url = %(url)s'
            json_data={'url':response.request.url}
            self.cursor.execute(query,json_data)
	    self.conn.commit()

	nxt_pg = ''.join(response.xpath('//form//following-sibling::div[@class="pagination"]//li[@class="arrow next"]//a[@rel="next"]/@href').extract())
        next_pg = re.sub('sid=(.*?)&', "", nxt_pg)
        next_pg = self.add_http(next_pg)
	if next_pg:
            '''try:
                text_case = ''.join(nodes[-1].xpath(POST_URL).extract())
                test_id = hashlib.md5(str(text_case)).hexdigest()
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		res = es.search(index="forum_posts", body={"query": query})
		if res['hits']['hits']==[]:'''    
	    yield Request(page_nav,callback = self.parse_meta)
            #except:pass
        order = 0
        for node in nodes:
            publishtime_, publish_epoch, publish_time,author_link,join_date,text ="","","","","",""
            post_title = ''.join(node.xpath(POST_TITLE).extract()) or 'Null'
            text = ''.join(node.xpath(TEXT).extract()) or 'Null'
	    order = order+1
            asidess = node.xpath('.//cite//text()')
            if asidess:
                asides_temp = [aside.extract().replace('\n','') for aside in asidess]
                asides = [x for x in asides_temp if x]
                for author in set(asides):
                    if author:
                        text = text.replace(author, ' Quote ' + author)
            try:
                text = clean_spchar_in_text(self,text)
            except:
                text = text.encode("utf8")
                text = clean_spchar_in_text(self,text)
            author = ''.join(node.xpath(AUTHOR).extract()) or 'Null'
            publish_time = ''.join(node.xpath(PUBLISH_TIME).extract()).replace('\n','').replace('\t','')
            try:
                publish_time_ = datetime.datetime.strptime(publish_time,'%a %b %d, %Y %H:%M %p')
            except:
                pass
            publish_epoch = int(time.mktime(publish_time_.timetuple())*1000)
	    if publish_epoch:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
	    else:
		publish_epoch = 'Null'
            fetchtime = (round(time.time()*1000))
            #post_url = ''.join(node.xpath(POST_URL).extract()) or 'Null'
            authorlink = ''.join(node.xpath(AUTHOR_LINK).extract()).replace('.','') 
            if "http" not in authorlink:
                #author_link = self.add_http(authorlink)
		author_link = 'https://www.hackerthreads.org' +authorlink
	    else:
		author_link = 'Null'
            Link = []
            postid =  ''.join(node.xpath(POST_ID).extract())
            post_id = postid.replace('#p','') or 'Null'
            post_url = ''.join(node.xpath(POST_URL).extract())
            if "http" not in post_url: 
		post_url = response.url + post_url
	    else:
		post_url ='Null'
	    record_id = re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")) or 'Null'
            total_posts= ''.join(node.xpath(TOTAL_POSTS).extract())
            group = ''.join(node.xpath(GROUP).extract())	
            join_date = ''.join(node.xpath(JOIN_DATE).extract())
            try:
                joindate = datetime.datetime.strptime(join_date, ' %a %b %d, %Y %H:%M %p')
                join_date = int(time.mktime(joindate.timetuple())*1000)
            except:
                join_date =  " "
	    links = node.xpath('.//div[@class="content"]//a[@class="postlink"]/@href | .//div[@class="content"]//img[@class="postimage"]/@src | .//div[@class="content"]//following-sibling::div[@class="notice"]/a/@href | .//div[@class="content"]//following-sibling::dl[@class="attachbox"]//a[@class="postlink"]/@href |.//a[@class="postlink"]//@href').extract()
	    
	    for link in links:
		if 'hackerthreads.org' not in link:
			link = self.add_http(link)
                Link.append(link)
            Links = ', '.join(Link)
	    if Links == '':
		Links = 'Null'

	    author_data = {
			   'name':author,
			   'url':author_link
			}

            json_posts.update({

		    'record_id' : record_id,
                    'hostname': 'www.hackerthreads.org',
                    'domain': "hackerthreads.org",
                    'sub_type':'openweb',
                    'type' : 'forum',
                    'author':json.dumps(author_data),
                    'title':thread_title,
                    'text': text,
                    'url': post_url,
                    'original_url': post_url,
                    'fetch_time': fetchtime,
                    'publish_time': publish_epoch,
                    'link_url':Links,
                    'post':{
                        'cache_link':'',
                        'section':category,
                        'language':'english',
                        'require_login':'false',
                        'sub_section':subcategory,
                        'sub_section_url':sub_category_url,
                        'post_id': post_id,
                        'post_title':post_title,
                        'ord_in_thread':order,
                        'post_url': post_url,
                        'post_text':text,
                        'thread_title':thread_title,
                        'thread_url': thread_url
		},

            })

            '''links = node.xpath('.//div[@class="content"]//a[@class="postlink"]/@href | .//div[@class="content"]//img[@class="postimage"]/@src | .//div[@class="content"]//following-sibling::div[@class="notice"]/a/@href | .//div[@class="content"]//following-sibling::dl[@class="attachbox"]//a[@class="postlink"]/@href').extract()

            for link in links:
                if 'hackerthreads.org' not in link:
                    link = self.add_http(link)
                Link.append(link)
            Links = ', '.join(Link)

            json_posts.update({
                                'author_url': author_link,
                                'links': Links
            })'''
            #self.cursor.execute(self.query, json_posts)
            #self.conn.commit()
	    #query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
	    self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts,request_timeout=30)
	    '''else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)'''

            author_signature = ''.join(node.xpath('.//div[@class="signature"]//text() | .//div[@class="signature"]//a[@class="postlink"]/@href').extract())
            if author_link:
                meta = {'publish_epoch': publish_epoch, 'thread_title': thread_title, 'join_date':join_date, 'total_posts':total_posts, 'author':author, 'author_signature':author_signature, 'group':group}
                json_crawl = {
                               "post_id": post_id,
                               "auth_meta": json.dumps(meta),
                                'crawl_status':0,
                               "links": author_link,
                            }
                self.cursor.execute(crawl_query, json_crawl)
                self.conn.commit()

        nxt_pg = ''.join(response.xpath('//form//following-sibling::div[@class="pagination"]//li[@class="arrow next"]//a[@rel="next"]/@href').extract())
        if nxt_pg:
            next_pg = re.sub('sid=(.*?)&', "", nxt_pg)
            if "http" not in next_pg:
                next_pg = self.add_http(next_pg)
                yield Request(next_pg, callback=self.parse_meta)



