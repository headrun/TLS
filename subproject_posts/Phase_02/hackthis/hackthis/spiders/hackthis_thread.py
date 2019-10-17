import scrapy
from scrapy.selector import Selector
import time
from scrapy.http import Request
import datetime
import sys
import json
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import xpaths
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
from pprint import pprint 

class HackThis(scrapy.Spider):
    name = "hackthis_thread"
    start_urls = ["https://www.hackthissite.org/forums/index.php"]

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
	self.conn,self.cursor = self.mysql_conn()
	self.crawl_query = utils.generate_upsert_query_authors_crawl('posts_hackthissite')
	self.query_posts = utils.generate_upsert_query_posts('posts_hackthissite')
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
        conn = MySQLdb.connect(db="posts_hackthissite",host="localhost",user="tls_dev",passwd="hdrn!" , use_unicode = True , charset = 'utf8')
        cursor = conn.cursor()
	return conn,cursor	

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()
    

    def start_requests(self):
        url_que = "select distinct(post_url) from hackthis_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread)

    def parse_thread(self,response):
        sel = Selector(response)
        if '&start=' in response.url:
            reference_url = ''.join(re.sub('&sid=(.*)&start','&start',response.url))
        else:
            reference_url = ''.join(re.sub('&sid=(.*)','',response.url))

        if '&start=' in reference_url:
            test = re.findall('&start=\d+',reference_url)
            thread_url = reference_url.replace(''.join(test),"")
        else:
            thread_url = reference_url

        thread_title = ''.join(sel.xpath(xpaths.THREADTITLE).extract()) or 'Null'
        category = ','.join(sel.xpath(xpaths.CATEGORY).extract()).split(',')[1] or 'Null'
        try:
            sub_category = ','.join(sel.xpath(xpaths.SUBCATEGORY).extract()).split(',')[2] or 'Null'
        except:pass
	sub_category_url = sel.xpath('//li[@class="icon-home"]//a/@href').extract()[1].replace('./', 'https://www.hackthissite.org/forums/') or 'Null'
        nodes = sel.xpath(xpaths.NODES)
        if nodes:
            query = 'update hackthis_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
	    self.conn.commit()
	else:
	    query = 'update hackthis_status set crawl_status = 9 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
            self.conn.commit()
	x = 0
        for node in nodes:
	    x = x+1
            post_title = ''.join(node.xpath(xpaths.POSTTITLE).extract()) or 'Null'
            author = ''.join(node.xpath(xpaths.AUTHOR).extract()) or 'Null'
            authorurl = ''.join(node.xpath(xpaths.AUTHOR_URL).extract()).replace('./','')
            if 'http:' not  in authorurl:
                authorurls = response.urljoin(authorurl)
                author_url = re.sub('&sid=(.*)','',authorurls)
            else:
                author_url = 'Null'
	    if authorurl == '':
		authorurl = 'Null'
		author_url = authorurl
            post_id = ''.join(node.xpath(xpaths.POSTID).extract()).replace('#p','') or 'Null'
            posturl = ''.join(node.xpath(xpaths.POSTURL).extract())
            if '#p' in posturl:
                post_url = reference_url + posturl
	    if posturl == '':
	        posturl = 'Null'
		post_url = posturl
            publishtimes = node.xpath(xpaths.PUBLISHTIMES).extract()
            publishtime = re.findall('\w+ \w+ \d+, \d+ \d+:\d+ \wm',publishtimes[0])
            publish_time = ''.join(publishtime).strip()
            publish_epoch = utils.time_to_epoch(publish_time, '%a %b %d, %Y %H:%M %p')
	    if publish_epoch:
	        month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
	    else:
		import pdb;pdb.set_trace()

            fetch_epoch = utils.fetch_time()
            post_text = '\n'.join(node.xpath(xpaths.POST_TEXT).extract()) or 'Null'
            if 'wrote:' in post_text:
                post_text = post_text.replace('wrote:','wrote: Quote ')
            if 'uncited' in post_text:
                post_text = post_text.replace('uncited','Quote ')
            links = node.xpath(xpaths.LINKS).extract()
            link = []
            for Link in links:
		Link = re.sub('&sid=(.*)','',Link)
                if '#' in Link:
                    alllinks = Link.replace('#',post_url)
		    all_link=re.sub('&sid=(.*)#','#',alllinks)
                    link.append(all_link)
                elif 'http:' not in Link and 'https:' not in Link:
                    Link = response.urljoin(Link)
                    link.append(Link)
                else:
                    link.append(Link)
		
	
            all_links = ', '.join(link).replace('https://www.hackthissite.org/forums/#','').replace('./','/').replace('forums//','forums/')
	    if all_links == '':
		all_links = 'Null'
	    if links == []:
	        links = 'Null'
		all_links = links
	    post = {
		'cache_link':'',
		'section':category,
		'language':'english',
		'require_login':'false',
		'sub_section':sub_category,
		'sub_section_url':sub_category_url,
		'post_id':post_id,
		'post_title':post_title,
		'ord_in_thread':x,
		'post_url':post_url,
		'post_text':utils.clean_text(post_text).replace('\n', ''),
		'thread_title':thread_title,
		'thread_url':thread_url
		}
	    author_data = {
		'name':author,
		'url':author_url
		}
            query_posts = utils.generate_upsert_query_posts('posts_hackthissite')
	    json_posts = {
			  'id':post_url,
			  'hostname':"www.hackthissite.org",
			  'domain': "hackthissite.org",
			  'sub_type':'openweb',
			  'type':'forum',
			  'author':json.dumps(author_data),
			  'title':thread_title,
			  'text':utils.clean_text(post_text).replace('\n', ''),
			  'url':post_url,
			  'original_url':post_url,
			  'fetch_time':fetch_epoch,
			  'publish_time':publish_epoch,
			  'link_url':all_links,
			  'post':post
            }
	    #query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
	    self.es.index(index="forum_posts_" + month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
	    #else:
		#data_doc = res['hits']['hits'][0]
                #if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    #self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)

            if author_url:
                meta = {'publish_epoch': publish_epoch}
                json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
                }

            self.cursor.execute(self.crawl_query, json_crawl)
	    self.conn.commit()
        if not re.findall('start=\d+',response.url):
            try:
                l_page = sel.xpath(xpaths.PAGE_NAVIGATION).extract()[-1]
                page_num = int(''.join(re.findall('start=\d+',l_page)).replace('start=',''))
                for i in range(10,page_num,10):
                    n_page = response.url + '&start='+str(i)
                    yield Request(n_page,callback = self.parse_thread)
            except:pass
