import scrapy
from scrapy.spider import Spider
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
        conn = MySQLdb.connect(db="posts_hackthissite",host="localhost",user="root",passwd="" , use_unicode = True , charset = 'utf8')
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
        if '&start=' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'

        if '&start=' in response.url:
            reference_url = ''.join(re.sub('&sid=(.*)&start','&start',response.url))
        else:
            reference_url = ''.join(re.sub('&sid=(.*)','',response.url))

        if '&start=' in reference_url:
            test = re.findall('&start=\d+',reference_url)
            thread_url = reference_url.replace(''.join(test),"")
        else:
            thread_url = reference_url

        thread_title = ''.join(sel.xpath(xpaths.THREADTITLE).extract())
        category = ','.join(sel.xpath(xpaths.CATEGORY).extract()).split(',')[1]
        try:
            sub_category = '["' + ','.join(sel.xpath(xpaths.SUBCATEGORY).extract()).split(',')[2] + '"]'
        except:pass
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

        for node in nodes:
            post_title = ''.join(node.xpath(xpaths.POSTTITLE).extract())
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            authorurl = ''.join(node.xpath(xpaths.AUTHOR_URL).extract()).replace('./','')
            if 'http:' not  in authorurl:
                authorurls = response.urljoin(authorurl)
                author_url = re.sub('&sid=(.*)','',authorurls)
            else:
                author_url = ''
            post_id = ''.join(node.xpath(xpaths.POSTID).extract()).replace('#p','')
            posturl = ''.join(node.xpath(xpaths.POSTURL).extract())
            if '#p' in posturl:
                post_url = reference_url + posturl
            publishtimes = node.xpath(xpaths.PUBLISHTIMES).extract()
            publishtime = re.findall('\w+ \w+ \d+, \d+ \d+:\d+ \wm',publishtimes[0])
            publish_time = ''.join(publishtime).strip()
            publish_epoch = utils.time_to_epoch(publish_time, '%a %b %d, %Y %H:%M %p')
            fetch_epoch = utils.fetch_time()
            post_text = '\n'.join(node.xpath(xpaths.POST_TEXT).extract())
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
		
	
            all_links = str(link).replace('https://www.hackthissite.org/forums/#','').replace('./','/').replace('forums//','forums/')
            query_posts = utils.generate_upsert_query_posts('posts_hackthissite')
	    json_posts = {'domain': "www.hackthissite.org",
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
                          'text': utils.clean_text(post_text),
                          'links': all_links,
            }
	    query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
	        self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
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
