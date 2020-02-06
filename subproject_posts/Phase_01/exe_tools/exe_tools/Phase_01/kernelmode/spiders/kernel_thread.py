import datetime
import json
import MySQLdb
import re
from MySQLdb import OperationalError 
import time
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import scrapy
from scrapy import signals
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.xlib.pydispatch import dispatcher
import tls_utils as utils
import xpaths
from elasticsearch import Elasticsearch
import hashlib
from urlparse import urljoin
from pprint import pprint
def extract_data(sel, xpath_, delim=''):
    return delim.join(sel.xpath(xpath_).extract()).strip()

def get_nodes(sel, xpath_):
    return sel.xpath(xpath_)

def extract_list_data(sel, xpath_):
    return sel.xpath(xpath_).extract()


class KernelPost(scrapy.Spider):
    name = "kernel_thread"
    start_urls = ["https://www.kernelmode.info/forum/index.php"]
    handle_httpstatus_list=[404]

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
	self.conn, self.cursor = self.mysql_conn()
	dispatcher.connect(self.close_conn, signals.spider_closed)

    '''def mysql_conn(self):
        conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="tls_dev",
                                    passwd="hdrn!",
                                    use_unicode=True,
                                    charset='utf8')
        cursor = conn.cursor()
	return conn, cursor'''
    def __init__(self, *args, **kwargs):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        super(KernelPost,  self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(db="posts",host="localhost",user="tls_dev",passwd="hdrn!" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.cursor.close()
        self.conn.close()

    def add_http(self, url):
	url  = urljoin('http://www.kernelmode.info/forum/',url)
	url = url.replace('/forum/forum/','/forum/')
	return url
	'''
        if 'http' not in url:
            url = url.replace('./', 'http://www.kernelmode.info/forum/')
        else:
            url = url.replace('./', '')
        return url
	'''

    def parse(self, response):
        sid = response.xpath('//input[@type="hidden"] [@name="sid"]/@value').extract()
        data = {'username': 'kerspdr',
                'password': 'Inq2018.',
                'redirect': './ucp.php?mode=login',
                'sid': sid,
                'redirect': 'index.php',
                'login': 'Login'}
        yield FormRequest('https://www.kernelmode.info/forum/ucp.php?mode=login', callback=self.parse_login, formdata=data)

    def parse_login(self, response):
        sel = Selector(response)
        urls = extract_list_data(sel, xpaths.URLS)
        for url in urls:
            # getting 6forums urls
            if url not in ['./viewforum.php?f=8', './viewforum.php?f=10']:
                url = self.add_http(url)
        	yield Request(url, callback=self.parse_nxt)

    def parse_nxt(self, response):
        sel = Selector(response)
        links = extract_list_data(sel, xpaths.LINKS)
        for link in links:
            link = self.add_http(link)
            yield Request(link, callback=self.parse_thread)
        # page navigation
        for i in set(extract_list_data(sel, xpaths.NEXT_PAGE)):
            if i:
                if 'http' not in i: 
		    i = self.add_http(i)
                yield Request(i, callback=self.parse_nxt)

    def parse_thread(self, response):
        json_posts = {}
        domain = "kernelmode.info"
        thread_url = response.url
        # if '&start=' in response.url:
        if '&start=' in thread_url:
            test = re.findall('&start=\d+', thread_url)
            thread_url = thread_url.replace(''.join(test), '')
        else:
            pass
        # Identifying the crawl_type of the post based on thread_url/response.url
        crawl_type = None
        if '&start=' not in response.url:
            crawl_type = 'keep_up'
        else:
            crawl_type = 'catch_up'

        sel = Selector(response)
        links = []
        thread_title = ''.join(response.xpath('//div[@class="side-segment"]//a[contains(@href,"./viewtopic.php")]/text()').extract()) or 'Null'
        author_links = extract_list_data(sel, xpaths.AUTHOR_LINKS)
        post_nodes = response.xpath('//div[@id="page-body"]//div[@class="clearfix"]//article')
        json_posts.update({'domain': domain,
                           #'crawl_type': crawl_type,
                           'thread_url': thread_url,
                           'thread_title': thread_title,
                           'hostname': 'www.kernelmode.info',
                           'sub_type':'openweb',
                           'type':'forum'
                       })
        ''''next_page = ''.join(response.xpath('//div[@class="row"]//div[@class="pull-right"]//li/a[@rel="next"]/@href').extract())
        if next_page:
	     try:
          	 last_post_id = ''.join(post_nodes[-1].xpath('.//div[@class="pull-left timepost"]//a[contains(@href,"./viewtopic.php?p=")]/text()').extract()) 
		 post_url_ = response.url+last_post_id
		 test_id = hashlib.md5(str(post_url_)).hexdigest()
		 query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		 res = self.es.search(index="forum_posts", body={"query": query})
		 if res['hits']['hits']==[]:
		     next_page = urljoin('https://www.kernelmode.info/forum/',next_page)
                     yield Request(self.add_http(next_page), callback=self.parse_thread)
	     except:pass '''
        category = response.xpath('//div[@class="crumbs"]//li[@class="active"]//a/text()').extract()[0] or 'Null'
	sub_category = ''.join(response.xpath('//div[@class="crumbs"]//li[@class="active"]//a/text()').extract()[1:]) or 'Null'
        sub_category_url = "Null"
        count = 0
        for node in post_nodes:
            #count = 0
            count +=1
            post_title = extract_data(node, xpaths.POST_TITLE) or 'Null'
            ord_in_thread = count
            post_id = ''.join(node.xpath('.//div[@class="pull-left timepost"]//a[contains(@href,"./viewtopic.php?p=")]/text()').extract())
            post_url = '%s%s' %(response.url, post_id)
            publish = ''.join(node.xpath('.//div[@class="pull-left timepost"]/text()').extract()).replace(u'\xa0','').replace('by','').strip()
	    try:
                publish_datetime = datetime.datetime.strptime(publish, '%a %b %d, %Y %I:%M %p')
		publish_time = int(time.mktime(publish_datetime.timetuple())) * 1000
	    except:pass
            if publish_time:
                   month_year = time.strftime("%Y_%m", time.localtime(int(publish_time/1000)))
            else:
                import pdb;pdb.set_trace()
            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
            author_name = ''.join(node.xpath('.//div[@class="pull-left timepost"]//a[contains(@href,"./memberlist.php?mode=viewprofile&u")]/text()').extract())
            author_link = ''.join(node.xpath('.//div[@class="pull-left timepost"]//a[contains(@href,"./memberlist.php?mode=viewprofile&u")]/@href').extract()).replace('./','http://www.kernelmode.info/forum/')
            post_text_ = node.xpath('.//div[@class="content"]//text() | .//div[@class="content"]//img[@class="postimage"]//@alt |.//dl[@class="attachbox"]//img[@class="postimage"]//@alt | .//img[@class="smilies"]//@alt | .//div[@class="content"]//blockquote//cite | .//div[@class="content"]/following-sibling::div[@class="clearfix"]//text()').extract()
            post_text = []
            for i in post_text_:
                if 'download/file.php' in i:
                    i = self.add_http(i)
                post_text.append(i)
            post_text = ' '.join(post_text)
            post_text = re.sub('(<cite>.*?</cite>)', 'Quote', post_text)
            post_text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', post_text)
            post_text = re.sub(r'(\n\s*)', '\n', post_text)
            post_text = re.sub('\s\s+', ' ', post_text)
            post_text = re.sub('(<blockquote[@class="uncited"]>.*?</blockquote>)', 'Quote', post_text)
            arrow = ', '.join(node.xpath('.//blockquote/div/cite/a[2]/@href').extract())
            if arrow:
                arrow = self.add_http(arrow)
            else:
                arrow = 'Null'
            post = {
               'cache_link': '',
               'section':category,
               'language': "english",
               'require_login':"True",
               'sub_section':sub_category,
               'sub_section_url':sub_category_url,
               'post_id':post_id,
               'post_title':post_title,
               'ord_in_thread':ord_in_thread,
               'post_url':post_url,
               'post_text':post_text,
               'thread_title':thread_title,
               'thread_url':thread_url
               } 
            author_data= {
                   'name':author_name,
                   'url':author_link
                   }
            json_posts.update ({
                       'id':post_url,
                       'author':json.dumps(author_data),
                       'title':thread_title,
                       'url':post_url,
                       'original_url':post_url,
                       'fetch_time':fetch_time,
                       'publish_time' : publish_time,
                       'post':post
               })
            Link = []
            links = node.xpath('.//div[@class="content"]//a[@class="postlink"]/@href | .//div[@class="clearfix"]//a[@class="postlink"]//@href | .//dl[@class="attachbox"]//a[@class="postlink"]/@href | .//div[@class="content"]//img[@class="postimage"]//@src | .//dl[@class="attachbox"]//img[@class="postimage"]//@src').extract()
            for link in links:
                link = self.add_http(link)
                Link.append(link)
            Links = Link
            #author = ''.join(node.xpath('.//div[@class="pull-left timepost"]//a[contains(@href,"./memberlist.php?mode=viewprofile&u")]/text()').extract()) or 'Null'
            #author_link = ''.join(node.xpath('.//div[@class="pull-left timepost"]//a[contains(@href,"./memberlist.php?mode=viewprofile&u")]/@href').extract()).replace('./','http://www.kernelmode.info/forum/')
            a_link = extract_data(node, xpaths.AUTHOR).replace('./', 'http://www.kernelmode.info/forum/')
            json_posts.update({
                #'author':author,
                #'author_url': author_link,
                'link_url': get_aggregated_links(Links)
            })
            #pprint(json_posts)
	    #query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
	    self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts,request_timeout=30)
	    #else:
		#data_doc = res['hits']['hits'][0]
		#if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    #self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
	    if author_link:
                meta = {'publish_time': publish_time, 'thread_title': thread_title}
                json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'links': author_link
                }
                crawl_query = utils.generate_upsert_query_authors_crawl('kernel_mode')
                if 'wtopic.php?f=10&t' not in response.url:
                    try:
			self.cursor.execute(crawl_query, json_crawl)
		    except OperationalError as e:
			if 'MySQL server has gone away' in str(e):
			    self.conn,self.cursor = self.mysql_conn()
			    self.cursor.execute(crawl_query, json_crawl)
                	else:
                      	    raise e()
        next_page = ''.join(response.xpath('//div[@class="row"]//div[@class="pull-right"]//li/a[@rel="next"]/@href').extract())
        if next_page:
            next_page = urljoin('https://www.kernelmode.info/forum/',next_page)
            yield Request(self.add_http(next_page), callback=self.parse_thread)
     
def get_aggregated_links(links):
    if not links:
        aggregated_links = ''
    else:
        aggregated_links = ', '.join(set(links))

    return aggregated_links
