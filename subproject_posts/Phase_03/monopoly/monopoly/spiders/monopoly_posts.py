from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
import MySQLdb
import json
from pprint import pprint
import datetime
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import xpaths
from datetime import timedelta
from elasticsearch import Elasticsearch
import hashlib
from urlparse import urljoin
#query_posts = utils.generate_upsert_query_posts('posts_monopoly')
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils


class Monopoly(scrapy.Spider):
    name = "monopoly_posts"

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user="root",passwd="qwe123" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
	self.es = Elasticsearch(['10.2.0.90:9342'])
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Origin': 'https://monopoly.ms',
            'Upgrade-Insecure-Requests': '1',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'https://monopoly.ms/',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        data = {
            'vb_login_username': 'inqspdr',#pass:mi110n@irinq
            'vb_login_password': '',
            's': '',
            'securitytoken': 'guest',
            'do': 'login',
            'vb_login_md5password': 'c823b3822a485d448a68ee415f5eea59',
            'vb_login_md5password_utf': 'c823b3822a485d448a68ee415f5eea59'
        }
        url = 'https://monopoly.ms/login.php?do=login'
        yield FormRequest(url, callback=self.parse_next, formdata=data)

    def parse_next(self, response):
        headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
        cooks = response.headers.getlist('Set-Cookie')
        cookies = {}
        [cookies.update({c.split(";")[0].split("=")[0]:c.split(";")[0].split("=")[1]}) for c in cooks]
        url = 'https://monopoly.ms/'
        yield Request(url, callback=self.parse_next1, headers = headers,cookies = cookies)


    def parse_next1(self,response):
	if "inqspdr" in response.body:
            sel = Selector(response)
            url_que = "select distinct(post_url) from monopoly_status where crawl_status = 0 or 1"
            self.cursor.execute(url_que)
            data = self.cursor.fetchall()
            for url in data:
		#url = 'https://monopoly.ms/showthread.php?t=5869'
	        yield Request(url[0], callback = self.parse_thread)
        

    def parse_thread(self, response):
        sel = Selector(response)
        domain = "monopoly.ms"
        crawl_type = ''
	text1 = ''.join(response.xpath('//div[@style="margin: 10px"]/a/@href').extract())
	if text1:
	    return
        try:
	    category = ''.join(sel.xpath(xpaths.CATEGORY).extract()[1]) or 'Null'
	except:
	     pass
        try:
            sub_category = ''.join(sel.xpath(xpaths.SUB_CATEGORY).extract()[2]) or 'Null'
	except:
	     pass
	sub_category_url = response.xpath('//span[@class="navbar"]//a//@href')[2].extract() 
	if sub_category_url:
		sub_category_url = urljoin("https://monopoly.ms/",sub_category_url)
	else:
		sub_category_url = 'Null'
        thread_title = ''.join(sel.xpath(xpaths.THREAD_TITLE).extract()).replace('\n','').replace('\r','').replace('\t','') or 'Null'
        nodes = sel.xpath(xpaths.NODES)
	'''try:
	    post_url_ = ''.join(nodes[-1].xpath(xpaths.POST_URL).extract())
	    test_id = hashlib.md5(str(post_url_)).hexdigest()
	    query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
	    res = es.search(index="forum_posts", body={"query": query})
	    if res['hits']['hits']==[]:'''
	try:
	    page_nav= sel.xpath(xpaths.PAGENAV).extract_first()
            if "http" not in page_nav:
	        page = urljoin("https://monopoly.ms/",page_nav)
	        yield Request(page, callback = self.parse_thread,meta = {'crawl_type':'catch_up'})
            if page_nav:
                pno = ''.join(re.findall('&page=\d+',page))
	    if crawl_type == 'keep_up':
                page = response.url + pno
            else:
	        page = re.sub('&page=\d+',pno,response.url)
	except:pass
        for node in nodes:
            author = ''.join(node.xpath(xpaths.AUTHOR).extract()) or 'Null'
	    ord_in_thread = ''.join(node.xpath('.//a[@rel="nofollow"]//strong//text()').extract()) or 'Null'
            author_url = ''.join(node.xpath(xpaths.AUTHOR_URL).extract())
            if  'member' in author_url :
                author_url = urljoin("https://monopoly.ms/",author_url)
	    else :
		author_url = 'Null'
            post_url = ''.join(node.xpath(xpaths.POST_URL).extract())
            if 'http'and 'https' not in post_url :
                post_url = urljoin("https://monopoly.ms/",post_url)
	    else:
		post_url = 'Null'
            post_id = ''.join(re.findall('\p=\d+',post_url)).replace('p=','').strip() or 'Null'
            post_title = '' or 'Null'
	    record_id = re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")) or 'Null'
            publish_times = ''.join(node.xpath(xpaths.PUBLISH_TIME).extract()).replace('\r','').replace('\n','').replace('\t','').replace(u'\u0421\u0435\u0433\u043e\u0434\u043d\u044f,',datetime.datetime.now().strftime('%d.%m.%Y,')).replace(u'\u0412\u0447\u0435\u0440\u0430,' ,(datetime.datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y,'))
            try:
	        publish_date = datetime.datetime.strptime(publish_times,'%d.%m.%Y, %H:%M')
	    except:pass
            publish_time =time.mktime(publish_date.timetuple())*1000
	    if publish_time:
                year = time.strftime("%Y", time.localtime(int(publish_time/1000)))
                if year > '2011':
                    month_year = time.strftime("%m_%Y", time.localtime(int(publish_time/1000)))
                else:
                    continue    
	    else:
		publish_time = 'Null'
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            Text = ' '.join(node.xpath(xpaths.TEXT).extract()).replace(u'\u0426\u0438\u0442\u0430\u0442\u0430:',u'\u0426\u0438\u0442\u0430\u0442\u0430: %s'%'Quote').replace('\n','')
            text = re.sub('\s\s+', ' ', Text) or 'Null'
            thread_url = response.url
            Links = node.xpath(xpaths.LINKS).extract()
            links = []
            for Link in Links:
                if 'http:' not in Link and 'https:' not in Link:
                    al_links = "https://monopoly.ms/" + Link
                    links.append(al_links)
                else:
                    links.append(Link)

            all_links = ', '.join(links) or 'Null'
	    
            reference_url = response.url
            if '&page=' not in response.url:
                crawl_type = 'keepup'
            else:
                crawl_type = 'catchup'
            if '&page=' in reference_url:
                test = re.findall('&page=\d+',reference_url)
                thread_url = reference_url.replace(''.join(test),"")
            else:
                thread_url = reference_url

	    author_data = {
			'name':author,
			'url':author_url
			}
            json_posts = {
		    'record_id' : record_id,
                    'hostname': 'monopoly.ms',
                    'domain': "monopoly.ms",
                    'sub_type':'openweb',
                    'type' : 'forum',
                    'author':json.dumps(author_data),
                    'title':thread_title,
                    'text': text,
                    'url':post_url,
                    'original_url': post_url,
                    'fetch_time': fetch_epoch,
                    'publish_time': publish_time,
                    'link.url':all_links,
                    'post':{
                        'cache_link':'',
                        'author':json.dumps(author_data),
                        'section':category,
                        'language':'russian',
                        'require_login':'false',
                        'sub_section':sub_category,
                        'sub_section_url':sub_category_url,
                        'post_id': post_id,
                        'post_title':post_title,
                        'ord_in_thread': ord_in_thread,
                        'post_url': post_url,
                        'post_text':text,
                        'thread_title':thread_title,
                        'thread_url': thread_url
                    },

            }
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
