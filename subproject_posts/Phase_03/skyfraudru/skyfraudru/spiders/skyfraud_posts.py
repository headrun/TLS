import scrapy
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import json
import re
import MySQLdb
import time
import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from datetime import timedelta
from elasticsearch import Elasticsearch
import hashlib

query_posts = utils.generate_upsert_query_posts('posts_skyfraud')
crawl_query = utils.generate_upsert_query_authors_crawl('posts_skyfraud')

class skyfraudSpider(scrapy.Spider):
    name = "skyfraud_posts"
    start_urls = ["https://sky-fraud.ru/"]

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn,self.cursor = self.mysql_conn()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn(self):
        conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="qwe123",
                                    use_unicode=True,
                                    charset="utf8mb4")
        cursor = conn.cursor()
        return conn,cursor

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        url_que = "select distinct(post_url) from skyfraud_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_ann,meta ={'crawl_type':'keep up'})

    def parse_ann(self, response):
	login = ''.join(response.xpath('//a[@class="buttong"]/@href | //div[@style="margin: 10px"]/a/@href').extract())
	if login:
	    return
	crawl_type = response.meta.get('crawl_type')
        sel = Selector(response)
        Domain = "sky-fraud.ru"
        try:
            category = ''.join(response.xpath(xpaths.CATEGORY).extract()[1]) or 'Null'
        except:
            category = 'Null'
        try:
	    sub_category = ''.join(response.xpath(xpaths.SUBCATEGORY).extract()[2]) or 'Null'
	except:
	    sub_category = 'Null'
	sub_categoryurl = response.xpath('//span[@class="navbar"]/a/@href | //span[@style="color: #FF6600;"]/@href').extract()[2]
	if sub_categoryurl:
	    sub_category_url = 'https://sky-fraud.ru/' + sub_categoryurl
	if sub_category_url == '':
	    sub_category_url = 'Null'
	if sub_categoryurl == '':
	    sub_category_url = 'Null'
        thread_title = ''.join(response.xpath('//table[@class="tborder"]//tr[@valign="middle"]//td/text()').extract()).replace('>','').strip() or 'Null'
        nodes = sel.xpath(xpaths.NODES)
        page = sel.xpath(xpaths.PAGENAV).extract_first()
        if page:
	    page = "https://sky-fraud.ru/" + page
            yield Request(page, callback = self.parse_ann,meta = {'crawl_type':'catch_up'})
        if page:
            pno = ''.join(re.findall('&page=\d+',page))
            if crawl_type == 'keep_up':
                page = response.url + pno
            else:
                page = re.sub('&page=\d+',pno,response.url)

        for node in nodes:
            authorurl = ''.join(node.xpath(xpaths.AUTHORURL).extract())
            if 'http'and 'https' not in authorurl:
                author_url = "https://sky-fraud.ru/" + authorurl
	    if author_url == '':
		author_url = 'Null'
	    if authorurl == '':
		author_url = 'Null'
            Posttitle = ' '.join(node.xpath(xpaths.POSTTITLE).extract()) or 'Null'
            Post_url = ''.join( node.xpath(xpaths.POSTURL).extract())
            if Post_url :
                post_url = "https://sky-fraud.ru/" + Post_url
	    if post_url == '':
		post_url = 'Null'
	    if Post_url == '':
		post_url = 'Null'
	    ord_in_thread = ''.join(node.xpath('.//td[@class="thead"]//a[contains(@id,"postcount")]/@name').extract())
            post_id = ''.join(re.findall('\p=\d+',Post_url)).replace('p=','').strip() or 'Null'
	    publish= ''.join(node.xpath(xpaths.PUBLISH).extract()).replace('\n','').replace('\r','').replace('\t','').replace(u'\u0412\u0447\u0435\u0440\u0430',(datetime.datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')).replace(u'\u0421\u0435\u0433\u043e\u0434\u043d\u044f',datetime.datetime.now().strftime('%d.%m.%Y'))
	    publishdate = datetime.datetime.strptime(publish,'%d.%m.%Y, %H:%M')
            PublishTime =time.mktime(publishdate.timetuple())*1000
            if PublishTime:
                year = time.strftime("%Y", time.localtime(int(PublishTime/1000)))
                if year > '2011':
                    month_year = time.strftime("%m_%Y", time.localtime(int(PublishTime/1000)))
                else:
                    continue
            FetchTime = int(datetime.datetime.now().strftime("%s")) * 1000
            Author =  ''.join(node.xpath(xpaths.AUTHOR).extract()) or 'Null'
            text = ' '.join(node.xpath(xpaths.TEXT).extract()).strip().replace(u'\u0426\u0438\u0442\u0430\u0442\u0430:',u'\u0426\u0438\u0442\u0430\u0442\u0430: %s'%'Quote') or 'Null'
            Text = re.sub('\s\s+', ' ', text)
            thread_url = response.url
            Links = node.xpath(xpaths.LINKS_).extract()
	    links = []
            for link in Links:
		if 'http' not in link:
                    link = "https://sky-fraud.ru/" + link
		    links.append(link)
                if not 'smilies' in link:
                    links.append(link)
		else :
                    links.append(link)
            all_links = ', '.join(links)
	    if all_links == '':
		all_links = 'Null'
	    if Links == []:
	        all_links = 'Null'

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
			'name':Author,
			'url':author_url
			}
	    post_data = {
			'cache_link':'',
			'author': json.dumps(author_data),
			'section':category,
			'language':'russian',
			'require_login':'false',
			'sub_section':sub_category,
			'sub_section_url':sub_category_url,
			'post_id':post_id,
			'post_title':Posttitle,
			'ord_in_thread':int(ord_in_thread),
			'post_url':post_url,
			'post_text':utils.clean_text(Text).replace('\n', ''),
			'thread_title':thread_title,
			'thread_url':thread_url
			} 
            json_posts = {
			  'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
			  'hostname':'sky-fraud.ru',
			  'domain': Domain,
			  'sub_type':'openweb',
			  'type':'forum',
			  'author':json.dumps(author_data),
			  'title':thread_title,
			  'text':utils.clean_text(Text).replace('\n', ''),
			  'url':Post_url,
			  'original_url':post_url,
			  'fetch_time':FetchTime,
			  'publish_time':PublishTime,
			  'link.url':all_links,
			  'post':post_data
            		}
	    self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)

	    meta = {'publish_epoch': PublishTime}
            json_crawl = {}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
