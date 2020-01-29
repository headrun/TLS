import scrapy
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
from wilderssecurity_xpaths import *
import unicodedata
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
from pprint import pprint 

def clean_spchar_in_text(self, text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    text = unicodedata.normalize('NFKD', text.decode('utf8'))
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
    text = re.sub(r'(\n\s*)', '\n', text)
    return text



class WilderssecuritySpider(scrapy.Spider):
    name = 'wilderssecurity'
    start_urls = ['http://wilderssecurity.com/']
    handle_httpstatus_list=[403]

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.query = utils.generate_upsert_query_posts('POSTS_WILDER')
        self.conn = MySQLdb.connect(db="POSTS_WILDER",
                                    host="localhost",
                                    user="tls_dev",
                                    passwd = "hdrn!",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url_que = "select distinct(post_url) from wilder_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_meta)


    def parse_meta(self, response):
        thread_url = response.url.split('/page')[0]
        json_posts = {}
        domain = "wilderssecurity.com"
        if "/page-" in response.url:
            crawl_type = "catchup"
        else:
            crawl_type = "keepup"
        category = ''.join(set(response.xpath(CATEGORY).extract())) or 'Null'
        subcategory = ''.join(set(response.xpath(SUBCATEGORY).extract())) or 'Null'
	sub_category_url = response.xpath('//span[3]//a[@class="crumb"]/@href').extract_first() or 'Null'
	if sub_category_url == '':
	    sub_category_url = 'Null'

        thread_title = ''.join(response.xpath(THREAD_TITLE).extract()) or 'Null'
        nodes = response.xpath(NODES)
        up_que = 'update wilder_status set crawl_status = 1 where post_url like %(url)s'
        if nodes:
            val = {'url':response.url}
            self.cursor.execute(up_que,val)
	    self.conn.commit()
        text = ""
        for node in nodes:
	    ord_in_thread = ''.join(node.xpath('.//div[@class="publicControls"]//a/text()').extract()).replace('#', '')
            text = ''.join(node.xpath(TEXT).extract()) or 'Null'
            text = clean_spchar_in_text(self,text)
            asidess = node.xpath('.//aside//div[@class="attribution type"]/text()')
            if asidess:
                asides_temp = [aside.extract().replace('\n','').replace('\t','') for aside in asidess]
                asides = [x for x in asides_temp if x]
                for author in set(asides):
                    text=text.replace(author, ' Quote '+author)
            text = text.replace("Click to expand...", "")
            text = clean_spchar_in_text(self,text)
            author_link = ''.join(node.xpath(AUTHOR_LINK).extract())
            if "http" not in author_link: 
	        author_link = site_domain + author_link
	    if author_link == '':
		author_link = 'Null'
	    if author_link == 'https://www.wilderssecurity.com/':
	        author_link = 'Null'
            author_name = ''.join(node.xpath(AUTHOR).extract()) or 'Null'
            publish_time = ''.join(node.xpath(PUBLISH_TIME).extract()) or ''.join(node.xpath('.//a/abbr[@class="DateTime"]/@data-datestring').extract()) or 'Null'
            try:
                publishtime_ = datetime.datetime.strptime((publish_time), '%b %d, %Y at %I:%M %p')
            except:
                publishtime_ = datetime.datetime.strptime((publish_time), '%b %d, %Y')
            publish_epoch = int(time.mktime(publishtime_.timetuple())*1000)
	    if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
	            month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                else:
                    continue
	    else:
		import pdb;pdb.set_trace()

            fetchtime = (round(time.time()*1000))
            posturl = ''.join(node.xpath(POSTURL).extract())
            if "http" not in posturl: 
		posturl = site_domain+ posturl
	    if posturl == '':
		posturl = 'Null'
            post_id = ''.join(node.xpath('.//a[@title="Permalink"]/@data-href').extract()) or 'Null'
            post_id = ''.join(re.findall('\d+',post_id))
            total_posts = ''.join(node.xpath('.//dl[@class="pairsJustified"]//dt[contains(text(), "Posts:")]//following-sibling::dd/a/text()').extract()) or ''.join(node.xpath('.//dl[@class="pairsJustified"]//dt[contains(text(), "Posts:")]//following-sibling::dd//text()').extract())
            author_data = {
                'name':author_name,
                'url':author_link
                }
            join_date = ''.join(node.xpath(JOINDATE).extract())
            if join_date:
                joindate = datetime.datetime.strptime((join_date), '%b %d, %Y')
                join_date = int(time.mktime(joindate.timetuple())*1000)
            else:
                pass
         
	    post = {
		'cache_link':'',
		'author':json.dumps(author_data),
		'section':category,
		'language':'english',
		'require_login':'false',
		'sub_section':subcategory,
		'sub_section_url':sub_category_url,
		'post_id':post_id,
		'post_title':'Null',
		'ord_in_thread':ord_in_thread,
		'post_url':posturl,
		'post_text':text.replace('\n',''),
		'thread_title':thread_title,
		'thread_url':thread_url
		}
            Link = []
            groups = ''.join(node.xpath(GROUPS).extract())
            if not groups: groups = ""
            links = node.xpath('.//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img[contains(@class, "bbCodeImage")]/@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a[contains(@class, "Link")]/@href | .//ul[@class="attachmentList SquareThumbs"]//div[@class="thumbnail"]//img/@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a[@class="username"]/@href').extract()

            for link in links:
                if "http" not in link: 
		    link = site_domain + link
                    Link.append(link)
		elif link:
		    Link.append(link)

            Links = ', '.join(Link)
	    if Links == '':
	        Links = 'Null'
	    if links == []:
	        links = 'Null'
		Links = links
            json_posts.update({
				'record_id' : re.sub(r"\/$", "", posturl.replace(r"https", "http").replace(r"www.", "")),
				'hostname':'www.wilderssecurity.com',
                            	'domain': domain,
                            	'sub_type':'openweb',
                            	'type':'forum',
				'author':json.dumps(author_data),
				'title':thread_title,
				'text':text.replace('\n',''),
				'url':posturl,
				'original_url':posturl,
				'fetch_time':fetchtime,
				'publish_time':publish_epoch,
				'link.url':Links,
				'post':post
            })
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(posturl)).hexdigest(), body=json_posts, request_timeout=30)

            if author_link == 'Null':
		continue
            meta = {'publish_epoch': publish_epoch, 'thread_title': thread_title, 'join_date':join_date, 'total_posts':total_posts, 'author':author_data, 'groups': groups}
            json_crawl = {
                            "post_id": post_id,
                            "auth_meta": json.dumps(meta),
                            "links": author_link,
                            }
            crawl_query = utils.generate_upsert_query_authors_crawl('POSTS_WILDER')
            self.cursor.execute(crawl_query, json_crawl)
            self.conn.commit()



        nav_urls = ''.join(response.xpath('//link[@rel="next"]/@href').extract())
        if nav_urls:
            if "http" not in nav_urls:  navigation = site_domain + nav_urls
            yield Request(navigation, callback = self.parse_meta)

