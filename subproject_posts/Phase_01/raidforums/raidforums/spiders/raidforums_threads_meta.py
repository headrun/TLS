from scrapy.http import Request
import time
import datetime
import scrapy
import re
import MySQLdb
import datetime
import json
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
import hashlib
#import utils
#d_que = utils.generate_upsert_query_posts('raidforums')
a_que = utils.generate_upsert_query_authors_crawl('raidforums')


class Raidforums(scrapy.Spider):
    name = 'raidforums_threads_meta'

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn = MySQLdb.connect(db="posts",
                                   host="localhost",
                                   user="root",
                                   passwd = "qwe123",
                                   use_unicode=True,
                                   charset="utf8")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def clean_spchar_in_text(self, thread_text):
        thread_text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', thread_text)
        thread_text = re.sub(r'(\n\s*)', '\n', thread_text)
        return thread_text 

    def start_requests(self):
        que = "select distinct(post_url) from raidforums_threads_crawl where crawl_status = 0 or 1"
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for url in data:
	    yield Request(url[0], callback = self.parse_comm,meta = {'crawl_type':'keepup'})

    def parse_comm(self,response):
        domain = "raidforums.com"
	crawl_type = response.meta.get('crawl_type')
        Category = ''.join(response.xpath('//nav[@id="breadcrumb"]//ul[@class="breadcrumb__main nav talign-mleft"]//li[@class="breadcrumb__bit"]//a/text()').extract()[1]).replace('\n','').replace('\r','') or 'Null'
        Subcategory = ''.join(response.xpath('//nav[@id="breadcrumb"]//ul[@class="breadcrumb__main nav talign-mleft"]//li[@class="breadcrumb__bit"]//a/text()').extract()[2]) or 'Null'
	sub_categoryurl = ''.join(response.xpath('//nav[@id="breadcrumb"]//ul[@class="breadcrumb__main nav talign-mleft"]//li[@class="breadcrumb__bit"]//a/@href').extract()[2])
	if sub_categoryurl:
	    sub_category_url = 'https://raidforums.com/'+ sub_categoryurl
	if sub_categoryurl == '':
	    sub_category_url = 'Null'
        thread_title = ''.join(response.xpath('//div[@class="thread-info__thread-wrap"]//span[@class="thread-info__name rounded"]/text()').extract()) or 'Null'
        thread_Topics = ''.join(response.xpath('//span[@class="crust"]//a/text()').extract())
        thread_Topics = thread_Topics.strip().replace('\n\n', '/').strip('.')
        thread_nodes = response.xpath('//td[@id="posts_container"]//div[@class="post "]')
        json_posts = {}
        Thread_url = response.url
        thread_text, email_id,commantdate = "","",""
        if '?page=' in Thread_url:
             test = re.findall('\?page=\d+',Thread_url)
             Thread_url = Thread_url.replace(''.join(test),'')
	if thread_nodes and crawl_type == 'keepup':
	    status_code_update = 'update raidforums_threads_crawl set crawl_status = 1 where post_url = "%(url)s"'%({'url':response.url})
            self.cursor.execute(status_code_update)
        for i in thread_nodes:
    	    #links,links_,link = [],[],[]
	    ord_in_thread = ''.join(i.xpath('.//div[@class="post_head"]//div[@class="float_right"]//strong/a/text()').extract()).replace('#', '')
            thread_text = ''.join(i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//text() | .//div[@class="post_content"]//div[contains(@class, "post_body")]//img/@alt | .//div[@class="post_content"]//div[contains(@class, "post_body")]//blockquote[@class="mycode_quote"]/@class | .//div[@class="post_content"]//div[contains(@class, "post_body")]//img/@original-title | .//div[@class="post_content"]//div[contains(@class, "post_body")]//a//span[contains(@id, "-tag")]//@id | .//div[@class="post_content"]//div[contains(@class, "post_body")]//@data-cfemail').extract()).strip()
            quote = i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//blockquote[@class="mycode_quote"]/@class').extract()
            if quote:
                thread_text = thread_text.replace(quote[0], " Quote ")
            thread_date = ''.join(i.xpath('.//div[@class="post_content"]//span[@class="post_date"]/span//@title | .//div[@class="post_content"]//span[@class="post_date"]/text()').extract())     
	    mails = i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//@data-cfemail').extract()
	    thread_text = thread_text.replace(u'[email\xa0protected]','')
	    for mail in mails:
	        try:
		    email_id = utils.decode_cloudflareEmail(mail)
		    thread_text = thread_text.replace(mail,email_id)
		except:pass
	    thread_text = thread_text.replace(u'[email\xc2\xa0protected]','')
            commant = ''.join(i.xpath('.//div[@class="post_head"]//span[@class="post_date"]/span/@title').extract()).strip() or ''.join(i.xpath('.//div[@class="post_head"]/span[@class="post_date"]/text()').extract()).strip()
            tag = ''.join(i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//a//span[contains(@id, "-tag")]//@id').extract())
            if "m-tag" in thread_text:
                thread_text = thread_text.replace("m-tag", "[Mod]")
            if "mp-tag" in thread_text:
                thread_text = thread_text.replace("mp-tag", "[Mod+]")
            if "a-tag" in thread_text:
                thread_text = thread_text.replace("a-tag", "[Admin]")
            if 'd-tag'in thread_text or 'top-d' in thread_text:
                thread_text = thread_text.replace("d-tag", 'Top donator').replace("top-d", 'Top donator')
            if 'o-tag' in thread_text:
                thread_text = thread_text.replace("o-tag", 'Owner')
            '''commant_date1 = ''.join(re.findall('\d\d-\d\d-\d+,\d\d:\d\d\wM',''.join(commant).replace(' ','')))
            thread_date = ''.join(re.findall('\d\d-\d\d-\d+,\d\d:\d\d\wM',''.join(thread_date).replace(' ',''))) or commant_date1
            try:
                commant_date2 = datetime.datetime.strptime(thread_date, '%m-%d-%Y,%I:%M%p')
            except:
                try:
                    commant_date2 = datetime.datetime.strptime(thread_date, '%d-%m-%Y,%I:%M%p')
                except:pass
            try:
                #commantdate = time.mktime(commant_date2.timetuple())*1000
                commant_date = int(time.mktime(commant_date2.timetuple())*1000)
            except:
		commantdate = time.mktime(commant_date2.timetuple())*1000'''
	    try:
	    	publish = datetime.datetime.strptime(commant, '%B %d, %Y at %I:%M %p')
	    	commant_date = time.mktime(publish.timetuple())*1000
	    except:
		import pdb;pdb.set_trace()

	    if commant_date:
	        month_year = time.strftime("%m_%Y", time.localtime(int(commant_date/1000)))
	    if commantdate:
	        month_year = time.strftime("%m_%Y", time.localtime(int(commantdate/1000)))

            commant_author = ''.join(i.xpath('.//div[@class="post_author scaleimages"]//span[@class="owner-hex"]/text() | ./div//span[@class="largetext"]/a/span/text() | .//span[@class="largetext"]//img/@original-title | .//span[@class="sparkles-ani"]//text() | .//span[@class="largetext"]//span[@class="god-hex"]//text() | .//div[@class="post__user"]//span/text()').extract()) or 'Null'
            Post_Url_ = ''.join(i.xpath('.//div[@class="post_content"]//div[@class="float_right"]//a/@href').extract())
            if  Post_Url_:
                Post_url = 'https://raidforums.com/'+Post_Url_
            if Post_url == '':
                Post_url = 'Null'
	    if Post_Url_ == '':
	        Post_url = 'Null'
	    post_title = ''.join(i.xpath('.//div[@class="post_head"]//div[@class="float_right"]/strong/a/@title').extract()) or 'Null'
            if Post_url != '-':
		Post_id = ''.join(re.findall('pid\d+',Post_url)).replace('pid','')
            else: 
		Post_id = 'Null'
	    links = []
            links_ = i.xpath('.//div[@class="post_content"]//div[@class="post_body scaleimages"]//a//@href | .//div[@class="post_content"]//div[@class="post_body scaleimages"]//img[contains(@src, ".jpg")]/@src | .//div[@class="post_content"]//div[contains(@class, "post_body")]//img[@class="mycode_img"]/@src').extract()
            for link in links_:
                if "http" not in link: 
		    link = "https://raidforums.com" + link
                    links.append(link)
            if links: 
		links = ', '.join(links)
            if links == '':
		links = 'Null'
	    if links_ == []:
		links = 'Null'
            auth_url = ''.join(i.xpath('.//div[@class="author_avatar"]/a/@href | .//div[@class="post__user"]//a/@href' ).extract())
            if 'https:/' not in auth_url:
		auth_url = 'https://raidforums.com/' + auth_url
	    if auth_url == '':
		auth_url = 'Null'
            meta = {'commant_date' : commantdate, 'thread_type' : thread_title.encode('utf8'), 'thread_Topics' : thread_Topics.encode('utf8')} 
            thread_text = self.clean_spchar_in_text(thread_text)
	    author_data = {
		'name':commant_author,
		'url':auth_url
		}
	    post = {
		'cache_link':'',
		'section':Category,
		'language':'english',
		'require_login':'false',
		'sub_section':Subcategory,
		'sub_section_url':sub_category_url,
		'post_id':Post_id,
		'post_title':post_title,
		'ord_in_thread':int(ord_in_thread),
		'post_url':Post_url,
		'post_text':thread_text.replace('\n', ''),
		'thread_title':thread_title,
		'thread_url':Thread_url
		}
            json_posts.update({
			       'record_id':re.sub(r"\/$", "", Post_url.replace(r"https", "http").replace(r"www.", "")),
			       'hostname':"www.raidforums.com",
			       'domain':domain,
			       'sub_type':'openweb',
			       'type':'forum',
			       'author':json.dumps(author_data),
			       'title':thread_title,
			       'text':thread_text.replace('\n', ''),
			       'url':Post_url,
			       'original_url':Post_url,
			       'fetch_time':int(datetime.datetime.now().strftime("%s")) * 1000,
			       'publish_time':commant_date,
			       'link_url':links,
			       'post':post
			       })
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(Post_url)).hexdigest(), body=json_posts)

            #try:self.cursor.execute(d_que, json_posts)
	    #except:pass#import pdb;pdb.set_trace()
            meta = {'publish_time': commant_date, 'thread_title': thread_title}
            json_crawl = {
                           'post_id': Post_id,
                           'auth_meta': json.dumps(meta),
                           'links':auth_url,
			   'crawl_status':0
                           }

    	    self.cursor.execute(a_que, json_crawl)
	    self.conn.commit()
        looppage_url = ''.join(response.xpath('//section[@id="thread-navigation"]//a[@class="pagination_next"]/@href').extract())
        if looppage_url:
	    next_page  = 'https://raidforums.com/' + looppage_url
	    yield Request(next_page, callback = self.parse_comm,meta = {'crawl_type':'catchup'})
