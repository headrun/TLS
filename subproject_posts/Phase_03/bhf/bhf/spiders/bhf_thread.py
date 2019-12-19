import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
import  tls_utils as utils
import datetime
import time
import json
import hashlib
from pprint import pprint
from urlparse import urljoin
from elasticsearch import Elasticsearch
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals

class TheGub(scrapy.Spider):
    name="bhf_thread"
    start_urls = ["https://bhf.io/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db= "posts", host = "localhost", user="tls_dev", passwd = "hdrn!", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()
        self.es = Elasticsearch(['10.2.0.90:9342'])
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        que = 'select distinct(post_url) from bhf_status where crawl_status = 0 '
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for url in data:
            #url = ["https://bhf.io/forums/23/page-768"]
	    url = ['https://bhf.io/threads/10015/']
            yield Request(url[0], callback = self.parse_thread)

    def parse_thread(self,response):
        sel = Selector(response)
        category = ''.join(sel.xpath('//li//a//span[@itemprop="name"]/text()').extract()[1]) or 'Null'
	sub_category_url = response.xpath('//a[@itemprop="item"]//@href')[2].extract()
	if sub_category_url:
	    sub_category_url = urljoin('https://bhf.io',sub_category_url)
	else:
	    sub_category_url = 'Null'
        sub_category = ''.join(sel.xpath('//li//a//span[@itemprop="name"]/text()').extract()[2]) or 'Null'
        thread_title = ''.join(sel.xpath('//h1[@class="p-title-value"]/text()').extract()) or 'Null'
        thread_url = response.url 
        nodes = response.xpath('//div[@class="block-body js-replyNewMessageContainer"]//div[@class="message-inner"]')

	'''navigations = ''.join(set(response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract()))
	import pdb;pdb.set_trace()
        if navigations:
            try:
                post_url_ = ''.join(set(nodes[-1].xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//a[@rel="nofollow"]//@href').extract())).strip()
                test_id = hashlib.md5(post_url).hexdigest()
                query = {'query_string': {'use_dis_max': 'true', 'query': '_id : {0}'.format(test_id)}}
                res = self.es.search(index="forum_posts", body={"query": query})
                if res['hits']['hits'] == []:
                    navigations = "https://bhf.io" + navigations
                    yield Request(navigations,callback=self.parse_thread)
            except:pass'''

        if nodes:
            up_que = 'update bhf_status set crawl_status = 1 where post_url = %(url)s'
            val = {'url':  response.request.url}
            self.cursor.execute(up_que,val)
            self.conn.commit()

        for node in nodes:
            #publish = ''.join(node.xpath('.//div[@class="message-attribution-main"]//time[@class="u-dt"]/@title | .//div[@class="message-attribution-main"]//time[@class="u-dt"]/@data-time').extract())
	    publish = ''.join(node.xpath('.//div[@class="message-attribution-main"]//time[@class="u-dt"]/@data-time').extract())
            '''publish = publish.replace(u'\u041e\u043a\u0442','oct').replace(u'\u0418\u044e\u043b','July').replace(u'\u0424\u0435\u0432','Feb').replace(u'\u041c\u0430\u0440','Mar').replace(u'\u0418\u044e\u043d','Jun').replace(u'\u041d\u043e\u044f','Nov').replace(u'\u0410\u0432\u0433','Aug').replace(u'\u0432','').replace(u'\u0414\u0435\u043a','Dec').replace(u'\u042f\u043d' ,'Jan').replace(u'\u041c\u0430\u0439', 'May').replace(u'\u0421\u0435\u043d','Sep').replace(u'\u0410\u043f\u0440','Apr')
	    try:
                publish_time =  datetime.datetime.strptime(publish,'%d %b %Y  %H:%M')
                publish_epoch = time.mktime(publish_time.timetuple())*1000
	 	if publish_epoch:
		    month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            except:
                try:
                    publish_time = datetime.datetime.strptime(publish,'%d %B %Y  %H:%M')
                    publish_epoch = time.mktime(publish_time.timetuple())*1000
		    if publish_epoch:
			month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                except:
		    try:'''
	    if publish:
	        #month_year = time.strftime("%m_%Y", time.localtime(int(publish)))
		month_year = utils.get_index(publish)
		    #except:
			#pass

	    if publish == '':
		publish_time = 'Null'
            author = ''.join(node.xpath('.//h4[@class="message-name"]//a//span//text()').extract()).replace('\n','').replace('#','') or 'Null'
	    author_url = ''.join(node.xpath('.//h4[@class="message-name"]//a//@href').extract()).replace('\n','') 
	    if author_url:
		author_url = urljoin('https://bhf.io',author_url)
	    else:
		author_url = 'Null'
            post_url = ''.join(set(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//a[@rel="nofollow"]//@href').extract())).strip()
            post_url = "https://bhf.io" + post_url
            if post_url =='':
		post_url ='Null'
            post_id = post_url.split("post-")[-1] or 'Null'
	    post_title = '' or 'Null'
	    ord_in_thread =''.join(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//li//a//text()').extract()).replace('\n','').replace('#','')
            post_text =  ''.join(node.xpath('.//article[@class="message-body js-selectToQuote"]//text() | .//div[@class="bbCodeBlock bbCodeBlock--screenLimited bbCodeBlock--code"]//text() | .//div[@class="bbCodeBlock bbCodeBlock--screenLimited bbCodeBlock--code"]/@class | .//blockquote[@class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote"]//text() | ..//blockquote[@class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote"]/@class  | .//img[@class="smilie"]//@alt | .//img[@class="smilie smilie--emoji"]//@alt | .//img[@class="smilie smilie--sprite smilie--sprite655"]//@alt | .//article[@class="message-body js-selectToQuote"]//@data-cfemail ').extract())
            post_text =  post_text.replace('bbCodeBlock bbCodeBlock--screenLimited bbCodeBlock--code','Quote').replace('bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote', 'Quote').replace(u'[email\xa0protected]','').replace('\n','') 
            mails = node.xpath('.//article[@class="message-body js-selectToQuote"]//@data-cfemail').extract()
            for mail in mails:
                asd = mail.replace(u'[email\xa0protected]','')
                email_id = utils.decode_cloudflareEmail(mail)
                post_text= post_text.replace(mail,email_id)
	    if post_text =='':
		post_text = 'Null'
            links = []
            all_link_ = node.xpath('.//img[@class="bbImage"]//@src | .//div[@class="bbCodeBlock-title"]/a/@href ').extract()
            fetch_epoch = utils.fetch_time()
            for all_link in all_link_:
                if "https://bhf.io" not in all_link:
                    all_link = "https://bhf.io" + all_link
                    links.append(all_link)

	        else:
                    links.append(all_link)
            all_links = ', '.join(links)
	    if all_links == '':
		all_links = 'Null'
	    if all_link_ == []:
		all_links = 'Null'

	    author_data= {
                        'name':author,
                        'url':author_url
                        }
            doc = {
			'id' : post_url,
                    	'hostname': 'bhf.io',
                    	'domain': "bhf.io",
                    	'sub_type':'openweb',
                    	'type' : 'forum',
			'author':json.dumps(author_data),
			'title':thread_title,
                    	'text': post_text,
                    	'url': response.url,
                    	'original_url': response.url,
                    	'fetch_time': utils.fetch_time(),
                    	'publish_time': publish,
                    	'link_url': all_links,
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
                    	'post_text':post_text,
                    	'thread_title':thread_title,
                    	'thread_url': thread_url
                        },
	    }
            #query={"query":{"match":{"_id":hashlib.md5(post_url).hexdigest()}}}
            #res_ = self.es.search(body=query)
            #if res_['hits']['hits'] == []:
            self.es.index(index = month_year, doc_type = 'post', id=hashlib.md5(post_url).hexdigest(), body = doc)
        navigations = ''.join(set(response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract()))
        if navigations:
            navigations = "https://bhf.io" + navigations
            yield Request(navigations,callback=self.parse_thread)




