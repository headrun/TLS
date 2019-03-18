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
        self.conn = MySQLdb.connect(db="posts_raidforums",
                                   host="localhost",
                                   user="root",
                                   passwd = "",
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
        que = "select distinct(post_url) from raidforums_threads_crawl where crawl_status = 0"
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for url in data:
	    yield Request(''.join(url),callback = self.parse_comm,meta = {'crawl_type':'keepup'})

    def parse_comm(self,response):
        domain = "www.raidforums.com"
	crawl_type = response.meta.get('crawl_type')
        Category = ''.join(response.xpath('//span[@class="crumbs"]//span/a/text()').extract()[1]).replace('\n','').replace('\r','')
        Subcategory = '['+''.join(response.xpath('//span[@class="crumbs"]//span/a/text()').extract()[2]).replace('\n','"') +']'
        thread_type = ''.join(response.xpath('//span[@class="crumbs"]//span/a/text()').extract()[-1]).replace('\n','').replace('\r','')
        thread_Topics = ''.join(response.xpath('//span[@class="crust"]//a/text()').extract())
        thread_Topics = thread_Topics.strip().replace('\n\n', '/').strip('.')
        thread_nodes = response.xpath('//td[@id="posts_container"]//div[@class="post classic "]')
        json_posts = {}
        Thread_url = response.url
        thread_text, email_id,commantdate = "","",""
        if '?page=' in Thread_url:
             test = re.findall('\?page=\d+',Thread_url)
             Thread_url = Thread_url.replace(''.join(test),'')
        json_posts.update({'domain': domain,
                            #'crawl_type': crawl_type,
                            'thread_url': Thread_url,
                            'thread_title' : thread_type
        })
	if thread_nodes and crawl_type == 'keepup':
	    status_code_update = 'update raidforums_threads_crawl set crawl_status = 1 where post_url = "%(url)s"'%({'url':response.url})
            self.cursor.execute(status_code_update)
        looppage_url = set(response.xpath('//a[@class="pagination_next"]/@href').extract())
        for url in looppage_url:
	    try:
		'''
		post_url_ = ''.join(thread_nodes[-1].xpath('.//div[@class="post_content"]//div[@class="float_right"]//a/@href').extract())
		if "http" not in post_url_:'https://raidforums.com/' +post_url_
		test_id = hashlib.md5(str(post_url_)).hexdigest()
                query = {'query_string': {'use_dis_max': 'true', 'query': '_id : {0}'.format(test_id)}}
                res = self.es.search(index="forum_posts", body={"query": query})
                in_es = res['hits']['hits']
                if in_es ==[]:
		'''
                if "http" not in url:url  = 'https://raidforums.com/' +url
                yield Request(url,callback = self.parse_comm,meta = {'crawl_type':'catchup'})
	    except:pass
        for i in thread_nodes:
    	    links,links_,link = [],[],[]
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
            commant_date1 = ''.join(i.xpath('.//div[@class="post_head"]//span[@class="post_date"]/span/@title').extract()).split() or ''.join(i.xpath('.//div[@class="post_head"]//span[@class="post_date"]//text()').extract()).split()
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
            commant_date1 = ''.join(re.findall('\d\d-\d\d-\d+,\d\d:\d\d\wM',''.join(commant_date1).replace(' ','')))
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
		commantdate = time.mktime(commant_date2.timetuple())*1000
            commant_author = ''.join(i.xpath('.//div[@class="post_author scaleimages"]//span[@class="owner-hex"]/text() | ./div//span[@class="largetext"]/a/span/text() | .//span[@class="largetext"]//img/@original-title | .//span[@class="sparkles-ani"]//text() | .//span[@class="largetext"]//span[@class="god-hex"]//text()').extract())
            Post_Url_ = ''.join(i.xpath('.//div[@class="post_content"]//div[@class="float_right"]//a/@href').extract())
            if  Post_Url_:
                Post_Url = 'https://raidforums.com/'+Post_Url_
            else:
                Post_Url = ''

            if Post_Url != '-':Post_id = ''.join(re.findall('pid\d+',Post_Url)).replace('pid','')
            else: Post_id = ''
            links_= i.xpath('.//div[@class="post_content"]//div[@class="post_body scaleimages"]//a//@href | .//div[@class="post_content"]//div[@class="post_body scaleimages"]//img[contains(@src, ".jpg")]/@src | .//div[@class="post_content"]//div[contains(@class, "post_body")]//img[@class="mycode_img"]/@src').extract()
            for link in links_:
                if "http" not in link: link = "https://raidforums.com" + link
                links.append(link)
            if links: links = str(links)
            if not links: links= " "
            a_name = ''.join(i.xpath('.//div[@class="post_author scaleimages"]//span[@class="largetext"]//span/text()').extract())or \
            ''.join(i.xpath('.//span[@class="largetext"]//a//span//text()').extract())
            auth_url = ''.join(i.xpath('.//div[@class="author_avatar"]/a/@href').extract())
            if 'https:/' not in auth_url:auth_url = 'https://raidforums.com/' + auth_url
            json_posts.update({
                            'author_url':auth_url,
                            'links':links
                    })
            meta = {'commant_date' : commantdate, 'thread_type' : thread_type.encode('utf8'), 'thread_Topics' : thread_Topics.encode('utf8')}
             
            thread_text = self.clean_spchar_in_text(thread_text)
            json_posts.update({
                               'category': Category,
                               'sub_category': Subcategory,
                               'post_title': '',
                               'post_id': Post_id,
                               'post_url': Post_Url,
                               'publish_time':  commant_date,
                               'author': commant_author,
                               'text': thread_text,
			       'fetch_time':int(datetime.datetime.now().strftime("%s")) * 1000,
                               #'reference_url': response.url
                })
	    query={"query":{"match":{"_id":hashlib.md5(str(Post_Url)).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
                self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(Post_Url)).hexdigest(), body=json_posts)
            #try:self.cursor.execute(d_que, json_posts)
	    #except:pass#import pdb;pdb.set_trace()
            meta = {'publish_time': commant_date, 'thread_title': thread_type}
            json_crawl = {
                           'post_id': Post_id,
                           'auth_meta': json.dumps(meta),
                           'links':auth_url,
                           }

    	    self.cursor.execute(a_que, json_crawl)
	    self.conn.commit()
        
        looppage_url = set(response.xpath('//a[@class="pagination_next"]/@href').extract())
        for url in looppage_url:
            if "http" not in url:url  = 'https://raidforums.com/' +url
	    yield Request(url,callback = self.parse_comm,meta = {'crawl_type':'catchup'})
