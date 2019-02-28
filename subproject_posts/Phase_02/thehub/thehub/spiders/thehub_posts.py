#encoding: utf-8
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request, FormRequest
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import utils
from json_val import posts_validate
from urllib import urlencode
query_posts = utils.generate_upsert_query_posts('thehub_tor')
crawl_query = utils.generate_upsert_query_authors_crawl('thehub_tor')

class Thehub(scrapy.Spider):
    name = "thehub_posts"
    start_urls = ["http://thehub7xbw4dc5r2.onion/"]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="thehub_tor",host="localhost",user="root",passwd="1216" , use_unicode = True , charset = 'utf8mb4')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        sel = Selector(response)
        name = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@name').extract())
        value = ''.join(response.xpath('//div[@class="roundframe"]//input[@type="hidden"]/@value').extract())
        params = (
    ('action', 'login2'),
)
        data={
name : value,
'cookielength':'',
'hash_passwrd':'',
'passwrd'       :'!nq!nq7@6',
'user': 'inqspdr',
}
        url = 'http://thehub7xbw4dc5r2.onion/index.php?' +urlencode(params)
        yield FormRequest(url,formdata=data,callback=self.after_login)


    def after_login(self, response):
        url_que = "select distinct(post_url) from thehub_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread)

    def parse_thread(self, response):
	sel = Selector(response)
	domain = "thehub7xbw4dc5r2.onion"
	crawl_type = 'keepup'
	categories = sel.xpath('//div[@id="main_content_section"]//div[@class="navigate_section"]//a//span//text()').extract()
	sub = categories[len(categories)/2:]
	category = sub[1] 
	sub_category = sub[2:-1]
	thread_url = response.url
	thread_title = ''.join(response.xpath('//h3[@class="catbg"]/text()').extract()).replace('\n','').replace('\t','')
	nodes = sel.xpath('//div[@id="forumposts"]//div[contains(@class,"windowbg")]')
	for node in nodes:	
	    post_title= ''.join(node.xpath('.//h5[contains(@id,"subject_")]//text()').extract()).replace('\n','').replace('\t','')
	    post_url =  ''.join(node.xpath('.//h5[contains(@id,"subject_")]//a/@href').extract())
	    post_id = ''.join(re.sub('(.*)#msg','',post_url))
	    text = '\n'.join(node.xpath('.//div[@class="post"]//div[@class="inner"]/text() | .//div[@class="post"]//div[contains(@class,"quote")]//a/text() | .//div[@class="post"]//text()| .//div[@class="post"]//img[@class="smiley"]//@alt | .//div[@class="smalltext modified"]//em/text()| .//blockquote/@class').extract()).strip().replace('bbc_standard_quote','Quote').replace('bbc_alternate_quote','Quote')
	    author = ''.join(node.xpath('.//div[@class="poster"]//h4//text()').extract()).replace('\n','').replace('\t','')
	    author_url = ''.join(node.xpath('.//div[@class="poster"]//h4//@href').extract())
	    publish = ''.join(node.xpath('.//div[@class="smalltext"]//strong//following-sibling::text()').extract())
	    published = re.sub('\xbb','',publish).strip()
	    try:
	        publish_date = datetime.datetime.strptime(published,'%B %d, %Y, %H:%M:%S %p')
	        publish_epoch = time.mktime(publish_date.timetuple())*1000
	    except:
		import pdb;pdb.set_trace()
	    links = node.xpath('.//div[@class="post"]//div[contains(@class,"quote")]//a/@href').extract()
	    all_links = str(links)
	    json_posts = {'domain': domain,
                          'crawl_type': crawl_type,
                          'category': category,
                          'sub_category': str(sub_category),
                          'thread_title': thread_title,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_epoch': publish_epoch,
                          'author': author,
                          'author_url': author_url,
                          'post_text': utils.clean_text(text),
                          'all_links': all_links,
                          'reference_url': response.url
            }
	    schema = posts_validate()
	    result, error = schema.dump(json_posts)
	    if not error :
	        self.cursor.execute(query_posts, json_posts)
	        self.conn.commit()
	    else:
		import pdb;pdb.set_trace()
	    meta = {'publish_epoch': publish_epoch}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
	    self.conn.commit()
	page_nav = sel.xpath('//div[@class="pagelinks floatleft"]//a[@class="navPages"]/@href').extract()
        for page in page_nav:
            yield Request(page, callback=self.parse_thread)
