from scrapy.http import Request
import scrapy
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
QUE = utils.generate_upsert_query_posts_crawl('raidforums')


class Raidforums(scrapy.Spider):
    name = 'raidforums_threads'
    start_urls = ['https://raidforums.com/']

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", host="localhost", user="root", passwd="qwe123", use_unicode=True, charset="utf8")
        self.cursor=self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        nodes = response.xpath('//div[@id="tab11"]//a[@class="forums__forum-name"]/@href').extract() 
        for link in nodes:
            if 'https:/' not in link:link = 'https://raidforums.com/'+link
            yield Request(link,callback = self.parse_forum)

    def parse_forum(self,response):
        n_page = response.xpath('//a[@class="pagination_next"]/@href').extract_first()
        if n_page:
            if 'https:/' not in n_page[0]:pg = 'https://raidforums.com/' + n_page
            yield Request(pg, callback = self.parse_forum)
        #threads_urls = response.xpath('//td[contains(@class,"thread-name")]/a/@href | //a[contains(@class,"thread-name")]/@href').extract()
	threads_urls = response.xpath('//span[@class=" subject_new"]/a/@href').extract()
        for url_ in threads_urls:
            if 'https:/' not in url_:url = 'https://raidforums.com/'+ url_
	    else:url = url_
	    val = {
                'sk': url_,
                'post_url': url,
                'crawl_status': 0,
                'reference_url': response.url,
        }
	    self.cursor.execute(QUE,val)
	    self.conn.commit()	    
