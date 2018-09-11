import scrapy
import MySQLdb

from scrapy.selector import Selector
from scrapy.spider import BaseSpider
from scrapy.http import FormRequest, Request
from scrapy.http import HtmlResponse

class Threats_kaspersky(BaseSpider):
    name = 'threats_kaspersky'
    start_domain = ['https://threats.kaspersky.com']
    start_urls = ['https://threats.kaspersky.com/en/threat/']
    data = dict([('action', 'infinite_scroll'),('page_no', '2'),('post_type', 'threat'),('template', 'row_threat4archive'),('', ''),('q', ''),])
    i = 2
    url = 'https://threats.kaspersky.com/en/wp-admin/admin-ajax.php'


    def __init__(self,*args, **kwargs):
        self.conn   = MySQLdb.connect(host="localhost", user="root", passwd="", db="crawling_sources", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.insert_query = "insert into threats_kaspersky(data_links,page_numbers,reference_url,crawl_type,created_at,modified_at) values(%s,%s, %s,%s,now(),now()) on duplicate key update modified_at = now()"
        self.crawl_type = kwargs.get('crawl_type')

        
	

    def parse(self,response):
        sel = Selector(response)

        all_datalinks =  sel.xpath('//tr//td[@class="cell_one column_one"]//a//@href').extract()
        for data_links_1 in all_datalinks:
            if self.crawl_type == 'keepup':
                print data_links_1
                listing_data = (data_links_1,self.i-1,response.url,'keepup')
                self.cursor.execute(self.insert_query,listing_data)
                self.conn.commit()
        if (self.crawl_type == 'catchup'):
            yield FormRequest(self.url,self.parse_nextpages, formdata=self.data)

    def parse_nextpages(self,response):
        sel = Selector(response)
        all_pages_links = sel.xpath('//tr//td//a//@href').extract()
        print len(all_pages_links)
        for pages_datalinks in all_pages_links:
            listing_data = (pages_datalinks,self.i,self.start_urls[0],'catchup')
            self.cursor.execute(self.insert_query, listing_data)

        if response.status == 200 and self.i < 50:
            self.i = self.i+1
            self.data['page_no'] = str(self.i)
            print self.data
            yield FormRequest(self.url,self.parse_nextpages,formdata=self.data)
            self.conn.commit()

		

		


			
