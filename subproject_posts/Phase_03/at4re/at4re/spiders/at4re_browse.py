'''
  at4re_browse
'''
import sys 
import re
from scrapy.selector import Selector
from scrapy.spiders import BaseSpider
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
reload(sys)
#sys.setdefaultencoding('UTF8')
import MySQLdb
from at4re_xpaths import *
import utils

class formus(BaseSpider):
    name = 'at4re_browse'
    allowed_domain = ["https://www.at4re.net"]
    start_urls = ["https://www.at4re.net/f"]
    handle_httpstatus_list = [403, 404]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="", db="at4re", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def add_http(self, url):
        ''' 
          Adding http link
        '''
        if 'http' not in url:
            url = 'https://www.at4re.net/f/%s'%url
        return url


    def parse(self, response):
        '''
          Main page urls
        '''
        sel = Selector(response)
        main_page_urls = sel.xpath(MAIN_URLS).extract()
        for main_urls in main_page_urls:
            main_urls = self.add_http(main_urls)
            #main_urls = 'https://www.at4re.net/f/forum-464.html'
            #main_urls = 'https://www.at4re.net/f/thread-316.html'
            #main_urls = 'https://www.at4re.net/f/thread-445.html'
            yield Request(main_urls, callback=self.parse_nextpage)

    def parse_nextpage(self, response):
        '''
          Navigation for next_pages links for thread_posts
        '''
        sel = Selector(response)
        #import pdb;pdb.set_trace()
        start_page_urls = sel.xpath(START_URLS).extract()
        for first_page_urls in start_page_urls:
            #first_page_urls = self.add_http(first_page_urls)
	    first_page_urls = 'https://www.at4re.net/f' + first_page_urls
            yield Request(first_page_urls, callback=self.parse_nextpage)
        thread_links = sel.xpath(THREAD_URLS).extract()
        for thread_urls in thread_links:
            #import pdb;pdb.set_trace()
            #thread_urls = self.add_http(thread_urls)
	    thread_urls = 'https://www.at4re.net/f/' + thread_urls
	    sk = ''.join(thread_urls).split('#')[-1]
            query_status = utils.generate_upsert_query_posts_crawl('at4re')
            json_posts = {'sk':sk,
                          'post_url':thread_urls,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
            #yield Request(thread_urls, callback=self.parse_all_pages_links)
        navigation_click = sel.xpath(NAVIGATION).extract()
        for click in navigation_click:
            if click:
                #if 'http' not in click: click = self.add_http(click)
		if 'http' not in click: click = 'https://www.at4re.net/f/' + click
                yield Request(click, callback=self.parse_nextpage)





