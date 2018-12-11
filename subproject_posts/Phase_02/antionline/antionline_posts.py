'''
Antionline_posts
'''
import datetime
import time
import sys
import json
import re
import scrapy
from scrapy.selector import Selector
from scrapy.spiders import BaseSpider
from scrapy.http import Request
from scrapy.http import HtmlResponse
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
reload(sys)
#sys.setdefaultencoding('UTF8')
import MySQLdb
import unicodedata
from antionline_xpaths import *
import utils
import antionline_csv
query_posts = utils.generate_upsert_query_posts('antionline')
auth_que = utils.generate_upsert_query_crawl('antionline')

class formus(BaseSpider):
    name = 'antionline_posts'
    #allowed_domain = ["http://www.antionline.com/"]
    start_urls = ["http://www.antionline.com/forum.php"]
    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
        '''
          Mysql connections 
        '''
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="hdrn59!", db="antionline", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def add_http(self, url):
        '''
          Adding http
        '''
        if 'http' not in url:
            url = 'http://www.antionline.com/%s'%url
        return url


    def parse(self, response):
        sel = Selector(response)
        start_page_urls = sel.xpath(MAIN_URLS).extract()
        for main_urls in start_page_urls:
            main_urls = self.add_http(main_urls)
        yield Request(main_urls, callback=self.parse_nextpage)

    def parse_nextpage(self, response):
        sel = Selector(response)
        '''
         Next page all_ithread_links
        '''
        thread_links = sel.xpath(THREAD_URLS).extract()
        for thread_urls in thread_links:
            thread_urls = self.add_http(thread_urls)
            yield Request(thread_urls, callback=self.parse_all_pages_links)
        navigation_click = sel.xpath(NAVIGATION).extract()
        for click in navigation_click:
             if click:
                 if 'http' not in click: click = self.add_http(click)
                 yield Request(click, callback=self.parse_nextpage)

    def clean_url(self, response):
        '''
         Removes the trailing '/?' or '/' in a given url scheme.
        '''
        cleaned_url = re.sub(r'(\/\?|\/)$', '', response)
        return cleaned_url

    def parse_all_pages_links(self, response):
        '''
         posts_terminal
        '''
        if 'page' in response.url:
            #crawl_type = "catchup"
            test = re.findall(r'page\d+', response.url)
            thread_url = response.url.replace(''.join(test), "")#.replace('&do=findComment','')
            thread_url = self.clean_url(thread_url)
        else:
            #crawl_type = "keepup"
            thread_url = response.url
            thread_url = self.clean_url(thread_url)

        sel = Selector(response)
        domain = 'http://www.antionline.com/forums.php'
        category = sel.xpath('//li[@class="navbit"]//a//text()').extract()[1]
        subcategory = '["' + sel.xpath('//li[@class="navbit"]//a//text()').extract()[2] + '"]'
        thread_title = ''.join(sel.xpath(THREAD_TITLE).extract())
        crawl_type = ''

        nodes = sel.xpath(NODES)
        for node in nodes:
            author_name = ''.join(node.xpath(AUTHOR_NAME).extract())
            author_link = ''.join(node.xpath(AUTHOR_LINK).extract())
            if author_link:
                author_link = self.add_http(author_link)
            post_urls = ''.join(node.xpath(POST_URL).extract())
            post_url = self.add_http(post_urls)
            postid = ''.join(re.findall(r'#post\d+', post_url)).replace('#post', '')

            post_title = ''.join(node.xpath(POST_TITLE).extract()).replace('\n', '').replace('\t', '').replace('\r', '')

            publish_time = ' '.join(node.xpath(PUBLISH_TIME).extract()).encode('ascii', 'ignore')
            publish_time = re.sub(r'\w+ \d+\w+,', ''.join(re.findall(r'\w+ \d+', publish_time)), publish_time)
            publish_time = datetime.datetime.strptime((publish_time), '%B %d %Y, %H:%M %p')
            publish_time = time.mktime(publish_time.timetuple())*1000

            fetch_time = (round(time.time()*1000))

            POST_TEXT = './/blockquote[@class="postcontent restore "]//text()|.//blockquote[@class="postcontent restore "]//img//@title | .//div[@class="bbcode_quote_container"]//@class' #|.//h2[@class="title icon"]//img//@alt ' #| .//div[@class="bbcode_postedby"]//img[not(contains (@alt , "View Post"))]//@alt'

            text = ' '.join(node.xpath(POST_TEXT).extract())
            text = utils.clean_text(text)
            if  'bbcode_quote_container' in text:
                text = text.replace('bbcode_quote_container', 'Quote ')

            Links = node.xpath(LINK).extract()
            Link = []
            for link_ in Links:
                if 'http' not in link_: link_ = 'https:'+ link_
                if not '.gif.' in link_:
                    Link.append(link_)
            links = str(Link)
            if "[]" in links: links = ""

	    json_posts = {}
            json_posts.update({'domain' : domain,
                        'crawl_type' : crawl_type,
                        'category' : category,
                        'sub_category' : subcategory,
                        'thread_title' : thread_title,
                        'post_title'  : post_title,
                        'thread_url' : thread_url,
                        'post_id' : postid,
                        'post_url' : post_url,
                        'publish_epoch' : publish_time,
                        'fetch_epoch' : fetch_time,
                        'author' : author_name,
                        'author_url' : author_link,
                        'post_text' : "{0}".format(utils.clean_text(text)),
                        'all_links' : links,
                        'reference_url' : response.url
            })

            self.cursor.execute(query_posts, json_posts)
	    meta = json.dumps({'time' : publish_time})
            json_author = {}
            json_author.update({
                'post_id' : postid,
                'auth_meta' : meta,
                'links' : author_link,
                })
            self.cursor.execute(auth_que, json_author)


	nav_click = set(sel.xpath('//span//a[@rel="next"]//@href').extract())
        for post_nav_click in nav_click:
            if post_nav_click:
                post_nav_click = self.add_http(post_nav_click)
                yield Request(post_nav_click, callback=self.parse_all_pages_links)








