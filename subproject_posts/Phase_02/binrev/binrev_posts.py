'''
  Binrev_posts
'''
import time
import sys
import json
import re
import unicodedata
from scrapy.selector import Selector
from scrapy.spiders import BaseSpider
from scrapy.http import Request
#from scrapy.http import HtmlResponse
#import calendar
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
reload(sys)
#sys.setdefaultencoding('UTF8')
import MySQLdb
from binrev_xpaths import *
import utils
import binrev_csv
query_posts = utils.generate_upsert_query_posts('binrev')
auth_que = utils.generate_upsert_query_crawl('binrev')

class formus(BaseSpider):
    name = 'binrev_posts'
    allowed_domain = ["http://www.binrev.com/"]
    start_urls = ["http://www.binrev.com/forums/"]
    handle_httpstatus_list = [403]


    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="hdrn59!", db="binrev", charset="utf8", use_unicode=True)
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
            url = 'http://www.binrev.com/%s'%url
        return url 


    def parse(self, response):
        '''
          Main page urls
        '''
        sel = Selector(response)
        start_page_urls = sel.xpath(MAIN_URLS).extract()
	for main_urls in start_page_urls:
	    main_urls = self.add_http(main_urls)
	    yield Request(main_urls, callback=self.parse_nextpage)

    def parse_nextpage(self, response):
        '''
          Navigation for next_pages links for posts
        '''
        sel = Selector(response)
        thread_links = sel.xpath(THREAD_URLS).extract()
        for thread_urls in thread_links:
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
         posts_terminal data which is required
        '''
 	if '&page=' in response.url:
            #crawl_type = "catchup"
            test = re.findall(r'&page=\d+', response.url)
            thread_url = response.url.replace(''.join(test), "").replace('&do=findComment', '')
            thread_url = self.clean_url(thread_url)
        else:
            #crawl_type = "keepup"
            thread_url = response.url
            thread_url = self.clean_url(thread_url)

 	sel = Selector(response)
        #thread_url = response.url
        domain = 'http://www.binrev.com'
        category = sel.xpath('//span[@itemprop="name"]/text()').extract()[1]
        subcategory = '["' + sel.xpath('//span[@itemprop="name"]/text()').extract()[2] + '"]'
        thread_title = ''.join(sel.xpath(THREAD_TITLE).extract())
	crawl_type = ''
        post_title = ''


        nodes = sel.xpath(NODES)
        for node in nodes:
            author_name = ''.join(node.xpath(AUTHOR_NAME).extract())
            author_link = ''.join(node.xpath(AUTHOR_LINK).extract())
            post_url = ''.join(node.xpath(POST_URL).extract())
            postid = post_url.split('=')[-1] ## try in re without indexing

            publish_time = ''.join(node.xpath(PUBLISH_TIME).extract())
            publish_time = datetime.datetime.strptime((publish_time), '%m/%d/%Y %H:%M %p') ## need to check later
            publish_time = time.mktime(publish_time.timetuple())*1000
            fetch_time = (round(time.time()*1000))
            TEXT_NEW = './/div[@data-role="commentContent"]//p//text() | .//div[@data-role="commentContent"]//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//img//@alt | .//div[@class="ipsEmbeddedVideo"]//@src | .//blockquote[@class="ipsQuote"]/p/text() |.//blockquote[@class="ipsQuote"]/@class | .//div[@data-role="commentContent"]//blockquote/@data-ipsquote-username | .//div[@data-role="commentContent"]//blockquote/@data-ipsquote-timestamp'

            text = node.xpath(TEXT_NEW).extract()#.strip().encode('ascii','ignore').decode('utf8')
            text = utils.clean_text(' '.join(text))
            text = text.replace('ipsQuote', 'Quote')
            TEXT_A = './/div[@data-role="commentContent"]//blockquote/@data-ipsquote-username'
            text_a = node.xpath(TEXT_A).extract()
            TEXT_T = './/div[@data-role="commentContent"]//blockquote/@data-ipsquote-timestamp'
            text_t = node.xpath(TEXT_T).extract()

            for ta, tt in zip(text_a, text_t):
                z = ta+' '+tt
                z1 = tt+' '+ta
                try:
                    tz = time.strftime("On %d/%m/%Y at %I:%M %p, ", time.localtime(int(tt)))
                except:
                    pass
                zz = tz+ta+' Said: '
                text = text.replace(z, zz).replace(z1, zz)
            text = text.replace('Quote \nQuote', 'Quote \n')
            if not text_t and text_a:
                for auth in text_a:
                    z = 'Quote '+auth
                    text = text.replace(z, z + ' Said: ')
            ext = ''.join(re.findall(r'\d+ \w+ ago, \w+ said:', text))
            ext1 = ''.join(re.findall(r'On \d+-\d+-\d+ at \d+:\d+ \wM, \w+ said:', text)) 
            ext2 = ''.join(re.findall(r'On \d+/\d+/\d+ at \d+:\d+ \wM, \w+ said:', text))
            #On 7/2/2017 at 3:08 AM, Kulverstukas said:
            ext3 = ''.join(re.findall(r'On \d+/\d+/\d+ at \d+:\d+ \wM,  said:', text))
            #On 7/2/2017 at 3:08 AM,  said:
            ext4 = ''.join(re.findall(r'On \w+ \d+, \d+ at \d+:\d+ \wM, \w+ said:', text))
            #On July 15, 2016 at 10:39 AM, systems_glitch said:

            text = text.replace(ext, '').replace(ext1, '').replace(ext2, '').replace(ext3, '').replace(ext4, '')
            #print text
            Links = node.xpath(LINK).extract()
            Link = []
            for link_ in Links:
                if 'http' not in link_: link_ = 'https:'+ link_
                #if not ('.gif.') or ('emoticons') in link_:
                if not 'emoticons' in link_:
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
	    meta = json.dumps({'time' : publish_time}) #,'threadtitle':thread_title})
	    json_author = {}
	    json_author.update({
		'post_id' : postid,
		'auth_meta' : meta,
		'links' : author_link,
		})
	    self.cursor.execute(auth_que, json_author)

        nav_click = set(sel.xpath('//li[@class="ipsPagination_next"]//a//@href').extract())
        for post_nav_click in nav_click:
            if post_nav_click:
                post_nav_click = self.add_http(post_nav_click)
                yield Request(post_nav_click, callback=self.parse_all_pages_links)

