'''
  at4re_posts
'''
import time
import os
import sys
import json
import re
import datetime
#import timedelta
import unicodedata
from datetime import timedelta
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
from at4re_xpaths import *
import utils
#import at4re_browse
import at4re_csv
query_posts = utils.generate_upsert_query_posts('at4re')
auth_que = utils.generate_upsert_query_authors_crawl('at4re')

class formus(BaseSpider):
    name = 'at4re_posts'
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
            url = 'https://www.at4re.net/%s'%url
        return url

    def start_requests(self):
        url_que = "select distinct(post_url) from at4re_browse where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_all_pages_links)


    def parse_all_pages_links(self, response):
        '''
         posts_terminal data which is required
        '''
        if '-page-' in response.url:
            crawl_type = "catchup"
            thread_url = response.url
            thread_url = ''.join(re.sub('-page-\d+.','.', thread_url))
            thread_url = utils.clean_url(thread_url)
        else:
            crawl_type = "keepup"
            thread_url = response.url
            thread_url = utils.clean_url(thread_url)

        sel = Selector(response)
        #thread_url = response.url
        domain = 'https://www.at4re.net'
	try:sub_category = '["' + sel.xpath('//div[@class="navigation"]//a//text()').extract()[1] + '"]'
        except:pass

	try:category =  sel.xpath('//div[@class="navigation"]//a//text()').extract()[2]
        except:pass

        try:thread_title = sel.xpath('//td[@class="thead"]//div//strong//text()').extract()[1]
        except:pass
        #crawl_type = ''
        #post_title =  ''.join(sel.xpath('//div[@class="navigation"]//span[@class="active"]//text()').extract())
        post_title = ''.join(sel.xpath(POST_TITLE).extract())

        nodes = sel.xpath(NODES)
        if nodes:
            query = 'update at4re_browse set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)


        for node in nodes:
            author = ''.join(set(node.xpath('.//span[contains(@class,"largetext")]//text()').extract()))
            #for author_name in author : author_name = author_name

            author_link = sel.xpath(AUTHOR_LINK).extract()
            for author_url in author_link:
                if author_url:
                    author_url = 'https://www.at4re.net/f/' + author_url
                    author_url = author_url

            post_link  = node.xpath(POST_URL).extract()
            for post_url in post_link:
                if post_url:
                    post_url = 'https://www.at4re.net/f/' + post_url
                    post_url=post_url

            postid = ''.join(re.findall('#pid\d+',post_url)).replace('#pid','')
            publish_time = ''.join(node.xpath(PUBLISH_TIME).extract())#.encode('ascii','ignore').decode('utf8').replace('\n','')
            try:
                publish_time_1 = publish_time.split('(')[0].replace('\n', '').strip()
                publish_time_11 = datetime.datetime.strptime((publish_time_1), '%d-%m-%Y, %H:%M %p')
                #publish_time_1 = datetime.datetime.strptime(re.findall('\d+-\d+-\d+, \d+:\d+ \wM',publish_time))
                publish_time = time.mktime(publish_time_11.timetuple())*1000
            except:
                try:
                    publish_time_2 = datetime.datetime.strptime((publish_time), '%d-%m-%Y, %H:%M %p ')
                    publish_time = time.mktime(publish_time_2.timetuple())*1000
                except:
                    #pass
                    try:
                        publish_time_3 = datetime.datetime.strptime((publish_time), '%d\%m\%Y, %H:%M %p ')
                        publish_time = time.mktime(publish_time_3.timetuple())*1000
                    except:
                        try:
                            publish_time_4 = datetime.datetime.strptime((publish_time), '%d/%m/%Y, %H:%M %p ')
                            publish_time = time.mktime(publish_time_4.timetuple())*1000
                        except: #pass
                            try:
                                if u'\u0623\u0645\u0633' in publish_time:
                                    time_format = ''.join(re.findall('\d+:\d+ \w+',publish_time))
                                    yesterday_date = str(datetime.datetime.now().date() - timedelta(days=1))
                                    publish_time_5 =  yesterday_date + ' ' + time_format
                                    publish_time = time.mktime(publish_time_5.timetuple())*1000
                            except:pass


            fetch_time = utils.fetch_time
            #text = node.xpath('.//div[@class="post_body scaleimages"]//text() | .//div[@class="post_body scaleimages"]//img//@alt |.//div[@class="post_body scaleimages"]//img[not(contains(@src ,"images/smilies"))]//@src | .//blockquote/@class').extract()#.replace('\n','').replace('\t','').replace('\r','')

            #text = node.xpath('.//div[@class="post_content"]//text() | .//div[@class="post_content"]//img//@alt |.//div[@class="post_content"]//img[not(contains(@src ,"images/smilies"))]//@alt | .//blockquote/@class | .//div[@class="codeblock phpcodeblock"]//@class').extract() #//div[@class="codeblock phpcodeblock"]//div[@class="title"]//text()').extract()

            text = node.xpath('.//div[@class="post_body scaleimages"]//text() |.//div[@class="post_body scaleimages"]//following-sibling::fieldset//a//..//text()| .//div[@class="post_body scaleimages"]//..//img[not(contains(@src,"images/icons/"))]//@alt |.//div[@class="post_body scaleimages"]//img[not(contains(@src,"images/smilies"))]//@alt | .//blockquote/@class | .//div[@class="codeblock phpcodeblock"]//@class |  .//div[@class="codeblock"]//@class').extract()     #//div[@class="codeblock phpcodeblock"]//div[@class="title"]//text()').extract()

            text = utils.clean_text(' '.join(text)).strip()#.encode('ascii','ignore').decode('utf8')#.encode('utf8')
            #import pdb;pdb.set_trace()

            text_date = ''.join(node.xpath('.//span[@class="post_date"]//text() | .//span[@class="post_date"]/text()').extract()).replace('\r','').replace('\n','').replace('\t','')
            #text_date = ' '.join(node.xpath('.//div[@class="post_head"]//text()').extract()).replace('\r','').replace('\n','').replace('\t','')

            if text_date in text:
                text = text.replace(text_date, '')

            #post_num = ''.join(node.xpath('.//div[@class="post_head"]//strong//text()').extract()).replace('\r','').replace('\n','').replace('\t','')
            #if post_num in text:
                #text = text.replace(post_num, '')


            if 'mycode_quote' in text:
                text = text.replace('mycode_quote', 'Quote')

            if 'codeblock phpcodeblock' in text:
                 text = text.replace('codeblock phpcodeblock', 'Quote').replace('title','').replace('body','')

            if "codeblock" in text:
                text = text.replace('codeblock', 'Quote').replace('title','').replace('body','')

            Links = node.xpath(LINK).extract()
            Link = []
            for link_ in Links:
                #if 'http' not in link_: link_ = 'https:'+ link_
                if 'http' not in link_: link_ = 'https://www.at4re.net/'+ link_

                #if not ('.gif.') or ('emoticons') in link_:
                if not 'images/icons/' in link_:
                    Link.append(link_)
            links = str(Link)
            #if "[]" in links: links = ""

	    json_posts = {}
            json_posts.update({'domain' : domain,
                          'crawl_type' : crawl_type,
                          'category' : category,
                          'sub_category' : sub_category,
                          'thread_title' : thread_title,
                          'post_title'  : post_title,
                          'thread_url' : thread_url,
                          'post_id' : postid,
                          'post_url' : post_url,
                          'publish_epoch' : publish_time,
                          'fetch_epoch' : fetch_time,
                          'author' : author,
                          'author_url' : author_url,
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
                'crawl_status':0,
                'links' : author_url
                })
            self.cursor.execute(auth_que, json_author)


        nav_click = set(sel.xpath('//a[@class="pagination_next"]//@href').extract())
        for post_nav_click in nav_click:
            if post_nav_click:
                post_nav_click = self.add_http(post_nav_click)
                yield Request(post_nav_click, callback=self.parse_all_pages_links)

