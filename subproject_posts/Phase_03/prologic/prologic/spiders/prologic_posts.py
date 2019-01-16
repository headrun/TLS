import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import re
import datetime
import time
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import json
import sys
import utils
import xpaths
query_posts = utils.generate_upsert_query_posts('prologic')


class PrlogicSpider(scrapy.Spider):
    name = 'prologic_posts'
    start_urls = ['http://prologic.su/']
    
    def __init__(self):
        self.conn = MySQLdb.connect(db="prologic", host="localhost", user="root", passwd="", use_unicode=True, charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)    
    
    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
	url_que = "select distinct(post_url) from prologic_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
	    yield Request(url[0], callback = self.parse_meta)

    def parse_meta(self, response):
        domain = "prologic.su"
        reference_url = response.url
        if 'page__st__' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'
        if 'page__st__' in reference_url:
            test = re.findall('page__st__\d+',reference_url)
            thread_url = reference_url.replace(''.join(test),"")
        else:
            thread_url = reference_url
        category = ''.join(response.xpath(xpaths.CATEGORY).extract())
        sub_category = '["'+''.join(response.xpath(xpaths.SUBCATEGORY).extract())+'"]'
        thread_title = ''.join(response.xpath(xpaths.THREAD_TITLE).extract()).strip()
        nodes = response.xpath(xpaths.NODES)
        for node in nodes:
            author = ''.join(node.xpath(xpaths.AUTHOR).extract()).replace('\n','')
            post_url = ''.join(node.xpath(xpaths.POST_URL).extract())
            post_id = ''.join(re.findall('#entry\d+',post_url)).replace('#entry','')
            publish_time = ''.join(node.xpath(xpaths.PUBLISH_TIME).extract())
            publish_time = ''.join(re.findall('\d+-\d+-\d+T\d+:\d+', ''.join(publish_time)))
            publish_time = datetime.datetime.strptime(publish_time, '%Y-%m-%dT%H:%M')
            publish_time = time.mktime(publish_time.timetuple())*1000
            fetchtime = round(time.time()*1000)
            input_text = ' '.join(node.xpath(xpaths.POST_TEXT).extract()).replace(u'\u041f\u0440\u043e\u0441\u043c\u043e\u0442\u0440 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f','%s ' u'\u041f\u0440\u043e\u0441\u043c\u043e\u0442\u0440 \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044f'%'Quote').replace(u'\u0426\u0438\u0442\u0430\u0442\u0430', '%s ' u'\u0426\u0438\u0442\u0430\u0442\u0430'%'Quote')
            text = utils.clean_text(input_text)
            links = node.xpath(xpaths.LINKS).extract()
	    Link = []
            for link_ in links:
            	if 'http' not in link_: link_ = 'https://prologic.su/'+ link_
               	if not 'emoticons' in link_:
                    Link.append(link_)
            links = str(Link)
            json_posts = {}
	    json_posts.update({'domain': domain,
                         'crawl_type': crawl_type,
                         'category': category,
                         'sub_category': sub_category,
                         'thread_title': thread_title,
                         'post_title' : '',
                         'thread_url': thread_url,
                         'post_id': post_id,
                         'post_url': post_url,
                         'publish_epoch': publish_time,
                         'fetch_epoch': fetchtime,
                         'author': author,
                         'author_url': '',
                         'post_text': text,
                         'all_links': links,
                         'reference_url': reference_url
            })
            self.cursor.execute(query_posts,json_posts)
        navigation = response.xpath(xpaths.PAGENATION).extract()
        if nodes and crawl_type == 'keepup':
            up_que_to1 = 'update  prologic_status set crawl_status = 1 where post_url = %(url)s'
            self.cursor.execute(up_que_to1,{'url':response.url})
        for pagenation in navigation:
            yield Request(pagenation, callback = self.parse_meta)
