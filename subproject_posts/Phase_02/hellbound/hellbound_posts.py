# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import my_utils
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import json
import xpaths
import MySQLdb

POST_QUERY = my_utils.generate_upsert_query_posts('hellbound')
AUTHOR_CRAWL_QUERY = my_utils.generate_upsert_query_authors_crawl('hellbound')

class Hellbound(Spider):
    name = "hellbound_posts"
    def __init__(self):
        self.conn = MySQLdb.connect(db= "hellbound", host = "127.0.0.1", user="root", passwd = "", use_unicode=True,charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = "select distinct(thread_url) from hellbound_threads_crawl where status_code = 0 "
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        meta = {'crawl_type':'keep up'}
        for url in data:
            url = url[0]
            UP_QUE_TO_9 = 'update hellbound_threads_crawl set status_code = 9 where thread_url = "%s"'%url
            self.cursor.execute(UP_QUE_TO_9)
        for url in data:
            url = url[0]
            yield Request(url, callback = self.parse_meta,meta = {'crawl_type':'keep up','thread_url':url})

    def parse_meta(self, response):
        json_values = {}
        thread_url = response.meta.get('thread_url','')
        category = response.xpath(xpaths.CATEGORY).extract()[1]
        sub_category = [response.xpath(xpaths.SUB_CATEGORY).extract()[2]]
        try:thread_title = response.xpath(xpaths.THREAD_TITLE).extract()[0].replace('RE: ','')
        except:thread_title = ''.join(response.xpath('//title/text()').extract()).replace('Thread |  Hellbound Hackers','')
        fetch_epoch = my_utils.fetch_time()
        reference_url = response.url
        json_values.update({
                        'domain':"www.hellboundhackers.org",
                        'crawl_type':response.meta.get('crawl_type',''),
                        'category':category,
                        'sub_category': str(sub_category),
                        'thread_title':thread_title,
                        'post_url':'None',
                        'thread_url': thread_url,
                        'fetch_epoch':my_utils.fetch_time(),
                        'reference_url':response.url
                        })
        #POST_URL is not availible in site.
        publish_times = response.xpath(xpaths.PUBLISH_TIME).extract()
        post_titles = response.xpath(xpaths.POST_TITLE).extract()
        authors_links = response.xpath(xpaths.AITHORS_LINKS).extract()
        author_name_nodes = response.xpath(xpaths.AUTHOR_NAME_NODES)
        text_nodes = response.xpath(xpaths.TEXT_NODES)
        post_ids = response.xpath(xpaths.POST_IDS).extract()
        all_zip = zip(author_name_nodes, publish_times, post_titles, authors_links, text_nodes, post_ids)
        for author_name_mode, publish_time, post_titles, a_link, text_node, post_id in all_zip:
            author = ''.join(author_name_mode.xpath('.//text()').extract())
            publish_epoch = my_utils.time_to_epoch(publish_time,"Posted on %d-%m-%y %H:%M" )
            a_link = a_link.replace('../../','https://www.hellboundhackers.org/')
            json_values.update({
                            'author': author,
                            'author_url': a_link,
                            'publish_epoch': publish_epoch,
                            'post_title': post_titles,
                            'post_id':post_id.replace('post_','')
                            })
            #post_text = text_node.xpath(xpaths.TEXT_HR).extract() or text_node.xpath(xpaths.TEXT).extract()
            hr = text_node.xpath('.//hr')
            if hr:
                post_text = text_node.xpath(xpaths.TEXT_HR).extract()
                all_links = text_node.xpath(xpaths.ALL_LINKS_HR).extract()
            else:
                post_text = text_node.xpath(xpaths.TEXT).extract()
                all_links = text_node.xpath(xpaths.ALL_LINKS_NO_HR).extract()
            post_text = my_utils.clean_text(' '.join(post_text))
            all_links = "'"+str(list(set(all_links))).replace('../user/','https://www.hellboundhackers.org/user/')+"'"
            json_values.update({
                            'post_text':post_text,
                            'all_links':all_links
                            })
            self.cursor.execute(POST_QUERY,json_values)
            a_meta = {'publish_epoch':publish_epoch}
            json_author = {}
            json_author.update({
                            'post_id':post_id.replace('post_',''),
                            'auth_meta': json.dumps(a_meta),
                            'links': a_link,
                            'status_code': 0
                            })
            self.cursor.execute(AUTHOR_CRAWL_QUERY,json_author)
        if post_ids:
            UP_QUE_TO_1 = 'update hellbound_threads_crawl set status_code = 1 where thread_url = "%s"'%response.url
            self.cursor.execute(UP_QUE_TO_1)
        #NEXT_PAGE REQUEST
        try:
            next_page = response.xpath(xpaths.NEXT_PAGE).extract()[0].replace('./','https://www.hellboundhackers.org/forum/')
            yield Request(next_page, callback = self.parse_meta,meta = {'crawl_type':'catch up','thread_url':thread_url})
        except:pass
