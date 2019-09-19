import utils
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
import re
POST_QUERY = utils.generate_upsert_query_posts('bbs')
AUTHOR_CRAWL_QUERY = utils.generate_upsert_query_authors_crawl('bbs')


class Bbs_posts(Spider):
    name = "bbs_posts"
    def __init__(self):
        self.conn = MySQLdb.connect(db= "bbs", host = "127.0.0.1", user="root", passwd = "123", use_unicode=True, charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = "select distinct(post_url) from bbs_threads_crawl where crawl_status = 0 limit 2"
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        meta = {'crawl_type':'keep up'}
        for url in data:
            url = url[0]
            url = 'https://bbs.pediy.com/thread-168402.htm'
            yield Request(url, callback = self.parse_meta, meta = {'crawl_type':'keep up','thread_url':url})

    def parse_meta(self, response):
        crawl_type = response.meta.get('crawl_type','')
        category = ''.join(response.xpath(xpaths.CATEGORY).extract()).strip()
        sub_category = '#<<>>#'.join(response.xpath(xpaths.SUB_CATEGORY).extract()).strip().split('#<<>>#')
        thread_title = ''.join(response.xpath(xpaths.THREAD_TITLE).extract()).strip()
        thread_url = response.meta.get('thread_url')
        json_values = {}
        thread_url = response.meta.get('thread_url','')
        json_values.update({
                        'domain':"https://bbs.pediy.com",
                        'crawl_type':crawl_type,
                        'category':category,
                        'sub_category':sub_category,
                        'thread_title':thread_title,
                        'post_url':'',
                        'post_title':'',
                        'thread_url': thread_url,
                        'reference_url':response.url
                        })
        nodes = response.xpath(xpaths.NODES)
        for node in nodes:
            publish_epoch = utils.time_to_epoch(''.join(node.xpath(xpaths.PUBLISH_EPOCH).extract()).strip(),'%Y-%m-%d %H:%M')
            if publish_epoch == False:
                import pdb;pdb.set_trace()
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            author_url = 'https://bbs.pediy.com/'+''.join(node.xpath(xpaths.AUTHOR_URL).extract())
            post_id = ''.join(node.xpath('.//@data-pid').extract())
            post_text = ' '.join(node.xpath(xpaths.POST_TEXT).extract()).strip() 
            links = node.xpath(xpaths.ALL_LINKS).extract()
            all_links = []
            for i in links:
                if 'http'not in i: i = 'https://bbs.pediy.com/'+i
                all_links.append(i)
            json_values.update({
                        'publish_epoch':publish_epoch,
                        'author':author,
                        'author_url':author_url,
                        'post_id':post_id,
                        'post_text': utils.clean_text(post_text).replace('blockquote','Quote '),
                        'all_links':str(all_links),
                        })
            self.cursor.execute(POST_QUERY,json_values)
            json_author = {}
            json_author.update({
                            'post_id':post_id,
                            'auth_meta': json.dumps({'publish_epoch':publish_epoch}),
                            'links': author_url,
                            'crawl_status': 0
                            })
            self.cursor.execute(AUTHOR_CRAWL_QUERY,json_author)
        #NEXT_PAGE REQUEST
        try:
            next_page = 'https://bbs.pediy.com/' + response.xpath(xpaths.NEXT_PAGE).extract()[0]
            yield Request(next_page, callback = self.parse_meta,meta = {'crawl_type':'catch up','thread_url':thread_url})
        except:pass
        #for post data not commats 
        if 'keep up'== crawl_type:
            post_id = ''.join(re.findall('thread-\d+',response.url)).replace('thread-','')
            text = ' '.join(response.xpath(xpaths.TEXT).extract()).strip()
            author = ''.join(response.xpath(xpaths.A_NAME).extract()).strip()
            author_url = 'https://bbs.pediy.com/'+''.join(response.xpath(xpaths.A_URL).extract())
            publish = ''.join(response.xpath(xpaths.P_EPOCH).extract())
            publish_epoch = utils.time_to_epoch(publish,'%Y-%m-%d %H:%M')
            if publish_epoch == False:
                import pdb;pdb.set_trace()
            links = response.xpath(xpaths.ALL_LINKS1).extract() 
            all_links = []
            for j in links:
                if j:
                    if 'http'not in j:
                        j = 'https://bbs.pediy.com/'+j
                    all_links.append(j)              
            json_values.update({
                        'publish_epoch' : publish_epoch,
                        'author' : author,
                        'author_url' : author_url,
                        'post_id' : post_id,
                        'post_text' : utils.clean_text(text),
                        'all_links' : str(list(set(all_links))),
                })
            self.cursor.execute(POST_QUERY,json_values)
            a_meta = {'publish_epoch':publish_epoch}
            json_author = {}
            json_author.update({
                            'post_id':post_id,
                            'auth_meta': json.dumps(a_meta),
                            'links': author_url,
                            'crawl_status': 0
                            })
            self.cursor.execute(AUTHOR_CRAWL_QUERY,json_author)
            if nodes or thread_title:
                UP_QUE_TO_1 = 'update bbs_threads_crawl set crawl_status = 1 where post_url = "%s"'%response.url
                self.cursor.execute(UP_QUE_TO_1)

