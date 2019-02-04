# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime
import time
import json
import re
import MySQLdb
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import xpaths

crawl_query = utils.generate_upsert_query_authors_crawl('posts_blackhat')

class BlackHat(scrapy.Spider):
    name = "blackhat_thread"
    start_urls = ["https://www.blackhatworld.com/forums/"]

    def __init__(self, *args, **kwargs):
        super(BlackHat,  self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(db="posts_blackhat",host="localhost",user="root",passwd="1216" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url_que = "select distinct(post_url) from blackhat_status where crawl_status = 0 limit 25000"
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread)

    def parse_thread(self, response):
        sel = Selector(response)
        reference_url = response.url
        domain = "www.blackhatworld.com"
        if '/page-' not in reference_url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'
        if '/page-' in reference_url:
            test = re.findall('/page-\d+',reference_url)
            thread_url = reference_url.replace(''.join(test),"")
        else:
            thread_url = reference_url
        thread_title = ''.join(sel.xpath(xpaths.THREADTITLE).extract())
        try:
	    category = ''.join(sel.xpath(xpaths.CATEGORY).extract()[1])
	except: pass
        try:
	    sub_category = '["' + ''.join(sel.xpath(xpaths.SUBCATEGORY).extract()[2]) + '"]'
	except: pass
        post_title = ' '
        nodes=sel.xpath(xpaths.NODES)
        if nodes:
            query = 'update blackhat_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
	if response.status not in (200,301):
	    query = 'update blackhat_status set crawl_status = 9 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)

        page_nav = ''.join(set(sel.xpath(xpaths.PAGENAV).extract()))
        if page_nav:
            que_ = 'select * from blackhat_posts  where post_id = %(post_id)s'
            try:
                text_case = ''.join(nodes[-1].xpath(xpaths.POSTID).extract()).replace('post_url_','')
                self.cursor.execute(que_,{'post_id':text_case})
                val_for_next = self.cursor.fetchall()
                if len(val_for_next) == 0:
                    meta = {'Crawl_type':'catch up'}
                    page = "https://www.blackhatworld.com/" + page_nav
                    yield Request(page,callback = self.parse_thread, headers = response.request.headers, meta=meta)
            except:pass

        for node in nodes:
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            authorurl =  ''.join(node.xpath(xpaths.AUTHORURL).extract())
            if authorurl == '':
                author_url = authorurl
            else:
                author_url = "https://www.blackhatworld.com/"  + authorurl
            post_url = "https://www.blackhatworld.com/" + ''.join(node.xpath(xpaths.POSTURL).extract())
            postid = ''.join(node.xpath(xpaths.POSTID).extract())
            post_id =  re.findall('\d+',postid)
            publishtimes = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
            publish_epoch = utils.time_to_epoch(publishtimes, '%b %d, %Y')
            post_texts = '\n'.join(node.xpath(xpaths.POST_TEXT).extract())
            post_text = utils.clean_text(post_texts.replace(u'[email\xa0protected]', ''))
            mails = node.xpath('//a[@class="__cf_email__"]/@data-cfemail').extract()
            for mail in mails:
                email = utils.decode_cloudflareEmail(mail)
                post_text = post_texts.replace(mail,email)
            if 'quoteContainer' in post_text:
                post_text = post_text.replace('quoteContainer' ,'Quote ')
            fetch_epoch = utils.fetch_time()
            author_signature = '\n'.join(node.xpath(xpaths.AUTHOR_SIGNATURE).extract())
            Links = node.xpath(xpaths.LINKS).extract()
            links = []
            for Link in Links:
                if 'http:' not in Link and 'https:' not in Link:
                    al_links = "https://www.blackhatworld.com/" + Link
                    links.append(al_links)
                else:
                    links.append(Link)

            all_links = str(links)
            query_posts = utils.generate_upsert_query_posts('posts_blackhat')
            json_posts = {'domain': domain,
                          'crawl_type': crawl_type,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_epoch': publish_epoch,
                          'fetch_epoch': fetch_epoch,
                          'author': author,
                          'author_url': author_url,
                          'post_text': "{0}".format(utils.clean_text(post_text)),
                          'all_links': all_links,
                          'reference_url': reference_url
            }
            self.cursor.execute(query_posts, json_posts)
            json_posts.update({
                'author_url': author_url,
                'all_links': all_links
            })
            # passing publish_time,thread_title,Topics using dict format
            # Write data into forums_crawl table
            meta = {'publish_epoch': publish_epoch, 'author_signature':utils.clean_text(author_signature)}
            json_crawl = {}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)

        pagenav = set(sel.xpath(xpaths.PAGENAV).extract())
        for page in pagenav:
            if page:
                page = "https://www.blackhatworld.com/" + page
                yield Request(page, callback = self.parse_thread)




