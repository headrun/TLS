import sys
reload(sys)
sys.setdefaultencoding('utf8')
import json
import re
import datetime
import time
import MySQLdb
import scrapy
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import xpaths
import logging

class Hackforums(scrapy.Spider):
    name ="hackforums_posts"
    handle_httpstatus_list = [403]
    def __init__(self, *args, **kwargs):
        super(Hackforums, self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(db="posts_hackforums", host="localhost", user="root", passwd="1216", use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url = "https://hackforums.net/"
        time.sleep(3)
        yield Request(url, callback=self.parse, meta={'proxy':'http://74.70.67.218:59112'})


    def parse(self, response):
        sel = Selector(response)
        headers = {
         'authority': 'hackforums.net',
         'pragma': 'no-cache',
         'cache-control': 'no-cache',
         'origin': 'https://hackforums.net',
         'upgrade-insecure-requests': '1',
         'content-type': 'application/x-www-form-urlencoded',
         'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
         'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,/;q=0.8',
         'referer': 'https://hackforums.net/member.php?action=login',
         'accept-encoding': 'gzip, deflate, br',
         'accept-language': 'en-US,en;q=0.9',
        }
        data = {
         'username': 'kerspdr',
         'password': 'Inqspdr2018.',
         'quick_gauth_code': '',
         'submit': 'Login',
         'action': 'do_login',
         'url': 'https://hackforums.net/'
        }
        url_form = "https://hackforums.net/member.php"
        time.sleep(5)
        yield FormRequest(url_form, callback=self.parse_next, formdata=data, meta={'proxy':'http://74.70.67.218:59112'},dont_filter=True)

    def parse_next(self, response):
        url_que = "select distinct(post_url) from hackforum_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread,meta={'proxy':'http://74.70.67.218:59112'})

    def parse_thread(self, response):
        logger=logging.getLogger()
        logger.setLevel(logging.WARNING)
        logger.setLevel(logging.ERROR)
        sel = Selector(response)
        if '&page=' in response.url:
            test = re.findall('&page=\d+',response.url)
            thread_url = response.url.replace(''.join(test),"")
        else:
            thread_url = response.url
        if '&page=' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'
        domain = 'www.hackforums.net'
        thread_title = ''.join(sel.xpath(xpaths.THREADTITLE).extract()).replace('Quick Reply','')
        post_title = ''
        try:category = sel.xpath(xpaths.CATEGORY).extract()[1]
        except:logger.warning("out of the index")
        try:sub_category = '["' + sel.xpath(xpaths.SUBCATEGORY).extract()[2] + '"]'
        except:logger.warning("out of the index")
        nodes = sel.xpath(xpaths.NODES)
        if nodes:
            query = 'update hackforum_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)

        for node in nodes:
            post_url = ''.join(node.xpath(xpaths.POSTURL).extract())
            post_id = ''.join(node.xpath(xpaths.POSTID).extract()).replace('post_url_','')
            publishs = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
            publish = publishs.split('M')[0] + 'M'
            publish_time = publish.replace(''.join(re.findall('\d+ minutes ago',publish) or re.findall('\d+ hours ago',publish)),'')
            try:
                publishdate = datetime.datetime.strptime(publish_time, '%m-%d-%Y, %I:%M %p')
            except:
                try:
                    publishdate = datetime.datetime.strptime(publish_time, '%m-%d-%Y, %I:%M %p ')
                except:
                    if '{1}' in publish:
                        try:
                            publis = ''.join(publish.replace('{1}',datetime.datetime.now().strftime('%m-%d-%Y')))
                            publishdate = datetime.datetime.strptime(publis, '%m-%d-%Y, %I:%M %p')
                        except:logger.error("value error")

            publish_epoch = (int(time.mktime(time.strptime(str_of_time, str_of_patter))) - time.timezone) * 1000
            fetch_epoch = utils.fetch_time()
            Text = '\n'.join(node.xpath(xpaths.POST_TEXT).extract())
            post_text = Text
            if 'mycode_quote' in Text:
                post_text = post_text.replace('mycode_quote', 'Quote ')
            if 'Quote Quote:' in Text:
                post_text = post_text.replace('Quote Quote:','Quote:')

            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            author_url = ''.join(node.xpath(xpaths.AUTHOR_URLS).extract())
            links = node.xpath(xpaths.LINKS).extract()
            if '.gif' in links:
                links = node.xpath(xpaths.GIF_LINKS).extract()

            all_links = str(links)
            query_posts = utils.generate_upsert_query_posts('posts_hackforums')
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
                          'reference_url': response.url
            }
            self.cursor.execute(query_posts, json_posts)

            json_posts.update({
                'author_url': author_url,
                'all_links': all_links
            })

            if author_url:
                # passing publish_time using dict format
                # Write data into forums_crawl table
                meta = {'publish_epoch': publish_epoch}
                json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
                }
            crawl_query = utils.generate_upsert_query_authors_crawl('posts_hackforums')
            self.cursor.execute(crawl_query, json_crawl)


        page_nav = set(sel.xpath(xpaths.INNERPAGENAV).extract())
        for page in page_nav:
            if page:
                page = "https://hackforums.net/" + page
                yield Request(page, callback = self.parse_thread, meta={'proxy':'http://74.70.67.218:59112'})

