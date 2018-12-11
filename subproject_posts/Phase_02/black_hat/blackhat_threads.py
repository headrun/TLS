# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import datetime
import time
import json
import re
import MySQLdb
from selenium.webdriver.support.wait import WebDriverWait
from selenium import webdriver
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import xpaths


class BlackHat(scrapy.Spider):
    name = "blackhat_thread"
    start_urls = ["https://www.blackhatworld.com/forums/"]
    handle_httpstatus_list = [503]

    def __init__(self, *args, **kwargs):
        super(BlackHat,  self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(db="posts_blackhat",host="localhost",user="root",passwd="" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.driver = open_driver()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        try:
            self.driver.quit()
        except Exception as exe:
            pass
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        sel = Selector(response)
        forums = sel.xpath(xpaths.FORUMS).extract()
        for forum in forums:
            forum = "https://www.blackhatworld.com/" + forum
            yield Request(forum,callback= self.parse_nxt)

    def parse_nxt(self, response):
        sel = Selector(response)
        urls = sel.xpath(xpaths.THREADURLS).extract()
        for url in urls:
            url = "https://www.blackhatworld.com/" + url
            yield Request(url, callback = self.parse_thread)
        for page in set(sel.xpath(xpaths.INNERPAGENAVIGATION).extract()):
            if page:
                page = "https://www.blackhatworld.com/" + page
                yield Request(page, callback = self.parse_nxt)


    def parse_thread(self, response):
        if "[email" in response.body  and "protected]" in response.body:
            self.driver.get(response.url)
            time.sleep(1)
            WebDriverWait(self.driver, 10)
            reference_url = response.url
            sel = Selector(text = self.driver.page_source)
        else:
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
        category = ''.join(sel.xpath(xpaths.CATEGORY).extract()[1])
        sub_category = '["' + ''.join(sel.xpath(xpaths.SUBCATEGORY).extract()[2]) + '"]'
        post_title = ' '
        nodes=sel.xpath(xpaths.NODES)
        for node in nodes:
            author = ''.join(node.xpath(xpaths.AUTHOR).extract()
            authorurl =  ''.join(node.xpath(xpaths.AUTHORURL).extract())
            if authorurl == '':
                author_url = authorurl
            else:
                author_url = "https://www.blackhatworld.com/"  + authorurl
            post_url = "https://www.blackhatworld.com/" + ''.join(node.xpath(xpaths.POSTURL).extract())
            postid = ''.join(node.xpath(xpaths.POSTID).extract())
            post_id =  re.findall('\d+',postid)
            publishtimes = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
            publishdate = datetime.datetime.strptime(publishtimes, '%b %d, %Y')
            publish_epoch = time.mktime(publishdate.timetuple())*1000
            post_text = '\n'.join(node.xpath(xpaths.POST_TEXT).extract())
            if 'quoteContainer' in post_text:
                post_text = post_text.replace('quoteContainer' ,'Quote ')
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            author_signature = ','.join(node.xpath(xpaths.AUTHOR_SIGNATURE).extract())
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
            if author_url:
                # passing publish_time,thread_title,Topics using dict format
                # Write data into forums_crawl table
                meta = {'publish_epoch': publish_epoch, 'author_signature':author_signature.format(utils.clean_text(author_signature))}
                json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'links': author_url
                }
            crawl_query = utils.generate_upsert_query_crawl('posts_blackhat')
            self.cursor.execute(crawl_query, json_crawl)

        pagenav = set(sel.xpath(xpaths.PAGENAV).extract())
        for page in pagenav:
            if page:
                page = "https://www.blackhatworld.com/" + page
                yield Request(page, callback = self.parse_thread)

def open_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--incognito")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument('--headless')
    driver = webdriver.Chrome(chrome_options=options)
    return  driver


