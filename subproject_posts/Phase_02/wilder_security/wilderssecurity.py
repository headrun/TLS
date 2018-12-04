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
from wilderssecurity_xpaths import *
import unicodedata

def clean_spchar_in_text(self, text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    text = unicodedata.normalize('NFKD', text.decode('utf8'))
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
    text = re.sub(r'(\n\s*)', '\n', text)
    return text



class WilderssecuritySpider(scrapy.Spider):
    name = 'wilderssecurity'
    start_urls = ['http://wilderssecurity.com/']
    handle_httpstatus_list=[403]

    def __init__(self, *args, **kwargs):
        super(WilderssecuritySpider, self).__init__(*args, **kwargs)
        self.query = utils.generate_upsert_query_posts('wilder_security')
        self.conn = MySQLdb.connect(db="POSTS_WILDER",
                                    host="localhost",
                                    user="root",
                                    passwd = "root",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        urls = response.xpath(URLS).extract()
        for url in urls:
            if "http" not in url: url = site_domain + url
            yield Request(url, callback = self.parse_next)

    def parse_next(self, response):
        thread_urls = response.xpath('//h3[@class="title"]//a//@href').extract() or response.xpath('//h3[@class="nodeTitle"]//a//@href').extract()
        for thread_url in thread_urls:
            if "http:" not in thread_url: thread_url = site_domain +thread_url
            yield Request(thread_url, callback = self.parse_meta)

        navigation = ''.join(response.xpath(NAVIGATION).extract())
        if navigation:
            if "http" not in navigation: page_nation = site_domain + navigation
            yield Request(page_nation, callback = self.parse_next)


    def parse_meta(self, response):
        thread_url = response.url.split('/page')[0]
        json_posts = {}
        domain = "www.wilderssecurity.com"
        if "/page-" in response.url:
            crawl_type = "catchup"
        else:
            crawl_type = "keepup"
        category = ''.join(set(response.xpath(CATEGORY).extract()))
        subcategory = "[" + ''.join(set(response.xpath(SUBCATEGORY).extract())) + "]"

        thread_title = ''.join(response.xpath(THREAD_TITLE).extract())
        json_posts.update({'domain': domain,
                            'crawl_type': crawl_type,
                            'thread_url': thread_url,
                            'thread_title' : thread_title
        })
        nodes = response.xpath(NODES)
        text = ""
        for node in nodes:
            text = ''.join(node.xpath(TEXT).extract())
            text = clean_spchar_in_text(self,text)
            asidess = node.xpath('.//aside//div[@class="attribution type"]/text()')
            if asidess:
                asides_temp = [aside.extract().replace('\n','').replace('\t','') for aside in asidess]
                asides = [x for x in asides_temp if x]
                for author in set(asides):
                    text=text.replace(author, ' Quote '+author)
            text = text.replace("Click to expand...", "")
            text = clean_spchar_in_text(self,text)
            author_link = ''.join(node.xpath(AUTHOR_LINK).extract())
            if "http" not in author_link: author_link = site_domain + author_link
            author = ''.join(node.xpath(AUTHOR).extract())
            publish_time = ''.join(node.xpath(PUBLISH_TIME).extract()) or ''.join(node.xpath('.//a/abbr[@class="DateTime"]/@data-datestring').extract())

            try:
                publishtime_ = datetime.datetime.strptime((publish_time), '%b %d, %Y at %I:%M %p')
            except:
                publishtime_ = datetime.datetime.strptime((publish_time), '%b %d, %Y')
            publish_epoch = int(time.mktime(publishtime_.timetuple())*1000)
            fetchtime = (round(time.time()*1000))
            posturl = ''.join(node.xpath(POSTURL).extract())
            if "http" not in posturl: posturl = site_domain+ posturl
            post_id = ''.join(node.xpath('.//a[@title="Permalink"]/@data-href').extract())
            post_id = ''.join(re.findall('\d+',post_id))
            total_posts = ''.join(node.xpath('.//dl[@class="pairsJustified"]//dt[contains(text(), "Posts:")]//following-sibling::dd/a/text()').extract()) or ''.join(node.xpath('.//dl[@class="pairsJustified"]//dt[contains(text(), "Posts:")]//following-sibling::dd//text()').extract())
            join_date = ''.join(node.xpath(JOINDATE).extract())
            try:
                joindate = datetime.datetime.strptime((join_date), '%b %d, %Y')
                join_date = int(time.mktime(joindate.timetuple())*1000)
            except:
                join_date = ""

            json_posts.update({
                                'category': category,
                                'sub_category': subcategory,
                                'post_title': '',
                                'post_id': post_id,
                                'post_url': posturl,
                                'publish_epoch': publish_epoch,
                                'fetch_epoch': fetchtime,
                                'author': author,
                                'post_text': text,
                                'reference_url': response.url
            })

            Link = []
            groups = ''.join(node.xpath(GROUPS).extract())
            if not groups: groups = ""
            links = node.xpath('.//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img[contains(@class, "bbCodeImage")]/@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a[contains(@class, "Link")]/@href | .//ul[@class="attachmentList SquareThumbs"]//div[@class="thumbnail"]//img/@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a[@class="username"]/@href').extract()

            for link in links:
                if "http" not in link: link = site_domain + link
                Link.append(link)
            Links = str(Link)
            if "[]" in Links: Links = ' '
            json_posts.update({
                                'author_url': author_link,
                                'all_links': Links
            })
            self.cursor.execute(self.query, json_posts)
            self.conn.commit()

            if author_link:
                meta = {'publish_epoch': publish_epoch, 'thread_title': thread_title, 'join_date':join_date, 'total_posts':total_posts, 'author':author, 'groups': groups}
                json_crawl = {
                            "post_id": post_id,
                            "auth_meta": json.dumps(meta),
                            "links": author_link,
                            }
                crawl_query = utils.generate_upsert_query_crawl('wilder_security')
                self.cursor.execute(crawl_query, json_crawl)
                self.conn.commit()



        nav_urls = ''.join(response.xpath('//link[@rel="next"]/@href').extract())
        if nav_urls:
            if "http" not in nav_urls:  navigation = site_domain + nav_urls
            yield Request(navigation, callback = self.parse_meta)

