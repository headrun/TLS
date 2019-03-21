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
import unicodedata
from hacker_threads_xpaths import *
crawl_query = utils.generate_upsert_query_authors_crawl('hacker_threads')

def clean_spchar_in_text(self, text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    #text = unicodedata.normalize('NFKD', text.decode('utf8')).encode('utf8')
    text = unicodedata.normalize('NFKD', text.decode('utf8'))
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
    #text = re.sub(r'(\n\s*)', '\n', text).encode('utf-8').encode('ascii', 'ignore')
    text = re.sub(r'(\n\s*)', '\n', text)
    return text

class Hacker_threads(scrapy.Spider):
    name = "hacker_threads"
    start_urls = ["https://www.hackerthreads.org/"]

    def __init__(self, *args, **kwargs):
        super(Hacker_threads, self).__init__(*args, **kwargs)
        self.query = utils.generate_upsert_query_posts('hacker_threads')
        self.conn = MySQLdb.connect(db="POSTS_HACKERTHREADS",
                                    host="localhost",
                                    user="root",
                                    passwd="1216",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def add_http(self, url):
        if 'http' not in url:
            url = url.replace('./', 'https://www.hackerthreads.org/')
        else:
            url = url.replace('./', '')
        return url

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        sel = Selector(response)
        categories = sel.xpath('//a[@class="forumtitle"]/@href').extract()
        for cate in categories[:2]:
            if "http" not in cate:
                cate =  self.add_http(cate)
                yield Request(cate, callback=self.parse_links)

    def parse_links(self, response):
        sel = Selector(response)
        links = sel.xpath('//div[@class="list-inner"]//a[@class="topictitle"]/@href').extract()
        for link in links:
            if "http" not in link:
                #link = self.add_http(link)
                link = "https://www.hackerthreads.org/Topic-35275#p120652"
                yield Request(link, callback=self.parse_meta)

        nxt_pg=''.join(sel.xpath('//div[@class="action-bar bar-top"]//following-sibling::div[@class="pagination"]//li[@class="arrow next"]//a[@class="button button-icon-only"]/@href').extract())
        if nxt_pg:
            if "http" not  in nxt_pg:
                nxt_pg = self.add_http(nxt_pg)
                yield Request(nxt_pg, callback=self.parse_links)

    def parse_meta(self, response):
        sel = Selector(response)
        url =response.url
        url_ = url.split('-')
        if len(url_) > 2:
            crawl_type = "catchup"
        else:
            crawl_type = "keepup"
	    json_posts = {}
        thread_url = response.url
        domain = "www.hackerthreads.org"
        category = sel.xpath(CATEGORY).extract()[1]
        subcategory = "[" + ''.join(sel.xpath(CATEGORY).extract()[2]) + "]"
        thread_title = ''.join(sel.xpath(THREAD_TITLE).extract())
        json_posts.update({'domain': domain,
                            'crawl_type': crawl_type,
                            'thread_url': thread_url,
                            'thread_title' : thread_title
        })
        nodes = sel.xpath('//div[contains(@class, "post has-profile bg")]')
        for node in nodes:
            publishtime_, publish_epoch, publish_time,author_link,join_date,text ="","","","","",""
            post_title = ''.join(node.xpath(POST_TITLE).extract())
            text = ''.join(node.xpath(TEXT).extract())
            asidess = node.xpath('.//cite//text()')
            if asidess:
                asides_temp = [aside.extract().replace('\n','') for aside in asidess]
                asides = [x for x in asides_temp if x]
                for author in set(asides):
                    if author:
                        text = text.replace(author, ' Quote ' + author)
            try:
                text = clean_spchar_in_text(self,text)
            except:
                text = text.encode("utf8")
                text = clean_spchar_in_text(self,text)
            author = ''.join(node.xpath(AUTHOR).extract())
            publish_time = ''.join(node.xpath(PUBLISH_TIME).extract()).replace('\n','').replace('\t','')
            try:
                publishtime_ = datetime.datetime.strptime(publish_time,'%a %b %d, %Y %H:%M %p')
            except:
                pass
            publish_epoch = int(time.mktime(publishtime_.timetuple())*1000)
            fetchtime = (round(time.time()*1000))
            post_url = ''.join(node.xpath(POST_URL).extract())
            authorlink = ''.join(node.xpath(AUTHOR_LINK).extract())
            if "http" not in authorlink:
                author_link = self.add_http(authorlink)
            Link = []
            postid =  ''.join(node.xpath(POST_ID).extract())
            post_id = postid.replace('#p','')
            post_url = ''.join(node.xpath(POST_URL).extract())
            if "http" not in post_url: post_url = response.url + post_url
            total_posts= ''.join(node.xpath(TOTAL_POSTS).extract())
            group = ''.join(node.xpath(GROUP).extract())
            join_date = ''.join(node.xpath(JOIN_DATE).extract())
            try:
                joindate = datetime.datetime.strptime(join_date, ' %a %b %d, %Y %H:%M %p')
                join_date = int(time.mktime(joindate.timetuple())*1000)
            except:
                join_date =  " "

            json_posts.update({
                                'category': category,
                                'sub_category': subcategory,
                                'post_title': post_title,
                                'post_id': post_id,
                                'post_url': post_url,
                                'publish_epoch': publish_epoch,
                                'fetch_epoch': fetchtime,
                                'author': author,
                                'post_text': text,
                                'reference_url': response.url
            })

            links = node.xpath('.//div[@class="content"]//a[@class="postlink"]/@href | .//div[@class="content"]//img[@class="postimage"]/@src | .//div[@class="content"]//following-sibling::div[@class="notice"]/a/@href | .//div[@class="content"]//following-sibling::dl[@class="attachbox"]//a[@class="postlink"]/@href').extract()

            for link in links:
                if 'hackerthreads.org' not in link:
                    link = self.add_http(link)
                Link.append(link)
            Links = str(Link)

            json_posts.update({
                                'author_url': author_link,
                                'all_links': Links
            })
            self.cursor.execute(self.query, json_posts)
            self.conn.commit()
            author_signature = ''.join(node.xpath('.//div[@class="signature"]//text() | .//div[@class="signature"]//a[@class="postlink"]/@href').extract())
            if author_link:
                meta = {'publish_epoch': publish_epoch, 'thread_title': thread_title, 'join_date':join_date, 'total_posts':total_posts, 'author':author, 'author_signature':author_signature, 'group':group}
                json_crawl = {
                               "post_id": post_id,
                               "auth_meta": json.dumps(meta),
                                'crawl_status':0,
                               "links": author_link,
                            }
                self.cursor.execute(crawl_query, json_crawl)
                self.conn.commit()

        nxt_pg = ''.join(response.xpath('//form//following-sibling::div[@class="pagination"]//li[@class="arrow next"]//a[@rel="next"]/@href').extract())
        if nxt_pg:
            next_pg = re.sub('sid=(.*?)&', "", nxt_pg)
            if "http" not in next_pg:
                next_pg = self.add_http(next_pg)
                yield Request(next_pg, callback=self.parse_meta)



