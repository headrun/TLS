import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import re
import datetime
import time
import MySQLdb
import utils
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from pojie_cn_xpaths import *
import unicodedata
import json

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

class Pojie_CN(scrapy.Spider):
    name = 'pojie_cn_crawl'
    start_urls = ["https://www.52pojie.cn"]

    def __init__(self):
        self.query = utils.generate_upsert_query_posts('pojie_cn')
        self.conn = MySQLdb.connect(db="POJIE_DB",
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

    def start_requests(self):
        url_queue = "select distinct(links) from pojie_status where crawl_status = 0 "
        self.cursor.execute(url_queue)
        data = self.cursor.fetchall()
        for url in data:
            #url = "https://www.52pojie.cn/thread-721975-1-1.html"
            #url = "https://www.52pojie.cn/thread-214819-1-1.html"
            #yield Request(url, callback = self.parse_meta)
            yield Request(url[0], callback = self.parse_meta)

    def parse_meta(self, response):
        sel = Selector(response)
        thread_url = response.url
        json_posts = {}
        domain = "www.52pojie.cn"
        #id_ = ''.join(re.sub('-\d+.html','',re.sub('(.*)thread-\d+-','',response.url)))
        url = response.url
        url = url.split('-')
        if url[2] == '1':
            crawl_type = "keepup"
        else:
            crawl_type = "catchup"
        subcategory = "[" + ''.join(sel.xpath(SUB_CATE).extract()) + "]"
        category = ''.join(sel.xpath(CAT).extract())
        thread_title = ''.join(sel.xpath('//span[@id="thread_subject"]//text()').extract()) or \
                        ''.join(sel.xpath(THREAD_TITLE).extract())
        json_posts.update({'domain': domain,
                            'crawl_type': crawl_type,
                            'thread_url': thread_url,
                            'thread_title' : thread_title
        })
        nodes = sel.xpath(NODES)
        up_que = 'update pojie_status set crawl_status = 1 where links = %(url)s'
        if nodes:
            val = {'url':response.url}
            self.cursor.execute(up_que,val)
        text = ""
        for node in nodes:
            text = ''.join(node.xpath(TEXT).extract())
            try:
                text = clean_spchar_in_text(self,text)
            except:
                text = text
            add_text =  ''.join(node.xpath(ADD_TEXT).extract())
            text_1 = text + ' ' + add_text
            asidess =  node.xpath('.//div[@class="quote"]/@class')
            asides_temp =[aside.extract().replace('\n','').replace('\t','') for aside in asidess]
            asides = [x for x in asides_temp if x]
            for quote in set(asides):
                text = text_1.replace(quote, ' Quote ')
            text = text.replace(".pcb{margin-right:0}",'')
            publish_tim = ''.join(node.xpath(PUBLISH_TIME).extract())
            publish_time = ''.join(re.findall('\d{4}-\d-\d \d{2}:\d{2}', publish_tim)) \
                         or ''.join(re.findall('\d{4}-\d-\d{2} \d{2}:\d{2}', publish_tim)) \
                         or ''.join(re.findall('\d{4}-\d{2}-\d{2} \d{2}:\d{2}', publish_tim))\
                         or ''.join(re.findall('\d{4}-\d{2}-\d \d{2}:\d{2}', publish_tim))


            try:
                publishtime_ = datetime.datetime.strptime((publish_time), '%Y-%m-%d %I:%M')
            except:
                publishtime_ = datetime.datetime.strptime((publish_time), '%Y-%m-%d %H:%M')
            publish_epoch = int(time.mktime(publishtime_.timetuple())*1000)
            fetchtime = (round(time.time()*1000))
            author = ''.join(node.xpath(AUTHOR).extract())
            author_link = ''.join(node.xpath(AUTHOR_URL).extract())
            if "http" not in author_link: author_link = site_domain + author_link
            joindate = ''.join(node.xpath(PUBLISH_TIME).extract())
            join_date = ''.join(re.findall('\d{4}-\d-\d \d{2}:\d{2}', joindate)) \
                        or ''.join(re.findall('\d{4}-\d-\d{2} \d{2}:\d{2}', joindate)) \
                         or ''.join(re.findall('\d{4}-\d{2}-\d{2} \d{2}:\d{2}', joindate))

            try:
                joindate = datetime.datetime.strptime((join_date), '%Y-%m-%d %I:%M')                
                join_date = int(time.mktime(joindate.timetuple())*1000)
            except:
                try:
                    joindate = datetime.datetime.strptime((join_date), '%Y-%m-%d %H:%M')
                    join_date = int(time.mktime(joindate.timetuple())*1000)
                except:
                    join_date = ""
            post = ''.join(node.xpath(POST_ID).extract())
            post_id = post.replace('postnum','')
            posturl = ''.join(node.xpath(POST_URL).extract())
            if "http" not in posturl: posturl = site_domain + posturl
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
                                'reference_url': response.url,
            })
            Link = []
            links = node.xpath(LINKS).extract()
            for link in links:
                if "javascript:;" in link: continue
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
            auth_sig = ''.join(node.xpath('.//div[@class="sign"]//p//text()').extract())
            if author_link:
                meta = {'publish_epoch': publish_epoch, 'thread_title': thread_title, \
                    'join_date':join_date, 'author':author}
                json_crawl = {
                            "post_id": post_id,
                            "auth_meta": json.dumps(meta),
                            "links": author_link,
                            }
                crawl_query = utils.generate_upsert_query_crawl('pojie_cn')
                self.cursor.execute(crawl_query, json_crawl)
                self.conn.commit()

        nxt_pg = ''.join(sel.xpath(NAV_PG).extract())
        if nxt_pg:
            if "http" not in nxt_pg: navigation = site_domain + nxt_pg
            yield Request(navigation, callback = self.parse_meta)


