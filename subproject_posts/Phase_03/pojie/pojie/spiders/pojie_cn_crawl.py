import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import re
import hashlib
import datetime
import time
import MySQLdb
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
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
    es = Elasticsearch(['10.2.0.90:9342'])

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="tls_dev",
                                    passwd = "hdrn!",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url_queue = "select distinct(post_url) from pojie_status where crawl_status = 0 "
        self.cursor.execute(url_queue)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_meta)

    def parse_meta(self, response):
        sel = Selector(response)
        thread_url = response.url
        json_posts = {}
        domain = "52pojie.cn"
        url = response.url
        url = url.split('-')
        try:
            if url[2] == '1':
                crawl_type = "keepup"
            else:
                crawl_type = "catchup"
        except:
             pass
        subcategory = ''.join(sel.xpath(SUB_CATE).extract()) or 'Null'
        sub_categoryurl = ''.join(sel.xpath('//div[@class="z"]//em[2]//following-sibling::a[2]//@href').extract()) 
        if sub_categoryurl:
            sub_category_url = 'https://www.52pojie.cn/' + sub_categoryurl
        if sub_categoryurl == '':
            sub_category_url = 'Null'
        category = ''.join(sel.xpath(CAT).extract()) or 'Null'
        thread_title = ''.join(sel.xpath('//span[@id="thread_subject"]//text()').extract()) or \
                        ''.join(sel.xpath(THREAD_TITLE).extract()) or 'Null'
        nodes = sel.xpath(NODES)
        up_que = 'update pojie_status set crawl_status = 1 where post_url = %(url)s'
        if nodes:
            val = {'url':response.url}
            self.cursor.execute(up_que,val)
        text = ""
        ord_in_thread = 0
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
            ord_in_thread = ord_in_thread + 1
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
            if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
                    month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                else:
                    continue
            else:
                import pdb;pdb.set_trace()
            fetchtime = (round(time.time()*1000))
            author_name = ''.join(node.xpath(AUTHOR).extract()) or 'Null'
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
            Link = []
            links = node.xpath(LINKS).extract()
            for link in links:
                if "javascript:;" in link: continue
                if "http" not in link: link = site_domain + link
                Link.append(link)
            Links = str(Link)
            if "[]" in Links: Links = ' '
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            author_data = {
                'name':author_name,
                'url':author_link
                }
            post = {
                'cache_link':'',
                'author':json.dumps(author_data),
                'section':category,
                'language':'chinese',
                'require_login':'false',
                'sub_section':subcategory,
                'sub_section_url':sub_category_url,
                'post_id':post_id,
                'post_title':'Null',
                'ord_in_thread':ord_in_thread,
                'post_url':posturl,
                'post_text':utils.clean_text(text).replace('\n',''),
                'thread_title':thread_title,
                'thread_url':thread_url
                }
            json_posts = {
                          'record_id':'Null',
                          'hostname':'www.52pojie.cn',
                          'domain': domain,
                          'sub_type':'openweb',
                          'type':"forum",
                          'author':json.dumps(author_data),
                          'title':thread_title,
                          'text':utils.clean_text(text).replace('\n',''),
                          'url':posturl,
                          'original_url':posturl,
                          'fetch_time':fetch_epoch,
                          'publish_time':publish_epoch,
                          'link.url':links,
                          'post':post
            }
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(domain+post_id).hexdigest(), body=json_posts, request_timeout=30)
            auth_sig = ''.join(node.xpath('.//div[@class="sign"]//p//text()').extract())
            if author_link:
                meta = {'publish_epoch': publish_epoch, 'thread_title': thread_title, \
                    'join_date':join_date, 'author':author_name}
                json_crawl = {
                            "post_id": post_id,
                            "auth_meta": json.dumps(meta),
                            'crawl_status':0,
                            "links": author_link,
                            }
                crawl_query = utils.generate_upsert_query_authors_crawl('pojie')
                self.cursor.execute(crawl_query, json_crawl)
                self.conn.commit()

        nxt_pg = ''.join(sel.xpath(NAV_PG).extract())
        if nxt_pg:
            if "http" not in nxt_pg: navigation = site_domain + nxt_pg
            yield Request(navigation, callback = self.parse_meta)


