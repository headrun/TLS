from datetime import timedelta
import MySQLdb
import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.spiders import Spider
import scrapy
import time
import datetime
import re
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
import json
crawl_query = utils.generate_upsert_query_authors_crawl('bleeping_computer')
upsert_query_posts = utils.generate_upsert_query_posts('bleeping_computer')
from elasticsearch import Elasticsearch
import hashlib


class BleepingSpider(scrapy.Spider):
    name = 'bleeping_threads'

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])
	self.conn,self.cursor = self.mysql_conn()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn(self):
        conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="",
                                    use_unicode=True,
                                    charset="utf8mb4")
        cursor = conn.cursor()
 	return conn,cursor
	
    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
	try:
            key = ''.join(re.findall("secure_hash\'](.*)= '(.*)",x)[0][1].split(';'))[:-1]
        except:
             key = '880ea6a14ea49e853634fbdc5015a024'
        data = {
            'auth_key': key,
            'referer': 'https://www.bleepingcomputer.com/forums/index.php',
            'ips_username': 'inqspdr2',
            'ips_password': 'lolw4@123~',
            'rememberMe': '1',
        }
        url = 'https://www.bleepingcomputer.com/forums/index.php?app=core&module=global&section=login&do=process'
	
        yield FormRequest(url, callback=self.parse_1st, formdata=data)

    def parse_1st(self, response):
	len_que = 'select count(*) from bleeping_threads_crawl '
        self.cursor.execute(len_que)
        total_links = self.cursor.fetchall()
        total_links = int(total_links[0][0])
        for i in range(1,total_links/500+2):
            que = 'select distinct(post_url) from bleeping_threads_crawl where crawl_status = 0 limit {0},{1}'.format((i-1)*500,500)
            try:
		self.cursor.execute(que)
		self.conn.commit()
	    except MySQLdb.OperationalError as e:
		self.conn.close()
		time.sleep(5)
		self.conn,self.cursor = self.mysql_conn()
		self.cursor.execute(que)
		self.conn.commit()
            data = self.cursor.fetchall()
            for url in data:
                yield Request(url[0], callback=self.parse_threads)

    def parse_threads(self, response):
        json_data = {}
        domain = "www.bleepingcomputer.com"
        thread_url = response.url
        if '/page-' in thread_url:
            crawl_type = 'catch_up'
            test = re.findall('page-\d+', thread_url)
            thread_url = thread_url.replace(''.join(test), "")
        else:
            crawl_type = 'keep_up'

        sel = Selector(response)
        category = sel.xpath(xpaths.CATEGORY).extract()[1]
        sub_category = str([sel.xpath(xpaths.SUB_CATEGORY).extract()[-1]])
        thread_title = ''.join(
            sel.xpath(xpaths.THREAD_TITLE).extract()).replace('\\', '').strip()
        json_data.update({
            "domain": domain,
            "category": category,
            "sub_category": sub_category,
            "thread_title": thread_title,
            "thread_url": thread_url,
            "post_title": '' 
        })

        post_nodes = sel.xpath('//div[@class="post_block hentry clear clearfix  "]')#sel.xpath(xpaths.POST_NODES)
        if post_nodes:
            up_que_1 = "update bleeping_threads_crawl set crawl_status = 1 where post_url = %(url)s"
            up_val = {'url': response.url}
            self.cursor.execute(up_que_1, up_val)
	    self.conn.commit()
        next_page_link = ''.join(sel.xpath(xpaths.NEXT_PAGE_LINK).extract())
        if next_page_link:
	    try:
		post_url_ = ','.join(post_nodes[-1].xpath(xpaths.POST_URL).extract()).strip()
		test_id = hashlib.md5(str(post_url_)).hexdigest() 
		query = {'query_string': {'use_dis_max': 'true', 'query': '_id : {0}'.format(test_id)}}
		res = self.es.search(index="forum_posts", body={"query": query})
		in_es = res['hits']['hits']
		if in_es ==[]:
		    yield Request(next_page_link,callback = self.parse_threads)
	    except:pass
        for node in post_nodes:
            post_url = ','.join(node.xpath(xpaths.POST_URL).extract()).strip()
            post_id = re.findall('entry(\d+)', post_url)[0]
            publish_time = ''.join(node.xpath(
                xpaths.PUBLISH_TIME).extract()).strip().replace('Today,',datetime.datetime.now().strftime('%d %B %Y -'))\
		.replace('Yesterday,',(datetime.datetime.now() - timedelta(days=1)).strftime('%d %B %Y -'))
            try:
                publish_time = datetime.datetime.strptime(
                    publish_time, '%d %B %Y - %H:%M %p')
                publish_time = time.mktime(publish_time.timetuple()) * 1000
            except:
                pass
            links = node.xpath(xpaths.LINKS).extract()
            all_links = prepare_links(links)

            author = ''.join(set(node.xpath(xpaths.AUTHOR).extract())).strip()
            author_links = "".join(node.xpath(xpaths.AUTHOR_LINKS).extract()).strip()
	    if not author_links:
                pass

            text = ""
            text = node.xpath(xpaths.TEXT)
            text = "\n".join([t.extract() for t in text])

            if "http" not in text:
                text = "\n".join(node.xpath(xpaths.TEXT_HTTP).extract())
            if ".jpg" in text:
                text = "\n".join(node.xpath(xpaths.TEXT_JPG).extract())
            junk = "".join(node.xpath(xpaths.TEXT_JUNK).extract())
            text = text.replace(junk, "")
            link = ''.join(node.xpath(xpaths.LINK).extract())
            if link:
                if ('.com' not in link) or ('http:' not in link) or ('https:' not in link):
                    link = "http" + link
            text = text + link
            auth_ = dt_ = date_ = ''
            auth_1, auth_2, date_1, date_2 = "", "", "", ""
            auth_ = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract())
            dt_ = ''.join(node.xpath(xpaths.DATE_DATE).extract())
            if auth_ and dt_:
                text = "\n".join(node.xpath(xpaths.AUTHOR_DATE_TEXT).extract())
                dt_1 = ''.join(node.xpath(xpaths.DATE_TIME).extract())
                try:
		    acd = int(dt_1)
                    asd = time.strftime('%d %b %Y - %I:%M %p', time.localtime(acd))
                    if dt_1:
                        dt_said = asd + ", said: "
                        auth_on = "Quote " + auth_ + ", on "
                        text = "\n".join(node.xpath(xpaths.TEXT_).extract())
                        text = text.replace(auth_, auth_on).replace(dt_1, dt_said)
		except:pass
                dt_said = dt_ + ", said: "
                auth_on = "Quote " + auth_ + ", on "
                text = text.replace(auth_, auth_on).replace(dt_, dt_said)
            if auth_ and dt_ and len(auth_) > 15:
                auth_ = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract())
                date_ = ''.join(node.xpath(xpaths.DATE_TIME).extract())
                text = "\n".join(node.xpath(xpaths.TEXT_AUTHOR_DATE).extract())
                auth_1, auth_2, date_1, date_2 = "", "", "", ""
                try:
                    auth_1 = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract()[0])
                except:
                    pass
                try:
                    auth_2 = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract()[1])
                except:
                    pass
                try:
                    date_1 = ''.join(node.xpath(xpaths.DATE_TIME).extract()[0])
                except:
                    pass
                try:
                    date_2 = ''.join(node.xpath(xpaths.DATE_TIME).extract()[1])
                except:
                    pass
                auth_on_1 = "Quote " + auth_1 + ", on "
                auth_on_2 = "Quote " + auth_2 + ", on "
                acd_1 = int(date_1 or 0)
                acd_2 = int(date_2 or 0)
                asd_1 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_1))
                asd_2 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_2))
                dt_said_1 = asd_1 + ", said: "
                dt_said_2 = asd_2 + ", said: "
                text = text.replace(auth_1, auth_on_1).replace(date_1, dt_said_1).replace(
                    auth_2, auth_on_2).replace(date_2, dt_said_2)
            if not dt_:
                auth_ = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract())
                date_ = ''.join(node.xpath(xpaths.DATE_TIME).extract())
            if date_ and auth_:
                text = "\n".join(node.xpath(xpaths.DATE_AUTHOR_TEXT).extract())
                try:
                    acd = int(date_)
                    asd = time.strftime(
                        '%d %b %Y - %I:%M %p', time.localtime(acd))
                    dt_said = asd + ", said: "
                    auth_on = "Quote " + auth_ + ", on "
                    text = text.replace(auth_, auth_on).replace(date_, dt_said)
                except:
                    pass
                if len(auth_) > 15:
                    try:
                        auth_1 = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract()[0])
                    except:
                        pass
                    try:
                        auth_2 = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract()[1])
                    except:
                        pass
                    try:
                        date_1 = ''.join(node.xpath(xpaths.DATE_TIME).extract()[0])
                    except:
                        pass
                    try:
                        date_2 = ''.join(node.xpath(xpaths.DATE_TIME).extract()[1])
                    except:
                        pass
                    auth_on_1 = "Quote " + auth_1 + ", on "
                    auth_on_2 = "Quote " + auth_2 + ", on "
                    acd_1 = int(date_1)
                    acd_2 = int(date_2)
                    asd_1 = time.strftime(
                        '%d %b %Y - %I:%M %p', time.localtime(acd_1))
                    asd_2 = time.strftime(
                        '%d %b %Y - %I:%M %p', time.localtime(acd_2))
                    dt_said_1 = asd_1 + ", said: "
                    dt_said_2 = asd_2 + ", said: "
                    text = text.replace(auth_1, auth_on_1).replace(date_1, dt_said_1).replace(
                        auth_2, auth_on_2).replace(date_2, dt_said_2)
            auth_ = node.xpath(xpaths.AUTHOR_QUOTE).extract()
            date_ = node.xpath(xpaths.DATE_TIME).extract()
            if len(date_) and len(auth_) == 2:
                auth_2 = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract()[1])
                auth_1 = ''.join(node.xpath(xpaths.AUTHOR_QUOTE).extract()[0])
                date_1 = ''.join(node.xpath(xpaths.DATE_TIME).extract()[0])
                date_2 = ''.join(node.xpath(xpaths.DATE_TIME).extract()[1])
                auth_on_1 = "Quote " + auth_1 + ", on "
                auth_on_2 = "Quote " + auth_2 + ", on "
                acd_1 = int(date_1)
                acd_2 = int(date_2)
                asd_1 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_1))
                asd_2 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_2))
                dt_said_1 = asd_1 + ", said: "
                dt_said_2 = asd_2 + ", said: "
                text = "\n".join(node.xpath(xpaths.FINAL_TEXT).extract())
                text = text.replace(auth_1, auth_on_1).replace(date_1, dt_said_1).replace(
                    auth_2, auth_on_2).replace(date_2, dt_said_2)

            text = text.replace('snapshot.png', "")
            text = text.replace("Quote Quote ", 'Quote ')
            text = utils.clean_text(text)
            json_data.update({
                "post_id": post_id,
                "post_url": post_url,
                "publish_time": int(publish_time),
		"fetch_time": utils.fetch_time(),
                "author": author,
                "text": utils.clean_text(text),
                "links": all_links,
                "author_url": author_links
            })
	    query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
	        self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_data)
	    auth_meta = str({
                'publish_time': publish_time,
                'thread_title': thread_title
            })
            json_data_crawl = {
                "links": author_links,
                "auth_meta": auth_meta,
                "post_id": int(post_id)
            }
            self.cursor.execute(crawl_query, json_data_crawl)
	    self.conn.commit()

def prepare_links(links_in_post):
    aggregated_links = []
    for link in links_in_post:
        aggregated_links.append(link)
    if not aggregated_links:
        all_links = '[]'
    else:
        all_links = list(set(aggregated_links))
        all_links = str(all_links)
    return all_links
