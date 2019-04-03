import scrapy
import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import json
import re
import MySQLdb
import time
import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from datetime import timedelta
from elasticsearch import Elasticsearch
import hashlib

query_posts = utils.generate_upsert_query_posts('posts_skyfraud')
crawl_query = utils.generate_upsert_query_authors_crawl('posts_skyfraud')

class skyfraudSpider(scrapy.Spider):
    name = "skyfraud_posts"
    start_urls = ["https://sky-fraud.ru/"]

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
        url_que = "select distinct(post_url) from skyfraud_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data: 
            yield Request(url[0], callback = self.parse_ann,meta ={'crawl_type':'keep up'})



    def parse_ann(self, response):
	crawl_type = response.meta.get('crawl_type')
        sel = Selector(response)
        Domain = "sky-fraud.ru"
        try:
            category = ''.join(response.xpath(xpaths.CATEGORY).extract()[1])
        except:
            category = ''
        try:
	    sub_category = '["' + ''.join(response.xpath(xpaths.SUBCATEGORY).extract()[2]) + '"]'
	except:
	    sub_category = ''
        thread_title = ''.join(re.findall('<title> (.*)</title>', response.body)).replace('- SKY-FRAUD.RU','')
        nodes = sel.xpath(xpaths.NODES)
        pagenav = set(sel.xpath(xpaths.PAGENAV).extract())
        for page in pagenav:
            if "http" not in page:
		try:
		    test_case = ''.join(nodes[-1].xpath(xpaths.POSTURL).extract())
		    test_id = hashlib.md5(str(test_case)).hexdigest()
		    query = {'query_string': {'use_dis_max': 'true', 'query': '_id:{0}'.format(test_id)}}
		    res = es.search(index="forum_posts", body={"query": query})
		    if res['hits']['hits']==[]:
		        page = "https://sky-fraud.ru/" + page
                        yield Request(page, callback = self.parse_ann,meta = {'crawl_type':'catch_up'})
		except:pass
            if page:
                pno = ''.join(re.findall('&page=\d+',page))
                if crawl_type == 'keep_up':
                    page = response.url + pno
                else:
                    page = re.sub('&page=\d+',pno,response.url)

        for node in nodes:
            authorurl = ''.join(node.xpath(xpaths.AUTHORURL).extract())
            if 'http'and 'https' not in authorurl:
                author_url = "https://sky-fraud.ru/" + authorurl
            Posttitle = ' '.join(node.xpath(xpaths.POSTTITLE).extract())
            Post_url = ''.join( node.xpath(xpaths.POSTURL).extract())
            if 'http'and 'https' not in Post_url :
                Post_url = "https://sky-fraud.ru/" + Post_url
            post_id = ''.join(re.findall('\p=\d+',Post_url)).replace('p=','').strip()
	    publish= ''.join(node.xpath(xpaths.PUBLISH).extract()).replace('\n','').replace('\r','').replace('\t','').replace(u'\u0412\u0447\u0435\u0440\u0430',(datetime.datetime.now() - timedelta(days=1)).strftime('%d.%m.%Y')).replace(u'\u0421\u0435\u0433\u043e\u0434\u043d\u044f',datetime.datetime.now().strftime('%d.%m.%Y'))
            try:
		publishdate = datetime.datetime.strptime(publish,'%d.%m.%Y, %H:%M')
            	PublishTime =time.mktime(publishdate.timetuple())*1000
	    except:
		PublishTime = 0
            FetchTime = int(datetime.datetime.now().strftime("%s")) * 1000
            Author =  ''.join(node.xpath(xpaths.AUTHOR).extract())
            text = ' '.join(node.xpath(xpaths.TEXT).extract()).strip().replace(u'\u0426\u0438\u0442\u0430\u0442\u0430:',u'\u0426\u0438\u0442\u0430\u0442\u0430: %s'%'Quote')
            Text = re.sub('\s\s+', ' ', text)
            thread_url = response.url
            Links = node.xpath(xpaths.LINKS_).extract()
	    links = []
            for link in Links:
		if 'http' not in link:
                    link = "https://sky-fraud.ru/" + link
                if not 'smilies' in link:
                    links.append(link)
		#else :
                    #links.append(link)
            all_links = str(links)
            reference_url = response.url
            if '&page=' not in response.url:
                crawl_type = 'keepup'
            else:
                crawl_type = 'catchup'
            if '&page=' in reference_url:
                test = re.findall('&page=\d+',reference_url)
                thread_url = reference_url.replace(''.join(test),"")
            else:
                thread_url = reference_url
            json_posts = {'domain': Domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : Posttitle,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': Post_url,
                          'publish_time': PublishTime,
                          'fetch_time': FetchTime,
                          'author': Author,
                          'author_url': author_url,
                          'text': utils.clean_text(Text),
                          'links': all_links
            }
	    query={"query":{"match":{"_id":hashlib.md5(str(Post_url)).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
	        self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(Post_url)).hexdigest(), body=json_posts)
	    meta = {'publish_epoch': PublishTime}
            json_crawl = {}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
        if nodes and crawl_type == 'keepup':
             up_que_to1 = 'update  skyfraud_status set crawl_status = 1 where post_url = %(url)s'
             self.cursor.execute(up_que_to1,{'url':response.url})
