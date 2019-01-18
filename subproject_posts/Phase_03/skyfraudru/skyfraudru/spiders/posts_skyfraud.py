import scrapy
import sys
reload(sys)
sys.setdefaultencoding('utf8')
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import json
import re
import MySQLdb
import utils
import time
import xpaths
from datetime import timedelta

class skyfraudSpider(scrapy.Spider):
    name = "skyfraud"
    start_urls = ["https://sky-fraud.ru/"]

    def __init__(self):
        self.conn =MySQLdb.connect(db="posts",host="localhost", use_unicode = True , charset = 'utf8')
        self.cursor= self.conn.cursor()
        
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url_que = "select distinct(post_url) from skyfraud_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data: 
            yield Request(url[0], callback = self.parse_ann)



    def parse_ann(self, response):
	crawl_type = response.meta.get('crawl_type')
        sel = Selector(response)
        Domain = "sky-fraud.ru"
        try:
            category = ''.join(response.xpath(xpaths.CATEGORY).extract()[1])
        except:
            import pdb;pdb.set_trace()
        sub_category = '["' + ''.join(response.xpath(xpaths.SUBCATEGORY).extract()[2]) + '"]'
        thread_title = ''.join(re.findall('<title> (.*)</title>', response.body)).replace('- SKY-FRAUD.RU','')
        nodes = sel.xpath(xpaths.NODES)
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
		import pdb;pdb.set_trace()
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
            query_posts = utils.generate_upsert_query_posts('posts_skyfraud')
            json_posts = {'domain': Domain,
                          'crawl_type': crawl_type,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : Posttitle,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': Post_url,
                          'publish_epoch': PublishTime,
                          'fetch_epoch': FetchTime,
                          'author': Author,
                          'author_url': author_url,
                          'post_text': utils.clean_text(Text),
                          'all_links': all_links,
                          'reference_url': response.url
            }
            self.cursor.execute(query_posts, json_posts)
            crawl_query = utils.generate_upsert_query_authors_crawl('posts_skyfraud')
            meta = {'publish_epoch': PublishTime}
            json_crawl = {}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
        pagenav = sel.xpath(xpaths.PAGENAV).extract()
        for page in pagenav:
	    if "http" not in page:
		page = "https://sky-fraud.ru/" + page
		yield Request(page, callback = self.parse_ann,meta = {'crawl_type':'catch_up'})
            if page:
		pno = ''.join(re.findall('&page=\d+',page))
                if crawl_type == 'keep_up':
		    page = response.url + pno
		else:
		    page = re.sub('&page=\d+',pno,response.url)
