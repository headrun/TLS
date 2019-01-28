#encoding: utf- 8
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import re
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import xpaths


class BoardBat(scrapy.Spider):
    name = "boardbat_thread"
    start_urls = ["https://board.b-at-s.info/"]

    def __init__(self, *args, **kwargs):
	self.conn, self.cursor = self.mysql_conn()
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def mysql_conn(self):
	conn = MySQLdb.connect(db="posts_boardbat",host="localhost",user="root",passwd="" , use_unicode = True , charset = 'utf8mb4')
        cursor = conn.cursor()
	return conn, cursor

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        sel = Selector(response)
        forum = 'https://board.b-at-s.info/index.php?showforum=2'
        yield Request(forum,callback=self.parse_nxt)

    def parse_nxt(self,response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREADURLS).extract()
        for thread_url in thread_urls:
            yield Request(thread_url,callback=self.parse_thread)
        for i in set(sel.xpath(xpaths.INNERPAGENAVIGATION).extract()):
            if i:
                yield Request(i,callback = self.parse_nxt)

    def parse_thread(self, response):
        thread_url = response.url
        if '&page=' in response.url:
            test = re.findall('&page=\d+',response.url)
            thread_url = response.url.replace(''.join(test),"")
        else:
            thread_url = response.url

        if '&page=' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'

        sel = Selector(response)
        domain = "www.board.b-at-s.info"
        category = ','.join(sel.xpath(xpaths.CATEGORY).extract()).split(',')[1]
        try:
            sub_category = '["' + ', '.join(sel.xpath(xpaths.SUBCATEGORY).extract()).split(',')[2] + '"]'
        except:
            logger.exception("OUT THE INDEX")
        thread_title = ''.join(sel.xpath(xpaths.THREADTITLE).extract())
        post_title = ''
        nodes = sel.xpath(xpaths.NODES)
        for node in nodes:
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            post_url = ''.join(node.xpath(xpaths.POST_URL).extract())
            post_id = post_url.split('#')[-1].replace('entry','')
            publish_time = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
            publish_time = ''.join(re.findall('\d+-\d+-\d+T\d+:\d+',publish_time))
            if not publish_time:
                pass
            publish_epoch = utils.time_to_epoch(publish_time,'%Y-%m-%dT%H:%M')
            Text = '\n'.join(node.xpath(xpaths.POST_TEXT).extract())

            t_author = node.xpath(xpaths.TEXT_AUTHOR).extract()
            t_date = node.xpath(xpaths.TEXT_DATE).extract()
            t_a_date = node.xpath(xpaths.TEXT_DATE_AUTHOR).extract()
            t_auth = ''.join(node.xpath(xpaths.TEXT_AUTHOR).extract())

            if t_a_date and t_date:
                for a,t,t1 in zip(set(t_author),t_a_date,t_date):
                    a1 =  a + ',on '
                    t2 = t1 + ', said:'
                    Text = Text.replace(a,a1).replace(t,'').replace(t1,t2)

            elif t_date and t_author:
                for a,t in zip(set(t_author),t_date):
                    a1 =   a + ', on '
                    t1 = t+', said:'
                    Text = Text.replace(a,a1).replace(t,t1)


            elif t_author and t_a_date:
                for a,t in zip(set(t_author),t_a_date):
                    a1 =   a + ', on '
                    try:
                        t1 = int(t)
                        t1 = time.strftime("%d %b %Y - %H:%M %p, said:", time.localtime(t1))
                        Text = Text.replace(a,a1).replace(t,t1)

                    except:pass

            elif t_author:
                auth =  ''.join(t_author)
                text = auth + ' said '
                Text = Text.replace(auth,text)
            if 'ipsBlockquote' in Text:
                Text = Text.replace('ipsBlockquote','Quote Quote')
            for a in t_author:
                Text = re.sub('Quote Quote\n'+a+',','Quote\n'+a+',',Text)
            fetch_epoch = utils.fetch_time()
            Links = node.xpath(xpaths.LINKS).extract()
            links = str(Links)
            query_posts = utils.generate_upsert_query_posts('posts_boardbat')
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
                          'author_url': '',
                          'post_text': "{0}".format(utils.clean_text(Text)),
                          'all_links': links,
                          'reference_url': response.url
            }
            try:
		self.cursor.execute(query_posts, json_posts)
            except OperationalError as e:
                if 'MySQL server has gone away' in str(e):
                    self.conn,self.cursor = self.mysql_conn()
                    self.cursor.execute(query_posts, json_posts)
                else:raise e()

        x = set(sel.xpath('//li[@class="next"]//a//@href').extract())
        for i in x:
            if i:
                yield Request(i,callback = self.parse_thread)

