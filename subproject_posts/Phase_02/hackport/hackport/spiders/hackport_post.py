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
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from datetime import timedelta
from elasticsearch import Elasticsearch
import hashlib

query_posts = utils.generate_upsert_query_posts('hackport')

class Hackport(scrapy.Spider):
    name = "hackport_post"
    start_urls = ["http://hack-port.ru/"]

    def __init__(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn,self.cursor = self.mysql_conn()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn(self):
        conn = MySQLdb.connect(db="posts",
                                host="localhost",
                                    user="root",
                                    passwd="qwe123",
                                    use_unicode=True,
                                    charset="utf8mb4")
        cursor = conn.cursor()
        return conn,cursor

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        url_que = "select distinct(post_url) from hackport_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_ann,meta ={'crawl_type':'keep up'})

    def parse_ann(self, response):
        domain = "hack-port.ru"
        count = 0
        referenceurl = ''.join(
            set(response.xpath('//a[@class="forumBarA"]/@href').extract())).strip()
        reference_url = "http://hack-port.ru" + referenceurl
        if '-\d+[-1]' not in reference_url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'
        if '-\d+[-1]' in reference_url:
            test = re.findall('-\d+', reference_url)
            thread_url = reference_url.replace(''.join(test), "")
        else:
            thread_url = reference_url

            category = ''.join(set(response.xpath(
                '//tr[@class="ThrForumBarRow1"]//td//a[2]//text()').extract()))
            sub_category = ''.join(set(response.xpath(
                '//tr[@class="ThrForumBarRow1"]//td//a[3]//text()').extract()))
            sub_categoryurl  = response.xpath('//tr[@class="ThrForumBarRow1"]//td//a[3]//@href').extract()[1]
            sub_category_url = 'http://hack-port.ru' + sub_categoryurl
        threadtitle = ''.join(response.xpath(
            '//div[@class="gDivLeft"]//td[@class="gTableTop"]//text()').extract())
        thread_title = re.sub('\s+\s+', '', threadtitle)
        nodes = response.xpath('//tr[contains(@id,"post")]')
        for node in nodes:
            author = ''.join(node.xpath(
                './/div[@class="postTdTop2"]//center//a//text()').extract_first())
            count += 1
            ord_in_thread = count
            author_url = "http://hack-port.ru/index/8-0-" + author
            post_url = ''.join(node.xpath('.//div[@class="postTdTop2"]//span//a[@class="postNumberLink"]/@onclick').extract_first()).replace(
                u"prompt('\u041f\u0440\u044f\u043c\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430 \u043a \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044e', ", '').replace("); return false;", '')
            post_id = ''.join(re.findall('\d+', post_url)[2])
            dte = ''.join(node.xpath(
                './/span[contains(@style, "text-shadow")]//text()').extract_first())
            dt = ''.join(re.findall('(.*) \| ', dte))
            date_ = ''.join(re.findall(', (.*)', dt))
            date_dict = {u'\u041d\u043e\u044f': 'November', u'\u0421\u0435\u043d': 'September', u'\u042f\u043d\u0432': "January", u'\u0410\u043f\u0440': "April", u'\u041c\u0430\u0439': 'May', u'\u0418\u044e\u043d': 'June', u'\u0418\u044e\u043b': 'July',
                         u'\u0412\u0447\u0435\u0440\u0430': 'Yesterday', u'\u0410\u0432\u0433': 'August', u'\u041e\u043a\u0442': 'October', u'\u041c\u0430\u0440': 'March', u'\u0414\u0435\u043a': 'December', u'\u0424\u0435\u0432': 'February'}
            for key, value in date_dict.iteritems():
                if key in date_:
                    publish_time = date_.replace(key, value)
            try:
                publish_time = publish_time.replace('Yesterday,', (datetime.datetime.now(
                ) - timedelta(days=1)).strftime('%d %B %Y,')).replace('Today,', datetime.datetime.now().strftime('%d %B %Y,'))
                publishtime_ = datetime.datetime.strptime(
                    (publish_time), '%d %B %Y, %H:%M')
                publish_epoch = int(time.mktime(publishtime_.timetuple())*1000)
                if publish_epoch:
                    month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            except:
                pass
            texts = '\n'.join(node.xpath(
                './/span[contains(@id,"ucoz-forum-post-")]//text() | .//span[contains(@id,"ucoz-forum-post-")]//div[@class="radius"]//text() | .//div[@class="bbQuoteBlock"]//text() | .//div[@class="bbQuoteBlock"]/@class | .//span[contains(@id,"ucoz-forum-post-")]//img//@alt ').extract()).replace("bbQuoteBlock", "Quote ")
            try:
                text_ = eval(
                    re.sub("(.*)':", '', ''.join(re.findall(":_uVideoPlayer(.*),'width'", texts))))
                post_text = re.sub(':_uVideoPlayer(.*);', text_, text).replace(
                    '[graytable]', '').replace('[/graytable]', '')
            except:
                pass
            post_text = texts.replace(
                '[graytable]', '').replace('[/graytable]', '')
            post_text = re.sub('\s+\s+', '', post_text)
            links = node.xpath('.//span[@class="UhideBlockL"]//a[not(contains(@href,"javascript:;"))] [not(contains(@src,".gif"))]//@href | .//span[@class="ucoz-forum-post"]//img[not(contains(@src,".gif"))]/@src | .//span[@class="ucoz-forum-post"]//a[not(contains(@href,"javascript:;"))] [not(contains(@src,".gif"))]//@href | .//span[@class="UhideBlockL"]//a[not(contains(@src,".gif"))]//@src').extract()
            Link = []
            video = node.xpath(
                './/span[@class="ucoz-forum-post"]/script[@type="text/javascript"]//text()').extract()
            if video:
                link = ''.join(video)
                links = links + re.findall("""'url':'(.*)','width'""", link)
            for link_ in links:
                if 'http' not in link_:
                    link_ = 'http://hack-port.ru' + link_
                if not 'emoticons' in link_:
                    Link.append(link_)
            links = list(set(Link))
            json_posts = {}
            try:
                post_url = eval(post_url)
            except:
                pass
            author_data = {
                        'name':author,
                        'url':author_url
                        }
            post_data = {
                        'cache_link':'',
                        'author': json.dumps(author_data),
                        'section':category,
                        'language':'russian',
                        'require_login':'false',
                        'sub_section':sub_category,
                        'sub_section_url':sub_category_url,
                        'post_id':post_id,
                        'post_title':'',
                        'ord_in_thread':int(ord_in_thread),
                        'post_url':post_url,
                        'post_text':utils.clean_text(post_text).replace('\n', ''),
                        'thread_title':thread_title,
                        'thread_url':thread_url
                        }
            json_posts = {
                          'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                          'hostname':'hack-port.ru',
                          'domain': domain,
                          'sub_type':'openweb',
                          'type':'forum',
                          'author':json.dumps(author_data),
                          'title':thread_title,
                          'text':utils.clean_text(post_text).replace('\n', ''),
                          'url':post_url,
                          'original_url':post_url,
                          'fetch_time':utils.fetch_time(),
                          'publish_time':publish_epoch,
                          'link.url':links,
                          'post':post_data
                        }
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
