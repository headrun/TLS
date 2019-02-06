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


query_posts = utils.generate_upsert_query_posts('hackport')

class HackportSpider(scrapy.Spider):
    name = 'hackport_posts'
    start_urls = ['http://hack-port.ru/']
    hanlde_httpstatus_list = [500]

    def __init__(self):
        self.conn = MySQLdb.connect(db="hackport", host="localhost", user="root", passwd="1216", use_unicode=True, charset="utf8")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        url_que = "select distinct(post_url) from hackport_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            url = ["http://hack-port.ru/forum/85-1301-1"]
            yield Request(url[0], callback = self.parse_meta)

    def parse_meta(self, response):
        domain = "hack-port.ru"
        reference_url = response.url
        if '-\d+[-1]' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'
        if '-\d+[-1]' in reference_url:
            test = re.findall('-\d+',reference_url)
            thread_url = reference_url.replace(''.join(test),"")
        else:
            thread_url = reference_url

	    category = ''.join(set(response.xpath('//tr[@class="ThrForumBarRow1"]//td//a[2]//text()').extract()))
	    sub_category = '["' + ','.join(set(response.xpath('//tr[@class="ThrForumBarRow1"]//td//a[3]//text()').extract())) + '"]'
        thread_title =  ''.join(response.xpath('//div[@class="gDivLeft"]//td[@class="gTableTop"]//text()').extract())
        nodes =  response.xpath('//tr[contains(@id,"post")]')
        for node in nodes:
            author = ''.join(node.xpath('.//div[@class="postTdTop2"]//center//a//text()').extract_first())
            post_url=''.join(node.xpath('.//div[@class="postTdTop2"]//span//a[@class="postNumberLink"]/@onclick').extract_first()).replace(u"prompt('\u041f\u0440\u044f\u043c\u0430\u044f \u0441\u0441\u044b\u043b\u043a\u0430 \u043a \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u044e', ",'').replace("); return false;", '')
            post_id = ''.join(re.findall('\d+',post_url)[2])
            dte =  ''.join(node.xpath('.//span[contains(@style, "text-shadow")]//text()').extract_first())
            dt = ''.join(re.findall('(.*) \| ', dte))
            date_ = ''.join(re.findall(', (.*)',dt))
            date_dict = {u'\u041d\u043e\u044f':'November', u'\u0421\u0435\u043d':'September',u'\u042f\u043d\u0432':"January",u'\u0410\u043f\u0440':"April",u'\u041c\u0430\u0439':'May',u'\u0418\u044e\u043d':'June',u'\u0418\u044e\u043b':'July',u'\u0412\u0447\u0435\u0440\u0430':'Yesterday',u'\u0410\u0432\u0433':'August',u'\u041e\u043a\u0442':'October',u'\u041c\u0430\u0440':'March',u'\u0414\u0435\u043a':'December',u'\u0424\u0435\u0432':'February'}
            for key,value in date_dict.iteritems():
                if key in date_: publish_time = date_.replace(key,value)
            try:
                publishtime_ = datetime.datetime.strptime((publish_time),'%d %B %Y, %H:%M')
                publish_epoch = int(time.mktime(publishtime_.timetuple())*1000)
            except:
                import pdb;pdb.set_trace()
	        #input_text = ''.join(node.xpath('.//span[@class="ucoz-forum-post"]//text() | .//span[@style="color:#FF0000"]//text() | .//div[@class="quoteMessage"]//text() | .//div[@class="WrapIfo2"]//span//img//@alt').extract_first()).replace(u'\u0426\u0438\u0442\u0430\u0442\u0430', '%s ' u'\u0426\u0438\u0442\u0430\u0442\u0430'%'Quote')
            texts = ''.join(node.xpath('.//span[contains(@id,"ucoz-forum-post-")]//text() | .//span[contains(@id,"ucoz-forum-post-")]//div[@class="radius"]//text() | .//div[@class="bbQuoteBlock"]//text() | .//div[@class="bbQuoteBlock"]/@class ').extract())
            #text = utils.clean_text(texts)
            try:
                text_ = eval(re.sub("(.*)':",'',''.join(re.findall(":_uVideoPlayer(.*),'width'",texts))))
                post_text = re.sub(':_uVideoPlayer(.*);', text_,text).replace('[graytable]','').replace('[/graytable]','')
            except:
                import pdb;pdb.set_trace()
                post_text = texts.replace('[graytable]','').replace('[/graytable]','')
            links = node.xpath('.//span[@class="UhideBlockL"]//a[not(contains(@href,"javascript:;"))]//@href | .//span[@class="ucoz-forum-post"]//img/@src | .//span[@class="ucoz-forum-post"]//a[not(contains(@href,"javascript:;"))]//@href | .//div[@class="WrapInfo2"]//a//@src').extract()
            Link = []
            video = node.xpath('.//span[@class="ucoz-forum-post"]/script[@type="text/javascript"]//text()').extract()
            if video:
                link = ''.join(video)
                links =  links + re.findall("""'url':'(.*)','width'""",link)
            for link_ in links:
                if 'http' not in link_: link_ = 'http://hack-port.ru/'+ link_
                if not 'emoticons' in link_:
                    Link.append(link_)
            links = str(Link)
            json_posts = {}
            json_posts.update({'domain': domain,
                         'crawl_type': crawl_type,
                         'category': category,
                         'sub_category': sub_category,
                         'thread_title': thread_title,
                         'post_title' : '',
                         'thread_url': thread_url,
                         'post_id': post_id,
                         'post_url': post_url,
                         'publish_epoch': publish_epoch,
                         'author': author,
                         'author_url': '',
                         'post_text': post_text,
                         'all_links': links,
                         'reference_url': reference_url
            })
            import pdb;pdb.set_trace()
            self.cursor.execute(query_posts,json_posts)

