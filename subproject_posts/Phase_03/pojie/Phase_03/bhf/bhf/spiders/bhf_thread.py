import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import MySQLdb
import  utils
import datetime
import time
import hashlib
from elasticsearch import Elasticsearch

POST_QUERY = utils.generate_upsert_query_posts('tls')

class TheGub(Spider):
    name="bhf_thread"
    start_urls = ["https://bhf.io/"]

    def __init__(self,*args,**kwargs):
        self.conn = MySQLdb.connect(db= "tls", host = "localhost", user="root", passwd = "123", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()
        self.es = Elasticsearch('http://192.168.0.8:9222')

    def start_requests(self):
        que = 'select distinct(post_url) from bhf_status where crawl_status = 0 '
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread)


    def parse_thread(self,response):
        sel = Selector(response)
        category = ''.join(sel.xpath('//li//a//span[@itemprop="name"]/text()').extract()[1])
        sub_category = '["' + ''.join(sel.xpath('//li//a//span[@itemprop="name"]/text()').extract()[2]) + '"]'
	thread_title = ''.join(sel.xpath('//h1[@class="p-title-value"]/text()').extract())
        thread_url = response.url
        nodes = response.xpath('//div[@class="block-body js-replyNewMessageContainer"]//div[@class="message-inner"]')
        navigations = response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract()
        for navigation in set(navigations):
            navigation = "https://bhf.io" + navigation
            yield Request(navigation,callback=self.parse_thread)

        if nodes:
            up_que = 'update bhf_status set crawl_status = 1 where post_url = %(url)s'
            val = {'url':  response.request.url}
            self.cursor.execute(up_que,val)

	for node in nodes:
            publish = ''.join(node.xpath('.//div[@class="message-attribution-main"]//time[@class="u-dt"]/@title').extract())
            publish = publish.replace(u'\u041e\u043a\u0442','oct').replace(u'\u0418\u044e\u043b','July').replace(u'\u0424\u0435\u0432','Feb').replace(u'\u041c\u0430\u0440','Mar').replace(u'\u0418\u044e\u043d','Jun').replace(u'\u041d\u043e\u044f','Nov').replace(u'\u0410\u0432\u0433','Aug').replace(u'\u0432','').replace(u'\u0414\u0435\u043a','Dec').replace(u'\u042f\u043d' ,'Jan').replace(u'\u041c\u0430\u0439', 'May').replace(u'\u0421\u0435\u043d','Sep').replace(u'\u0410\u043f\u0440','Apr')

            try:
                publish_time =  datetime.datetime.strptime(publish,'%d %b %Y  %H:%M')
	        publish_epoch = time.mktime(publish_time.timetuple())*1000
            except:
                try:
                    publish_time = datetime.datetime.strptime(publish,'%d %B %Y  %H:%M')
                    publish_epoch = time.mktime(publish_time.timetuple())*1000
                except:pass

            author = ''.join(node.xpath('.//h4//span[@class="username"]//text() | .//span[@class="username "]//text()').extract())
	    post_url = ''.join(set(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//a[@rel="nofollow"]//@href').extract()))
	    post_url = "https://bhf.io" + post_url
            post_id = post_url.split("post-")[-1]
            post_text =  ''.join(node.xpath('.//article[@class="message-body js-selectToQuote"]//text() | .//div[@class="bbCodeBlock bbCodeBlock--screenLimited bbCodeBlock--code"]//text() | .//div[@class="bbCodeBlock bbCodeBlock--screenLimited bbCodeBlock--code"]/@class | .//blockquote[@class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote"]//text() | ..//blockquote[@class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote"]/@class  | .//img[@class="smilie"]//@alt | .//img[@class="smilie smilie--emoji"]//@alt | .//img[@class="smilie smilie--sprite smilie--sprite655"]//@alt | .//article[@class="message-body js-selectToQuote"]//@data-cfemail ').extract())
            post_text =  post_text.replace('bbCodeBlock bbCodeBlock--screenLimited bbCodeBlock--code','Quote').replace('bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote', 'Quote').replace(u'[email\xa0protected]','')
            mails = node.xpath('.//article[@class="message-body js-selectToQuote"]//@data-cfemail').extract()
            for mail in mails:
                asd = mail.replace(u'[email\xa0protected]','')
                email_id = utils.decode_cloudflareEmail(mail)
                post_text= post_text.replace(mail,email_id)
            links = []
            all_link_ = node.xpath('.//img[@class="bbImage"]//@src | .//div[@class="bbCodeBlock-title"]/a/@href ').extract()
            fetch_epoch = utils.fetch_time()
            links = []
            for all_link in all_link_:
                if "https://bhf.io" not in all_link:
                    all_link = "https://bhf.io" + all_link
                    links.append(all_link)
                else:
                    links.append(all_link)
            all_links = str(links)
	    doc = {
                        'domain':"bhf.io",
                        'category':category,
                        'sub_category': sub_category,
                        'thread_title':thread_title,
                        'thread_url': thread_url,
                        'post_title':' ',
                        'publish_time': publish_epoch,
                        'author': author,
                        'post_url':post_url,
                        'post_id':post_id,
                        'author_url': ' ',
                        'text':post_text,
                        'links':all_links,
                        'fetch_time':utils.fetch_time(),
                        #'reference_url':response.url
                        }
            self.es.index(index='forum_posts',doc_type = 'post',id=hashlib.md5(post_url).hexdigest(),body = doc)

