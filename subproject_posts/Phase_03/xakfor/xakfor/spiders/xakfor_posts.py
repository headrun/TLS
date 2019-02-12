import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import json
import MySQLdb
import utils
import xpaths
import cfscrape
import re
POST_QUERY = utils.generate_upsert_query_posts('xakfor')
AUTHOR_CRAWL_QUERY = utils.generate_upsert_query_authors_crawl('xakfor')
up_que_to_1 = 'update  xakfor_threads_crawl set crawl_status = 1 where post_url = %(url)s'

class Xakfor_posts(Spider):
    name = "xakfor_posts"

    def __init__(self):
        self.conn = MySQLdb.connect(db="xakfor", host="localhost",
 use_unicode=True, charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        scraper = cfscrape.create_scraper()
        r = scraper.get("https://xakfor.net/forum/")
        request_cookies = r.request._cookies.get_dict()
        response_cookies = r.cookies.get_dict()
        cookies = {}
        cookies.update(request_cookies)
        cookies.update(response_cookies)
        headers = {'Accept': '*/*',
                   'Accept-Encoding': 'gzip, deflate',
                   'Connection': 'keep-alive',
                   'User-Agent': r.request.headers.get('User-Agent', '')
                   }
        select_que = "select distinct(post_url) from xakfor_threads_crawl where crawl_status = 0 "
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        return (Request(url[0], callback=self.parse_meta,
                        meta={'crawl_type': 'keep up', 'thread_url': url[0],
                              'headers': headers, 'cookies': cookies}, headers=headers, cookies=cookies,) for url in data)

    def parse_meta(self, response):
        cookies = response.meta.get('cookies')
        headers = response.meta.get('headers')
        nodes = response.xpath('//div[@class="cat_border_block"]')
        json_values = {}
        thread_url = re.sub('page-\d+', '', response.url)
        try:
            category = response.xpath('//fieldset[@class="breadcrumb"]//span[@itemscope="itemscope"]//span[@itemprop="title"]/text()').extract()[1]
        except: import pdb;pdb.set_trace()
        sub_category = str([response.xpath('//fieldset[@class="breadcrumb"]//span[@itemscope="itemscope"]//span[@itemprop="title"]/text()').extract()[2]])
        thread_title = ''.join(response.xpath('//div[@class="titleBar"]//h1/text()').extract())
        json_values.update({
            'domain': "xakfor.net",
            'crawl_type': response.meta.get('crawl_type', ''),
            'category':category,
            'sub_category': str(sub_category),
            'thread_title': thread_title,
            'thread_url': thread_url,
            'reference_url': response.url,
            'post_title': '',
            })
        for node in nodes:
            t1 = u"\u041d\u0430\u0436\u043c\u0438\u0442\u0435, \u0447\u0442\u043e\u0431\u044b \u0440\u0430\u0441\u043a\u0440\u044b\u0442\u044c..."
            text = ' '.join(node.xpath(xpaths.TEXT).extract()).replace('quote','Quote ').replace('bbCodeBlock bbCodeCode','Quote ')
            text = utils.clean_text(text.replace(u'[email\xa0protected]', '')).replace(t1,'')
            cfemails = node.xpath(xpaths.CFEMAIL).extract()
            for cfemail in cfemails:
                email = utils.decode_cloudflareEmail(cfemail)
                text = text.replace(cfemail, email)
            post_id = ''.join(node.xpath(xpaths.POST_ID).extract()).replace('post-', '')
            post_url = 'https://xakfor.net/'+ ''.join(node.xpath(xpaths.POST_URL).extract())
            author_url = ''.join(node.xpath(xpaths.AUTHOR_URL).extract())
            if author_url:
                author_url = 'https://xakfor.net/' + author_url
            author_name = ''.join(node.xpath(xpaths.AUTHOR_NAME).extract())
            publish_date = xpaths.ressian_date_to_eng(''.join(node.xpath(xpaths.PUBLISH_1).extract() or node.xpath(xpaths.PUBLISH_2).extract()))
            publish_time = utils.time_to_epoch(publish_date, '%d %b %Y')
            all_links = []
            links = node.xpath(xpaths.ALL_LINKS).extract()
            for i in links:
                if 'https:/' not in i:
                    i = 'https://xakfor.net/'+i
                all_links.append(i)
            json_values.update({
                'post_url': post_url,
                'author': author_name,
                'author_url': author_url,
                'publish_epoch': publish_time,
                'post_id': post_id,
                'post_text': text,
                'all_links': str(all_links)
                })
            self.cursor.execute(POST_QUERY,json_values)
            a_meta = {'publish_epoch':publish_time}
            json_author = {}
            json_author.update({
                            'post_id':post_id,
                            'auth_meta': json.dumps(a_meta),
                            'links': author_url,
                            'crawl_status': 0
                            })
            self.cursor.execute(AUTHOR_CRAWL_QUERY,json_author)
        next_page = ''.join(set(response.xpath(xpaths.NEXT_PAGE).extract()))
        if response.meta.get('crawl_type') == 'keep up':
            self.cursor.execute(up_que_to_1,{'url':response.url})
        if next_page:
            que_ = 'select * from xakfor_posts  where post_id = %(post_id)s'
            self.cursor.execute(que_,{'post_id':post_id})
            val_for_next = self.cursor.fetchall()
            if len(val_for_next) == 0:
                yield Request('https://xakfor.net/'+next_page, callback=self.parse_meta, headers=headers, cookies=cookies, meta={'headers': headers, 'cookies': cookies,'crawl_type': 'catch up'})


