from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
import cfscrape
from scrapy.selector import Selector
import MySQLdb
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import utils
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import xpaths
import re
query_posts = utils.generate_upsert_query_posts('posts_hacklife')
crawl_query = utils.generate_upsert_query_authors_crawl('posts_hacklife')


class Hack_life(scrapy.Spider):
    name = "hacklife_posts"
    handle_httpstatus_list =[403]
    def __init__(self):
        self.conn = MySQLdb.connect(
            db="posts_hacklife",
            host="127.0.0.1",
            user="root",
            passwd="1216",
            use_unicode=True,
            charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        log_in_url = 'https://hack-life.net/login/login'
        scraper = cfscrape.CloudflareScraper()
        r1 = scraper.get(log_in_url)
        sel = Selector(text=r1.text)
        token = ''.join(sel.xpath('//input[@name="_xfToken"]/@value').extract())
        data = {
            'login': 'inqspdr',
            'password': 'h@ck!nqspdr',
            'remember': '1',
            '_xfRedirect': '',
            '_xfToken': token,
        }
        r2 = scraper.post(log_in_url,data = data)
        headers = {
    'authority': 'hack-life.net',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'upgrade-insecure-requests': '1',
    'user-agent': r2.request.headers.get('User-Agent'),
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
}
        cookies = r2.cookies.get_dict()
        cooks = r2.request.headers.get('Cookie').split(";")
        [cookies.update({cook.split("=")[0]:cook.split("=")[1]}) for cook in cooks]
        response = Selector(text=r2.text)
        url="https://hack-life.net/"
        yield Request(url, callback=self.forums_page, headers=headers,cookies = cookies)

    def forums_page(self, response):
        url_que = "select distinct(post_url) from hacklife_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread,headers = response.request.headers)

    def parse_thread(self, response):
        if '/page-' in response.url:
            test = re.findall('/page-\d+',response.url)
            thread_url = response.url.replace(''.join(test),"")
        else:
            thread_url = response.url
        if '/page-' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'

        thread_title=response.xpath(xpaths.THREADTITLE).extract()
        try:
            category = response.xpath(xpaths.CATEGORY).extract()[1]
        except: pass
        try:
            sub_category = '["' + response.xpath(xpaths.SUBCATEGORY).extract()[2] + '"]'
        except: pass
        domain = "www.hack-life.net"
        post_title = ''
        nodes = response.xpath(xpaths.NODES)
        if nodes:
            query = 'update hacklife_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)
        for node in nodes:
            post_id = ''.join(node.xpath(xpaths.POST_ID).extract()).replace('post-','')
            post_url = ''.join(node.xpath(xpaths.POST_URL).extract())
            publishtime = ''.join(node.xpath(xpaths.PUBLISHTIME).extract())
            text = '\n'.join(node.xpath(xpaths.TEXT).extract())
            if 'bbCodeBlock-expandContent' in text:
                text = text.replace('bbCodeBlock-expandContent','Quote ')
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            author = utils.clean_text(author.replace(u'[email\xa0protected]', ''))
            mails = node.xpath('//span[@class="__cf_email__"]//@data-cfemail').extract()
            for mail in mails:
                email = utils.decode_cloudflareEmail(mail)
                author = author.replace(mail,email)
            author_url = ''.join(node.xpath(xpaths.AUTHOR_URL).extract())
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            links = node.xpath(xpaths.LINKS).extract()
            all_links = str(links)
            json_posts = {'domain': domain,
                          'crawl_type': crawl_type,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_epoch': publishtime,
                          'fetch_epoch': fetch_epoch,
                          'author': author,
                          'author_url': author_url,
                          'post_text': utils.clean_text(text),
                          'all_links': all_links,
                          'reference_url': response.url
            }
            self.cursor.execute(query_posts, json_posts)
            json_posts.update({
                'author_url': author_url,
                'all_links': all_links
            })
            meta = {'publish_epoch': publishtime}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)
        page_nav = ''.join(set(response.xpath(xpaths.INNERPAGENAV).extract()))
        if page_nav:
            yield Request(page_nav, callback=self.parse_thread,headers=response.request.headers)




