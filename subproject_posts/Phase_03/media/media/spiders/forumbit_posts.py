#encoding: utf-8
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import utils
import xpaths
query_posts = utils.generate_upsert_query_posts('posts_forumbit')
crawl_query = utils.generate_upsert_query_authors_crawl('posts_forumbit')


class Forumbit(scrapy.Spider):
    name = "forumbit_posts"
    start_urls = ["https://forum.bits.media/"]
    handle_httpstatus_list = [404]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="posts_forumbit",host="localhost",user="root",passwd="1216" , use_unicode = True , charset = 'utf8mb4')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        url_que = "select distinct(post_url) from forumbit_status where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_thread)


    def parse_thread(self, response):
        sel = Selector(response)
        domain = "www.forum.bits.media"
        if '/&page=' in response.url:
            test = re.findall('/&page=\d+',response.url)
            thread_url = response.url.replace(''.join(test),"")
        else:
            thread_url = response.url

        if '/&page=' not in response.url:
            crawl_type = 'keepup'
        else:
            crawl_type = 'catchup'

        thread_title = ''.join(response.xpath(xpaths.THREADTITLE).extract()).replace('\n','').replace('\t','')
    	category = ''.join(response.xpath(xpaths.CATEGORY).extract()[1])
        try:
            sub_category =  '["' + ''.join(response.xpath(xpaths.SUBCATEGORY).extract()[2]) + '"]'
        except: pass
        nodes = response.xpath(xpaths.NODES)
        if nodes:
            query = 'update forumbit_status set crawl_status = 1 where post_url = %(url)s'
            json_data={'url':response.url}
            self.cursor.execute(query,json_data)

        for node in nodes:
            authors = ''.join(node.xpath(xpaths.AUTHOR).extract()[0])
            author = ''.join(re.sub('\s\s+', ' ', authors))
            author_url = ''.join(node.xpath(xpaths.AUTHOR_URL).extract())
            post_url = ''.join(node.xpath(xpaths.POST_URL).extract())
            post_id = post_url.split('=')[-1]
            publishtimes = ''.join(node.xpath(xpaths.PUBLISHTIME).extract()).replace('Posted','').replace('(edited)','').replace('\n','').replace('\t','')
            publishdate = datetime.datetime.strptime(publishtimes,'%m/%d/%y %I:%M:%S  %p')
            publish_epoch = time.mktime(publishdate.timetuple())*1000
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            text = ''.join(node.xpath(xpaths.POST_TEXT).extract()).replace('ipsSpoiler', 'Hide contents ')
            text = re.sub('\s\s+', ' ', text)
            text_date = node.xpath(xpaths.TEXT_DATE).extract()
            text_author =  node.xpath(xpaths.TEXT_AUTHOR).extract()
            if 'ipsQuote' in text:
                text = re.sub('\s\s+', ' ', text)
                text = text.replace('ipsQuote', 'Quote Quote ')
            if u'\u0426\u0438\u0442\u0430\u0442\u0430' in text:
                text = re.sub('\s\s+', ' ', text)
                text = text.replace('Quote Quote %s'%(u'\u0426\u0438\u0442\u0430\u0442\u0430'),'Quote %s'%(u'\u0426\u0438\u0442\u0430\u0442\u0430'))

            for a,dt in zip(text_author,text_date):
                x = int(dt)
                z1 = dt + a
                z2 = a+dt
                z3 = time.strftime("On %m/%d/%Y at %I:%M %p, ", time.localtime(x)) + a + ' said:'
                text = text.replace(z1,z3).replace(z2,z3)
                if not dt and a:
                    t_author = ''.join(a)+ 'said: '
                    text = text.replace('a','t_author')

            for a in text_author:
                x = a+' said:'
                text = re.sub('Quote Quote ' +a,'Quote ' +x,text)
                text = re.sub('Quote Quote On', 'Quote On ',text)
                if 'Format.C^' in text:
                    y = 'Format.C^' + ' said:'
                    text = re.sub(('Quote Quote ' +'Format.C\^'),'Quote ' +y,text)
            text = re.sub('Quote Quote  Quote ','Quote Quote ',text)

            author = utils.clean_text(author.replace(u'[email\xa0protected]', ''))
            mails = node.xpath('.//span[@class="__cf_email__"]//@data-cfemail').extract()
            for mail in mails:
                email = utils.decode_cloudflareEmail(mail)
                author = author.replace(mail,email)

            text = utils.clean_text(text.replace(u'[email\xa0protected]', ''))
            mails = node.xpath('.//p//a[@class="__cf_email__"]//@data-cfemail | .//span[@class="__cf_email__"]/@data-cfemail').extract()
            for mail in mails:
                email = utils.decode_cloudflareEmail(mail)
                text = text.replace(mail,email)

            Links = node.xpath(xpaths.LINKS).extract()
            links = []
            for Link in Links:
                if 'http:' not in Link and 'https:' not in Link:
                    al_links = "https://forum.bits.media" + Link
                    links.append(al_links)
                else:
                    links.append(Link)

            all_links = str(links)
            rm_text = node.xpath('.//div[@class="ipsType_normal ipsType_richText ipsContained"]//div[contains(@class,"ipsQuote_citation")]//text()').extract()
            for rm in rm_text:
                text= text.replace(rm.strip(),'')
            json_posts = {'domain': domain,
                          'crawl_type': crawl_type,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : '',
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_epoch': publish_epoch,
                          'fetch_epoch': fetch_epoch,
                          'author': author.replace('[email protected]',''),
                          'author_url': author_url,
                          'post_text': utils.clean_text(text),
                          'all_links': all_links,
                          'reference_url': response.url
            }
            self.cursor.execute(query_posts, json_posts)

            json_posts.update({
                'author_url': author_url,
                'all_links': links
            })

            meta = {'publish_epoch': publish_epoch}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(crawl_query, json_crawl)

