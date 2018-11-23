#encoding: utf- 8
import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import FormRequest
from scrapy.http import Request
import datetime
import calendar
import csv
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import re
import unicodedata
import utils



class BoardBat(scrapy.Spider):
    name = "boardbat_thread"
    start_urls = ["https://board.b-at-s.info/"]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="bb_posts",host="localhost",user="root",passwd="" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        sel = Selector(response)
        forum = 'https://board.b-at-s.info/index.php?showforum=2'
        yield Request(forum,callback=self.parse_nxt)

    def parse_nxt(self,response):
        sel = Selector(response)
        thread_urls = sel.xpath('//td[@class="col_f_content "]//a[@class="topic_title"]//@href').extract()
        for thread_url in thread_urls:
            yield Request(thread_url,callback=self.parse_thread)
        for i in set(sel.xpath('//li[@class="next"]//a[@title="Next page"]//@href').extract()):
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
        category = ','.join(sel.xpath('//span[@itemprop="title"]//text()').extract()).split(',')[1]
        try:
            sub_category = '["' + ', '.join(sel.xpath('//span[@itemprop="title"]//text()').extract()).split(',')[2] + '"]'
        except:pass
        thread_title = ''.join(sel.xpath('//div[@class="ipsBox_withphoto"]//h1[@class="ipsType_pagetitle"]//text()').extract())
        post_title = ''
        nodes = sel.xpath('//div[@class="post_wrap"]')
        for node in nodes:
            author = ''.join(node.xpath('.//span[@itemprop="creator name"]//text() | .//h3[@class="guest row2"]/text()').extract())
            post_url = ''.join(node.xpath('.//span[@class="post_id right ipsType_small desc blend_links"]//a[@itemprop="replyToUrl"]//@href').extract())
            post_id = post_url.split('#')[-1].replace('entry','')
            publish_time = ''.join(node.xpath('.//p[@class="posted_info desc lighter ipsType_small"]//abbr[@class="published"]//@title').extract())
            publish_time = ''.join(re.findall('\d+-\d+-\d+T\d+:\d+',publish_time))
            if not publish_time:
                import pdb;pdb.set_trace()
            publishdate = datetime.datetime.strptime((publish_time),'%Y-%m-%dT%H:%M')
            publish_epoch = time.mktime(publishdate.timetuple())*1000
            Text = '\n'.join(node.xpath('.//div[@class="post entry-content "]//text()  | .//div[@class="post entry-content "]//p[@class="citation"]//text() |.//blockquote[@class="ipsBlockquote"]/p/text() |.//blockquote[@class="ipsBlockquote"]/@class | .//div[@class="post entry-content "]//p[@style="text-align:center;"]//span[@rel="lightbox"]//img[@class="bbc_img"]//@alt | .//div[@class="post entry-content "]//span[@rel="lightbox"]//img[@class="bbc_img"]//@alt  | .//div[@class="post entry-content "]//img[@class="bbc_emoticon"]//@alt  | .//a//img//@alt |.//p[@class="edit"]//strong//text() | .//div[@itemprop="commentText"]//iframe[@class="EmbeddedVideo"]/@src | .//div[@itemprop="commentText"]//blockquote/@data-date | .//div[@itemprop="commentText"]//blockquote/@data-author | .//div[@itemprop="commentText"]//blockquote/@data-time ').extract())
            t_author = node.xpath('.//div[@itemprop="commentText"]//blockquote/@data-author').extract()
            t_date = node.xpath('.//div[@itemprop="commentText"]//blockquote/@data-date').extract()
            t_a_date = node.xpath('.//div[@itemprop="commentText"]//blockquote/@data-time').extract()

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
                Text = Text.replace('ipsBlockquote','Quote ')

            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            Links = node.xpath('.//a[@class="bbc_url"]/@href | .//div[@class="post entry-content "]//a[@title="Download attachment"]/@href | .//div[@class="post entry-content "]//span[@rel="lightbox"]//img[@class="bbc_img" and not(contains(@src,"/style_emoticons/"))]//@src | .//p//span[@rel="lightbox"]//img[@class="bbc_img" and not(contains(@src,"/style_emoticons/"))]//@src  | .//div[@class="post entry-content "]//iframe[@class="EmbeddedVideo"]/@src').extract()
            links = str(Links)

            query_posts = utils.generate_upsert_query_posts('bb_posts')
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
            self.cursor.execute(query_posts, json_posts)


        x = set(sel.xpath('//li[@class="next"]//a//@href').extract())
        for i in x:
            if i:
                yield Request(i,callback = self.parse_thread)

