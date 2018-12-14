import MySQLdb
import utils
import xpaths
from scrapy.xlib.pydispatch import dispatcher
from scrapy import signals
from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.spiders import Spider
import scrapy
import unicodedata
import time
import datetime
import json
import re
import sys
reload(sys)
sys.setdefaultencoding('UTF8')
crawl_query = utils.generate_upsert_query_crawl('bleeping_computer')
upsert_query_posts = utils.generate_upsert_query_posts('bleeping_computer')


class BleepingSpider(scrapy.Spider):
    name = 'bleeping_threads'

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user="root",
                                    passwd="123",
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def clean_spchar_in_text(self, text):
        text = unicodedata.normalize('NFKD', text.decode('utf8'))
        text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', text)
        text = re.sub(r'(\n\s*)', '\n', text)
        return text

    def start_requests(self):
        data = {
            'auth_key': '880ea6a14ea49e853634fbdc5015a024',
            'referer': 'https://www.bleepingcomputer.com/forums/index.php',
            'ips_username': 'inqspdr',
            'ips_password': 'Inq2018.',
            'rememberMe': '1',
        }
        url = 'https://www.bleepingcomputer.com/forums/index.php?app=core&module=global&section=login&do=process'
        yield FormRequest(url, callback=self.parse_1st, formdata=data)

    def parse_1st(self, response):
        que = 'select distinct(post_url) from bleeping_threads_crawl where crawl_status = 0'
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback=self.parse_threads)

    def parse_threads(self, response):
        json_data = {}
        domain = "www.bleepingcomputer.com"
        thread_url = response.url
        if '/page-' in thread_url:
            crawl_type = 'catch_up'
            test = re.findall('page-\d+', thread_url)
            thread_url = thread_url.replace(''.join(test), "")
        else:
            crawl_type = 'keep_up'

        sel = Selector(response)
        category = sel.xpath(xpaths.CATEGORY).extract()[1]
        sub_category = str([sel.xpath(xpaths.SUB_CATEGORY).extract()[-1]])
        thread_title = ''.join(
            sel.xpath(xpaths.THREAD_TITLE).extract()).replace('\\', '').strip()
        post_title = ""
        json_data.update({
            "domain": domain,
            "crawl_type": crawl_type,
            "category": category,
            "sub_category": sub_category,
            "thread_title": thread_title,
            "thread_url": thread_url,
            "post_title": post_title
        })

        post_nodes = sel.xpath(xpaths.POST_NODES)
        if post_nodes:
            up_que_1 = "update bleeping_threads_crawl set crawl_status = 1 where post_url = %(url)s"
            up_val = {'url': response.url}
            self.cursor.execute(up_que_1, up_val)

        for node in post_nodes:

            post_url = ','.join(node.xpath(xpaths.POST_URL).extract()).strip()
            post_id = re.findall('entry(\d+)', post_url)[0]
            publish_time = ''.join(node.xpath(
                xpaths.PUBLISH_TIME).extract()).strip()
            try:
                publish_time = datetime.datetime.strptime(
                    publish_time, '%d %B %Y - %H:%M %p')
                publish_time = time.mktime(publish_time.timetuple()) * 1000
            except:
                try:
                    if 'Yesterday' in publish_time:
                        today = round(time.time() * 1000) - 24 * 60 * 60 * 1000
                        am = ''.join(re.findall('\d+:\d+ AM', publish_time))
                        pm = ''.join(re.findall('\d+:\d+ PM', publish_time))
                        if am:
                            H = int(am[0:2])
                            M = int(am[3:5])
                        if pm:
                            H = int(pm[0:2]) + 12
                            M = int(pm[3:5])
                        dt = H * 60 * 60 * 1000 + M * 60 * 1000
                        publish_time = today + dt
                except:
                    pass
            fetch_epoch = round(time.time() * 1000)

            links = node.xpath(xpaths.LINKS).extract()
            all_links = prepare_links(links)

            author = ''.join(set(node.xpath(xpaths.AUTHOR).extract())).strip()
            author_links = "".join(node.xpath(
                './/span[@class="author vcard"]//a//@href').extract()).strip()
            if not author_links:
                pass

            text = ""
            text = node.xpath(
                './/div[@class="post entry-content "]//text() | .//div[@class="post entry-content "]//li[@class="attachment"]//a//img//@alt |  .//div[@class="post entry-content "]//p//a//@alt | .//div[@class="post entry-content "]//br')
            text = "\n".join([t.extract() for t in text])

            if "http" not in text:
                text = "\n".join(node.xpath('.//div[@class="post entry-content "]//text() | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//iframe[@class="EmbeddedVideo"]//@alt | .//img[@class="attach"]//@alt | .//div[@class="post entry-content "]//li[@class="attachment"]//a[img]/@alt | .//div[@class="post entry-content "]//li[@class="attachment"]//a[strong]//text() | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img/@alt ').extract())
            if ".jpg" in text:
                text = "\n".join(node.xpath('.//div[@class="post entry-content "]//text() | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//iframe[@class="EmbeddedVideo"]//@alt | .//img[@class="attach"]//@alt | .//div[@class="post entry-content "]//li[@class="attachment"]//a[img]/@href | .//div[@class="post entry-content "]//li[@class="attachment"]//a[strong]//text() | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt ').extract())
            junk = "".join(node.xpath(
                './/div[@class="post entry-content "]//p[@style="text-align:center"]//a//img[@class="bbc_img"]/@alt').extract())
            text = text.replace(junk, "")
            link = ''.join(node.xpath(
                './/div[@id="gifv"]//video/source/@src').extract())
            if link:
                if ('.com' not in link) or ('http:' not in link) or ('https:' not in link):
                    link = "http" + link
            text = text + link
            auth_ = dt_ = date_ = ''
            auth_1, auth_2, date_1, date_2 = "", "", "", ""
            auth_ = ''.join(node.xpath(
                './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract())
            dt_ = ''.join(node.xpath(
                './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-date').extract())
            if auth_ and dt_:
                text = "\n".join(node.xpath('.//div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt ').extract())
                dt_1 = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract())
                acd = int(dt_1)
                asd = time.strftime('%d %b %Y - %I:%M %p', time.localtime(acd))
                if dt_1:
                    dt_said = asd + ", said: "
                    auth_on = "Quote " + auth_ + ", on "
                    text = "\n".join(node.xpath('.//div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt ').extract())
                    text = text.replace(auth_, auth_on).replace(dt_1, dt_said)
                dt_said = dt_ + ", said: "
                auth_on = "Quote " + auth_ + ", on "
                text = text.replace(auth_, auth_on).replace(dt_, dt_said)
            if auth_ and dt_ and len(auth_) > 15:
                auth_ = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract())
                date_ = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract())
                text = "\n".join(node.xpath('.//div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt ').extract())
                auth_1, auth_2, date_1, date_2 = "", "", "", ""
                try:
                    auth_1 = ''.join(node.xpath(
                        './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract()[0])
                except:
                    pass
                try:
                    auth_2 = ''.join(node.xpath(
                        './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract()[1])
                except:
                    pass
                try:
                    date_1 = ''.join(node.xpath(
                        './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract()[0])
                except:
                    pass
                try:
                    date_2 = ''.join(node.xpath(
                        './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract()[1])
                except:
                    pass
                auth_on_1 = "Quote " + auth_1 + ", on "
                auth_on_2 = "Quote " + auth_2 + ", on "
                acd_1 = int(date_1 or 0)
                acd_2 = int(date_2 or 0)
                asd_1 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_1))
                asd_2 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_2))
                dt_said_1 = asd_1 + ", said: "
                dt_said_2 = asd_2 + ", said: "
                text = text.replace(auth_1, auth_on_1).replace(date_1, dt_said_1).replace(
                    auth_2, auth_on_2).replace(date_2, dt_said_2)
            if not dt_:
                auth_ = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract())
                date_ = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract())
            if date_ and auth_:
                text = "\n".join(node.xpath('.//div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt ').extract())
                try:
                    acd = int(date_)
                    asd = time.strftime(
                        '%d %b %Y - %I:%M %p', time.localtime(acd))
                    dt_said = asd + ", said: "
                    auth_on = "Quote " + auth_ + ", on "
                    text = text.replace(auth_, auth_on).replace(date_, dt_said)
                except:
                    pass
                if len(auth_) > 15:
                    try:
                        auth_1 = ''.join(node.xpath(
                            './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract()[0])
                    except:
                        pass
                    try:
                        auth_2 = ''.join(node.xpath(
                            './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract()[1])
                    except:
                        pass
                    try:
                        date_1 = ''.join(node.xpath(
                            './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract()[0])
                    except:
                        pass
                    try:
                        date_2 = ''.join(node.xpath(
                            './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract()[1])
                    except:
                        pass
                    auth_on_1 = "Quote " + auth_1 + ", on "
                    auth_on_2 = "Quote " + auth_2 + ", on "
                    acd_1 = int(date_1)
                    acd_2 = int(date_2)
                    asd_1 = time.strftime(
                        '%d %b %Y - %I:%M %p', time.localtime(acd_1))
                    asd_2 = time.strftime(
                        '%d %b %Y - %I:%M %p', time.localtime(acd_2))
                    dt_said_1 = asd_1 + ", said: "
                    dt_said_2 = asd_2 + ", said: "
                    text = text.replace(auth_1, auth_on_1).replace(date_1, dt_said_1).replace(
                        auth_2, auth_on_2).replace(date_2, dt_said_2)
            auth_ = node.xpath(
                './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract()
            date_ = node.xpath(
                './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract()
            if len(date_) and len(auth_) == 2:
                auth_2 = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract()[1])
                auth_1 = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author').extract()[0])
                date_1 = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract()[0])
                date_2 = ''.join(node.xpath(
                    './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time').extract()[1])
                auth_on_1 = "Quote " + auth_1 + ", on "
                auth_on_2 = "Quote " + auth_2 + ", on "
                acd_1 = int(date_1)
                acd_2 = int(date_2)
                asd_1 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_1))
                asd_2 = time.strftime(
                    '%d %b %Y - %I:%M %p', time.localtime(acd_2))
                dt_said_1 = asd_1 + ", said: "
                dt_said_2 = asd_2 + ", said: "
                text = "\n".join(node.xpath('.//div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt ').extract())
                text = text.replace(auth_1, auth_on_1).replace(date_1, dt_said_1).replace(
                    auth_2, auth_on_2).replace(date_2, dt_said_2)

            text = text.replace('snapshot.png', "")
            text = text.replace("Quote Quote ", 'Quote ')
            text = utils.clean_spchar_in_text(text)
            json_data.update({
                "post_id": post_id,
                "post_url": post_url,
                "publish_epoch": int(publish_time),
                "fetch_epoch": int(fetch_epoch),
                "author": author,
                "post_text": utils.clean_text(text),
                "all_links": all_links,
                "reference_url": response.url,
                "author_url": author_links
            })
            self.cursor.execute(upsert_query_posts, json_data)

            auth_meta = str({
                'publish_time': publish_time,
                'thread_title': thread_title
            })
            json_data_crawl = {
                "links": author_links,
                "auth_meta": auth_meta,
                "post_id": int(post_id)
            }
            self.cursor.execute(crawl_query, json_data_crawl)

        next_page_link = ''.join(sel.xpath(
            '//link[@rel="stylesheet"]//following-sibling::link[@rel="next"]/@href').extract())
        if next_page_link:
            yield Request(next_page_link, callback=self.parse_threads)


def prepare_links(links_in_post):
    aggregated_links = []
    for link in links_in_post:
        aggregated_links.append(link)
    if not aggregated_links:
        all_links = '[]'
    else:
        all_links = list(set(aggregated_links))
        all_links = str(all_links)
    return all_links
