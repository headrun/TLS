#encoding: utf- 8
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import json
import MySQLdb
import re
import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import tls_utils as utils
import xpaths
import hashlib
from elasticsearch import Elasticsearch

class BoardBat(scrapy.Spider):
    name = "boardbat_thread"
    start_urls = ['https://board.b-at-s.info/index.php?showforum=2']

    def __init__(self):
	self.es = Elasticsearch(['10.2.0.90:9342'])

    def parse(self,response):
        sel = Selector(response)
        thread_urls = sel.xpath(xpaths.THREADURLS).extract()
        for thread_url in thread_urls:
	    yield Request(thread_url,callback=self.parse_thread)
        for i in set(sel.xpath(xpaths.INNERPAGENAVIGATION).extract()):
            if i:
                yield Request(i,callback = self.parse)

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
            pass#logger.exception("OUT THE INDEX")
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
            json_posts = {'domain': domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'fetch_time': fetch_epoch,
                          'author': author,
                          'author_url': '',
                          'text': utils.clean_text(Text),
                          'links': links
            }
	    query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
	        self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
        x = set(sel.xpath('//li[@class="next"]//a//@href').extract())
        for i in x:
            if i:
                yield Request(i,callback = self.parse_thread)

