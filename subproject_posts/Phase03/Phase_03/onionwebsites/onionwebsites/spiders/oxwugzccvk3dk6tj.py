import scrapy
import pprint
from scrapy.selector import Selector
from scrapy.http import Request
import re
import time
import datetime
from urlparse import urljoin
from elasticsearch import Elasticsearch
import hashlib


def clean_text(input_text):
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', input_text)
    text = re.sub(r'(\n\s*)', '\n', text)
    text = re.sub('\s\s+', ' ', text)
    return text

class ChanSpider(scrapy.Spider):
    name = "oxwugzccvk3dk6tj"
    start_urls = ["http://oxwugzccvk3dk6tj.onion/index.html"]
    es = Elasticsearch(['10.2.0.90:9342'])

    def parse(self,response):
        sel = Selector(response)
        links = response.xpath('//span[@class="col col-6"]/p/a/@href').extract()
        for link in links:
            yield Request(link, callback=self.parse_next)
        urls = response.xpath('//div[@class="box-title col col-6"]//p//small//a/@href').extract()
        for url in urls:
            yield Request(url, callback=self.parse)
     
    def parse_next(self,response):
        sel = Selector(response)
        thread_title = ''.join(sel.xpath('//span[@class="subject"]//text()').extract())
        nodes = response.xpath('//div[contains(@id, "reply_")] | //div[contains(@class, "post reply body-not-empty")] | //div[contains(@class, "post reply has-file body-not-empty")]')
        for node in nodes:
            author =  ''.join(node.xpath('.//span[@class="name"]//text()').extract())
            text = ' '.join(node.xpath('.//div[@class="body"]//text()|.//div[@class="files"]//p//span//..//text() |.//span[@style="white-space: nowrap;"]//text() |.//span[@style="font-weight: bold; white-space: nowrap;"]//text() |.//div[@class="files"]//a[@class="hash_unix hash_h"]//text() |.//span[@class="unimportant"]//text()').extract())
            post_url =  'http://oxwugzccvk3dk6tj.onion'+''.join(node.xpath('.//a[@class="post_no"]/@href').extract_first())
            x = post_url
            post_id =  ''.join(re.findall('#\d+',x)).replace("#","")
            links_ =  node.xpath('.//div[@class="files"]//a[@target="_blank"]//@href | .//div[@class="file"]//img[@class="post-image"]//@src | .//div[@class="video-container"]//a[@target="_blank"]//@href | .//div[@class="video-container"]//a//img[@class="post-image"]//@src | .//div[@class="files"]//a//img[@class="post-image"]//@src | .//div[@class="body"]//p//a[@target="_blank"]//@href | .//div[@class="files"]//a[@target="_blank"]//@href | .//div[@class="files"]//a//img[@class="post-image"]//@src | .//div[@class="files"]//a[@class="file"]//@href | .//div[@class="files"]//a[class="hash_unix hash_h"]//@href | .//span[@class="unimportant"]//a//@href |.//div[@class="files"]//img[@class="post-image"]//@src').extract()
            linkss = []
            for link in links_:
                if 'player.php?v=' in link:
                    link_ =  'http://oxwugzccvk3dk6tj.onion' + link
                    linkss.append(link_)
                elif '//img.youtube.com' in link:
                    link_ = 'https:' + link
                    linkss.append(link_)
                elif '/spoiler.png' in link:
                    link_ = "https://media.8ch.net" + link
                    linkss.append(link_)
                else:
                    linkss.append(link)
            category = ''
            subcategory = ''
            author_link = ''
            all_links = ', '.join(set(linkss))
            publish =  ''.join(node.xpath('.//time//@datetime').extract_first())
            publish_time = datetime.datetime.strptime(publish ,'%Y-%m-%dT%H:%M:%SZ')
            publish_epoch = time.mktime(publish_time.timetuple())*1000
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            doc = {
                    'domain': 'oxwugzccvk3dk6tj.onion',
                         'thread_url': response.url,
                         'category': category,
                         'sub_category': str(subcategory),
                         'thread_title': thread_title,
                         'post_title': '',
                         'author': author,
                         'text': clean_text(text),
                         'author_url': author_link,
                         'post_url': post_url,
                         'post_id': post_id,
                         'publish_time': publish_epoch,
                         'fetch_time': fetch_epoch,
                         'links': all_links
                       }
	    query={"query":{"match":{"_id":hashlib.md5(post_url).hexdigest()}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:	
                self.es.index(index="forum_posts",doc_type='post',id=hashlib.md5(post_url).hexdigest(),body=doc)
	    else:
		data_doc = res['hits']['hits'][0]
		if (doc['links'] != data_doc['_source']['links']) or (doc['text'] != data_doc['_source']['text']):
		    self.es.index(index="forum_posts",doc_type='post',id=hashlib.md5(post_url).hexdigest(),body=doc)
 
