import scrapy
from scrapy.selector import Selector
from scrapy.http import Request
import time
import datetime
from elasticsearch import Elasticsearch
es = Elasticsearch(['10.2.0.90:9342'])
import hashlib
import re

def clean_text(input_text):
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', input_text)
    text = re.sub(r'(\n\s*)', '\n', text)
    text = re.sub('\s\s+', ' ', text)
    return text

class Avax(scrapy.Spider):
    name = "avaxhome"
    start_urls = ["http://avaxhome5lcpcok5.onion/software"]

    def parse(self, response):
        sel = Selector(response)
        links = sel.xpath('//div[@class="col-lg-12"]//h1//a//@href').extract()
        for link in links:
            if 'http' not in link:
                link = "http://avaxhome5lcpcok5.onion/software/" + link
                yield Request(link, callback=self.parse_next)
        page_urls =  response.xpath('//div//li//a[@class="next"]//@href').extract()
        for url in page_urls:
            if 'http' not in url:
                url = "http://avaxhome5lcpcok5.onion/" + url
                yield Request(url,callback=self.parse)

    def parse_next(self,response):
        sel = Selector(response)
        nodes = sel.xpath('//div[@class="col-md-12 article"]//div[@class="col-lg-12"]')
        for node in nodes:
            author = ''.join(node.xpath('.//div[@class="author visible-lg-inline-block visible-md-inline-block visible-sm-block visible-xs-block"]//a//text()').extract())
            author_url = ''.join(node.xpath('.//div[@class="author visible-lg-inline-block visible-md-inline-block visible-sm-block visible-xs-block"]//a//@href').extract())
            title = ''.join(response.xpath('//div[@class="row"]//h1[@class="title-link"]//text()').extract()).strip()
            text = clean_text('\n'.join(node.xpath('.//div[@class="text"]//text() | //div[@class="text download_links"]//text()').extract()).replace("\n","").replace("\t","").strip().replace('     Close   ','').replace(u'\xd7',''))
            image =   ''.join(node.xpath('.//div[@class="text-center"]//a//img//@src').extract_first())
            links_ =  response.xpath('.//div[@class="text"]/a/@href | .//div[@class="text-center"]//a//img//@src | //div[@class="text"]//a[@target="_blank"]/@href | //div[@class="text"]//iframe[@class="embed-responsive-item"]/@src | //div[@class="text download_links"]//@href').extract()
            links = []
            for link in set(links_):
                if 'http' not in link: link = 'http://avaxhome5lcpcok5.onion'+link
                links.append(link)
            publish =  ''.join(node.xpath('.//div[@class="post-date visible-lg-inline-block visible-md-inline-block visible-sm-block visible-xs-block"]/text()').extract()).strip()
            try:
                try:
                    try:
                        publish_time = datetime.datetime.strptime(publish, '%d %B %Y %I:%M:%S')
                        publish_epoch = time.mktime(publish_time.timetuple())*1000
                    except:
                        publish_time = datetime.datetime.strptime(publish ,'%d %b %Y %I:%M:%S')
                        publish_epoch = time.mktime(publish_time.timetuple())*1000
                except:
                    publish_time = datetime.datetime.strptime(publish, '%d %B %Y %H:%M:%S')
                    publish_epoch = time.mktime(publish_time.timetuple())*1000
            except:
                publish_time = datetime.datetime.strptime(publish ,'%d %b %Y %H:%M:%S')
                publish_epoch = time.mktime(publish_time.timetuple())*1000
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000

            doc = {
                    'author':author,
                    'author_url':'http://avaxhome5lcpcok5.onion'+author_url,
                    'post_title':title,
                    'post_url':response.url,
                    'links':', '.join(links),
                    'publish_time':publish_epoch,
                    'fetch_time':fetch_epoch,
                    'domain':'avaxhome5lcpcok5.onion',
                    'category': '',
                    'sub_category': '',
                    'thread_title': '',
                    'thread_url':'',
                    'thread_title':'',
                    'text':text
                    }
	    sk = hashlib.md5(response.url).hexdigest() 
	    query={"query":{"match":{"_id":sk}}}
            res = es.search(body=query)
            if res['hits']['hits'] == []:
                es.index(index="forum_posts", doc_type='post', id=sk, body=doc)
	    else:
		data_doc = res['hits']['hits'][0]
                if (doc['links'] != data_doc['_source']['links']) or (doc['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=doc)

