from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
from scrapy.utils.response import open_in_browser
import re
import time
from pprint import pprint
from elasticsearch import Elasticsearch
import hashlib
es = Elasticsearch(['10.2.0.90:9342'])

def clean_text(input_text):
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', input_text)
    text = re.sub(r'(\n\s*)', '\n', text)
    text = re.sub('\s\s+', ' ', text)
    return text

def time_to_epoch(str_of_time, str_of_patter):
    try:time_in_epoch = (int(time.mktime(time.strptime(str_of_time, str_of_patter))) - time.timezone) * 1000
    except:time_in_epoch = False
    return time_in_epoch

class M6om(scrapy.Spider):
    name = "4m6omb3"

    def start_requests(self):
        last_url = 'http://4m6omb3gmrmnwzxi.onion/last.php'
        top_url = 'http://4m6omb3gmrmnwzxi.onion/top.php'

        yield Request(last_url,callback = self.pages)
        yield Request(top_url,callback = self.pages)

    def pages(self,response):
        next_page = ''.join(set(response.xpath('//a[contains(text(),"Next -->")]/@href').extract()))
        if next_page:
            next_page = 'http://4m6omb3gmrmnwzxi.onion'+next_page
            yield Request(next_page,callback = self.pages)
        urls = response.xpath('//a[contains(@href,"show.php?md5")]/@href').extract()
        for url in urls:
            yield Request('http://4m6omb3gmrmnwzxi.onion/'+url,callback = self.parse_meta)


    def parse_meta(self,response):
        thread_title = response.xpath('//p[@style="font-size: 0.7em; margin-top: -30px; margin-bottom: 5px;"]/preceding-sibling::h3//text()').extract()[0]
        post_title = response.xpath('//p[@style="font-size: 0.7em; margin-top: -30px; margin-bottom: 5px;"]/preceding-sibling::h3//text()').extract()[-1]
        post_id = re.sub('(.*)md5=','',response.request.url)
        post_url = response.request.url
        auth = ''.join(response.xpath('//p[@style="font-size: 0.7em; margin-top: -30px; margin-bottom: 5px;"]/text()').extract())
        date_ = re.sub('\w+, ','',auth)
        author = re.sub(', \w+ \d+, \d+ - \d+:\d+ \w\w UTC','',auth).strip()
        if date_:
            publish_epoch = time_to_epoch(date_,'%b %Y - %I:%M %p UTC')*1000
        else:
            publish_epoch = 0
        text = ''.join(response.xpath('//textarea[@cols="110"]/text()').extract())
        links = str(re.findall("(?P<url>https?://[^\s]+)", text)) #re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', text))
        commants = []
        c_nodes = response.xpath('//textarea[(@cols="80") and not(contains(@maxlength,"2048"))]')
        for i ,node in enumerate(c_nodes):
            try:
                commant_data = response.xpath('//textarea[@cols="80"]/preceding-sibling::span[not(contains(@style,"float: right;"))]//text()').extract()[i]
                commant_text = ''.join(response.xpath('//textarea[(@cols="80") and not(contains(@maxlength,"2048"))]')[i].xpath('.//text()').extract())
                commant_links = re.findall("(?P<url>https?://[^\s]+)", commant_text)
                #re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', commant_text)
                commant_author = re.sub(', \w+ \d+, \d+ - \d+:\d+ \w\w UTC','',commant_data).strip()
                commant_date = re.sub('\w+, ','',commant_data)
                commant_date = time_to_epoch(commant_date.split(),'%b %Y - %I:%M %p UTC')*1000
                commant_doc = {
                    'comment_author': commant_author,
                    'comment_text':clean_text(commant_text),
                    'comment_date':commant_date,
                    'comment_links': str(commant_links)
                    }
                commants.append(commant_doc)
            except:
                pass


        doc = {
                'domain': '4m6omb3gmrmnwzxi.onion',
                'thread_url': '',
                'thread_title': thread_title,
                'category': '',
                'sub_category': '',
                'post_title': post_title,
                'post_id': post_id,
                'post_url': post_url,
                'publish_time': publish_epoch,
                'fetch_time': round((time.time()- time.timezone)*1000),
                'author': author,
                'text': clean_text(text),
                'links': links,
                'comment': str(commants)
            }
	sk = hashlib.md5(post_url).hexdigest()
	query={"query":{"match":{"_id":sk}}}
        res = es.search(body=query)
        if res['hits']['hits'] == []:
            es.index(index="forum_posts", doc_type='post', id=sk, body=doc)

