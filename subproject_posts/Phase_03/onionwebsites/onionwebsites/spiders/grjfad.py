from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
from scrapy.utils.response import open_in_browser
import re
import time
from pprint import pprint
from elasticsearch import Elasticsearch
from onionwebsites.utils import *
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

class Grjfad(scrapy.Spider):
    name = "grjfad_posts"
    start_urls = ["http://grjfadb7bweuyauw.onion/index.html"] 


    def parse(self, response):
        nodes = response.xpath('//label')
        nodes1 = response.xpath('//div[@class="message"]')
        nodes2 = response.xpath('//span[@class="reflink"]')
        num = ''.join(response.xpath('//table/tbody/tr/td[3]/form/@action').extract())
        if num:
            num_= "http://grjfadb7bweuyauw.onion/" + num
            yield Request(num_,callback=self.parse)
        count = 0
        for node,node1,node2 in zip(nodes,nodes1,nodes2):
            count +=1
            ord_in_thread = count
            post_title = ''.join(node.xpath('.//span[@class="filetitle"]//text()').extract()) or 'Null'
            author_name = ''.join(node.xpath('.//span[@class="postername"]//text()').extract())
            author_url = ''.join(node.xpath('.//a//@href').extract())
            post_url = ''.join(node2.xpath('.//a[contains(text(),"No.")]//@href').extract())
            if post_url:
                post_url = 'http://grjfadb7bweuyauw.onion/' + post_url
            post_id = ''.join(re.findall('#\d+',post_url)).replace('#','')
            text = ' '.join(node1.xpath('.//text()').extract())
            date = ''.join(node.xpath('./text()').extract()).strip()
            publish = ''.join(re.findall('\d+/\d+/\d+',date))
            publishdate = datetime.datetime.strptime(publish,'%y/%m/%d')
            publish_epoch =time.mktime(publishdate.timetuple())*1000
            if publish_epoch:
                month_year = get_index(publish_epoch)
            else:
                import pdb;pdb.set_trace()
            fetchTime = int(datetime.datetime.now().strftime("%s")) * 1000
            doc = {}
            author_data = {
                'name':author_name,
                'url':author_url
                }
            post = {
                'cache_link':'',
                'author':json.dumps(author_data),
                'section':'Null',
                'language':'english',
                'require_login':'false',
                'sub_section':'Null',
                'sub_section_url':'Null',
                'post_id':post_id,
                'post_title':post_title,
                'ord_in_thread':ord_in_thread,
                'post_url':post_url,
                'post_text':text.replace('\n', ''),
                'thread_title':'Null',
                'thread_url':response.request.url
                }
            doc.update({
                'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                'hostname':'grjfadb7bweuyauw.onion',
                'domain':'grjfadb7bweuyauw.onion',
                'sub_type':'darkweb',
                'type':'forum',
                'author':json.dumps(author_data),
                'title':'Null',
                'text':text.replace('\n', ''),
                'url':post_url,
                'original_url':post_url,
                'fetch_time':fetchTime,
                'publish_time':publish_epoch,
                'link.url':'Null',
                'post':post
                })
            sk = md5_val(post_id)
            es.index(index=month_year, doc_type='post', id=sk, body=doc)
