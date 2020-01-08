import scrapy 
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
from datetime import date, timedelta
import time
import re
import hashlib
from elasticsearch import Elasticsearch
from onionwebsites.utils import *



class Zzq7gpluliw6iq7l(Spider):
    name = "zzq7gpluliw6iq7l"
    start_urls = ['http://zzq7gpluliw6iq7l.onion/']
    es = Elasticsearch(['10.2.0.90:9342'])

    def parse(self, response):
        main_urls = response.xpath('//table[@class="threadlist"]//td //a/@href').extract()
        for url in main_urls:    
            url = "http://zzq7gpluliw6iq7l.onion/" + url
            yield Request(url, callback = self.parse_next)

    def parse_next(self, response):
        thread_title = ''.join(response.xpath('//div[@class="centeralign"]//h2/text()').extract()).strip() or 'Null'
        body = ''.join(response.xpath('//div[@class="threadshow"]//hr  | //div[@class="threadshow"]//text()').extract()).split('<hr>--')
        all_posts = []
	ord_in_thread = 0
        for i in body:
            i = i.replace('<hr>','')
            if not i: 
                continue
	    ord_in_thread = ord_in_thread+1
            author = re.sub(' posted (.*)','',''.join(re.findall('\w+ posted \d+ days \d+ hours ago:',i)+ re.findall('\w+ posted \d+ days \d+ minutes ago:',i)))
            date = ''.join(re.findall('posted \d+ \w+ \d+ \w+ \w+',i))
            days_hours = re.findall('\d+',date)
            try:
                x_days = days_hours[0]
                y_hours = days_hours[1]
            except:
		pass
            publish = datetime.datetime.now() - timedelta(days=int(x_days),hours=int(y_hours))
            publish_epoch = time.mktime(publish.timetuple())*1000
	    if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
		    month_year = get_index(publish_epoch)
	    else:
		import pdb;pdb.set_trace()

            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
            text = re.sub('(.*) posted \d+ days \d+ hours ago:','',i)
            text = re.sub('(.*) posted \d+ days \d+ minutes ago:','',text)
            post_text = text.replace('\r','').replace('\n','')
            doc = {}
	    author_data = {
		'name':author,
		'url':'Null'
		}
	    post = {
		'cache_link':'',
		'author':json.dumps(author_data),
		'section':'Null',
		'language':'english',
		'require_login':'false',
		'sub_section':'Null',
		'sub_section_url':'Null',
		'post_id':'Null',
		'post_title':'Null',
		'ord_in_thread':ord_in_thread,
		'post_url':'Null',
		'post_text':post_text.replace('\n', ''),
		'thread_title':thread_title,
		'thread_url':response.request.url
		}
            doc.update({
		'record_id':'Null',
		'hostname':'zzq7gpluliw6iq7l.onion',
                'domain':'zzq7gpluliw6iq7l.onion',
		'sub_type':'darkweb',
		'type':'forum',
		'author':json.dumps(author_data),
		'title':thread_title,
		'text':post_text.replace('\n', ''),
		'url':'Null',
		'original_url':'Null',
		'fetch_time':fetch_time,
		'publish_time':publish_epoch,
		'link.url':'Null',
		'post':post
		})
            sk = hashlib.md5(post_text).hexdigest() 
	    #query={"query":{"match":{"_id":sk}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
            self.es.index(index=month_year, doc_type='post', id=(sk), body=doc)



