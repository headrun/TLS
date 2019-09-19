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
        thread_title = ''.join(response.xpath('//div[@class="centeralign"]//h2/text()').extract()).strip()
        body = ''.join(response.xpath('//div[@class="threadshow"]//hr  | //div[@class="threadshow"]//text()').extract()).split('<hr>--')
        all_posts = []
        for i in body:
            i = i.replace('<hr>','')
            if not i: 
                continue
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
            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
            text = re.sub('(.*) posted \d+ days \d+ hours ago:','',i)
            text = re.sub('(.*) posted \d+ days \d+ minutes ago:','',text)
            post_text = text.replace('\r','').replace('\n','')
            doc = {}
            doc.update({
                'domain':'zzq7gpluliw6iq7l.onion',
                'thread_url':response.request.url,
                'thread_title':thread_title,
                'author':author,
                'author_url':'',
                'publish_epoch':publish_epoch,
                'fetch_time':fetch_time,
                'post_text':post_text,
                'fetch_time':fetch_time,
                'post_id':'',
                'post_url':'',
                'category':'',
                'sub_category':'',
                'all_links': '[]'
            })
            all_posts.append(doc)
            sk = hashlib.md5(post_text).hexdigest() 
	    query={"query":{"match":{"_id":sk}}}
            res = self.es.search(body=query)
            if res['hits']['hits'] == []:
                self.es.index(index="forum_posts",doc_type='post',id=(sk),body=doc)
