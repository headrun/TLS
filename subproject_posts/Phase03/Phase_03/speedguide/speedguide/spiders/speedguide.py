import scrapy
from elasticsearch import Elasticsearch
#es = Elasticsearch(['127.0.01:9222'])
import re, json
import hashlib


class speedguide(scrapy.Spider):
    name = "speedguide"
    start_urls = ['https://www.speedguide.net/port.php?port=0']

    def parse(self,response):
        posts = response.xpath('//table[@class="port-outer"]//table[@class="port"]//tr[@class="port-outer"] | //table[@class="port-outer"]//table[@class="port"]//tr[@class="port"]')
        docs = []
        for post in posts:
            doc = {
                'port': ''.join(post.xpath('.//td[1]//text()').extract()).strip(),
                'protocal': ''.join(post.xpath('.//td[2]//text()').extract()).strip(),
                'service': ''.join(post.xpath('.//td[3]//text()').extract()).strip(),
                'details': ''.join(post.xpath('.//td[4]//text()').extract()).strip(),
                'details_links': post.xpath('.//td[4]//a[@target="_blank"]/@href').extract(),
                'source':''.join(post.xpath('.//td[5]//text()').extract()).strip()
                }
            docs.append(doc)
        port = hashlib.md5(re.sub('(.*)port=','',response.url)).hexdigest()
        json_doc = {
                'post_number': re.sub('(.*)port=','',response.url),
                'assignments_and_vulnerabilities':str(docs)
                }
        #es.index(index="speedguide", doc_type='post', id=port, body=json_doc)
        next_page = ''.join(response.xpath('//a[@class="button" and contains(@title,"next port")]/@href').extract())
        if next_page:
            n_url = 'https://www.speedguide.net'+next_page
            yield scrapy.Request(n_url,callback = self.parse)

