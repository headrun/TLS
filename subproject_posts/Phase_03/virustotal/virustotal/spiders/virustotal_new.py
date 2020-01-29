import json
import re
import unicodedata
import time
from scrapy.http import Request
import MySQLdb
import logging
from time import strftime
import datetime
import os
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from urllib import urlencode
from scrapy.http import FormRequest
from elasticsearch import Elasticsearch
from datetime import date, timedelta,datetime
import hashlib
import scrapy
from scrapy import Spider
from scrapy.selector import Selector
from scrapy.http import FormRequest
from urlparse import urljoin
es = Elasticsearch(['10.2.0.90:9342'])
from virustotal.utils import *

class Virus(Spider):
    name = 'virustotal_test'
    start_urls = ['https://www.virustotal.com/gui/latest-comments']

    def __init__(self):
        self.conn = MySQLdb.connect(db= "virustotal", host = "localhost", user="tls_dev", passwd = "hdrn!", use_unicode=True,charset="utf8mb4")
        self.cursor=self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        headers = {
       'x-app-hostname': 'https://www.virustotal.com/gui/',
       'x-app-version': '20190814t121927',
       'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.100 Safari/537.36',
       'accept': 'application/json',
       'referer': 'https://www.virustotal.com/',
       'authority': 'www.virustotal.com'

}

        params = (
       ('relationships', 'author,item'),
       ('limit', '10'),
       ('filter', 'tag:"_:web"'),
)
        url = "https://www.virustotal.com/ui/comments?" + urlencode(params)
        yield FormRequest(url, callback=self.parse_next,headers=headers)

    def parse_next(self,response):
        hostname = "www.virustotal.com"
        domain = "virustotal.com"
        da = json.loads(response.body)
        data1 = da.get('data')
        for data2 in data1:
            author_name = data2.get('relationships',{}).get('author',{}).get('data',{}).get('id','')
            author_url = "https://www.virustotal.com/gui/user/" + author_name + "/comments"
            publish_time = data2.get('attributes',{}).get('date','')
            if publish_time:
                year = time.strftime("%Y", time.localtime(int(publish_time)))
                if year > '2011':
                    month_year = time.strftime("%m_%Y", time.localtime(publish_time))
                else:
                    continue
            else:
                import pdb;pdb.set_trace()
            hashtags = ",".join(data2.get('attributes',{}).get('tags',''))
            text = data2.get('attributes',{}).get('text','')
            abuse = data2.get('attributes',{}).get('votes',{}).get('abuse','')
            negative = data2.get('attributes',{}).get('votes',{}).get('negative','')
            positive = data2.get('attributes',{}).get('votes',{}).get('positive','')
            indicator_ = data2.get('relationships',{}).get('item',{}).get('data',{}).get('id','')
            indicator_type = data2.get('relationships',{}).get('item',{}).get('data',{}).get('type','')
            ioa_ = {"community": {"virustotal": {"votes" : {"abuse":abuse, "positive":positive, "negative":negative },"indicator":indicator_, "indicator_type":indicator_type}}}
            if "ip_address" in indicator_type:
               indicator_type = "ip-address"
            else:
                indicator_type = indicator_type
            id_ = data2.get('id','')
            typ = "community"
            sub_type = "virustotal"
            title = "Virustotal community comments for " + indicator_
            original_url = "https://www.virustotal.com/gui/" + indicator_type + "/" + indicator_ + "/community"
            url = "http://virustotal.com/gui/" + indicator_type + "/" + indicator_ + "/community"
            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
            author_data= {
                   'name':author_name,
                   'url':author_url
                   }
            doc = {}
            doc.update({
                'hostname':hostname,
                'domain':domain,
                'author':json.dumps(author_data),
                'hashtags':hashtags,
                'text':text.replace('\n\r\t','').replace('\n',' '),
                'publish_time':publish_time,
                'fetch_time':fetch_time,
                'ioa':json.dumps(ioa_),
                'original_url':original_url,
                'url':url,
                'id':id_,
                'type':typ,
                'sub_type':sub_type,
                'title':title
               })
            #sk = md5_val(indicator_)
            sk = indicator_
            es.index(index= "forum_posts_"+month_year,doc_type='post',id=sk,body = doc)

        next_page = da.get('links',{}).get('next','')
        if next_page:
            time.sleep(15)
            yield Request(next_page,callback = self.parse_next)
