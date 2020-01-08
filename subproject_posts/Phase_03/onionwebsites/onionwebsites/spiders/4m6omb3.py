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
	ord_in_thread = 0
	ord_in_thread = ord_in_thread+1
        thread_title = response.xpath('//p[@style="font-size: 0.7em; margin-top: -30px; margin-bottom: 5px;"]/preceding-sibling::h3//text()').extract()[0] or 'Null'
        try:
            thread_title = thread_title
        except:
            pass
        post_title = response.xpath('//p[@style="font-size: 0.7em; margin-top: -30px; margin-bottom: 5px;"]/preceding-sibling::h3//text()').extract()[-1] or 'Null'
        post_id = re.sub('(.*)php?','',response.request.url).replace('?', '').replace('md5=','')
        post_url = response.request.url
        auth = ''.join(response.xpath('//p[@style="font-size: 0.7em; margin-top: -30px; margin-bottom: 5px;"]/text()').extract())
        date_ = re.sub('\w+, ','',auth)
        author = re.sub(', \w+ \d+, \d+ - \d+:\d+ \w\w UTC','',auth).strip()
	if author == '':
	    author = 'Null'
        '''if date_:
            publish_epoch = time_to_epoch(date_,'%b %Y - %I:%M %p UTC')*1000
        else:
            publish_epoch = 0'''
        publish_epoch = time_to_epoch(date_,'%B %Y - %I:%M %p UTC')
        if publish_epoch ==False:
            import pdb;pdb.set_trace()
	if publish_epoch:
            year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
            if year > '2011':
	        month_year = get_index(publish_epoch)
            else:
                return None
	else:
	    import pdb;pdb.set_trace()

        text = ''.join(response.xpath('//textarea[@cols="110"]/text()').extract()).replace('\n', '').strip() or 'Null'
        #links = str(re.findall("(?P<url>https?://[^\s]+)", text)) #re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', text))
	all_links = ', '.join(re.findall("(?P<url>https?://[^\s]+)", text))
	if all_links == '':
	    all_links = 'Null'
        commants = []
        c_nodes = response.xpath('//textarea[(@cols="80") and not(contains(@maxlength,"2048"))]')
        for i ,node in enumerate(c_nodes):
            try:
                commant_data = response.xpath('//textarea[@cols="80"]/preceding-sibling::span[not(contains(@style,"float: right;"))]//text()').extract()[i] or 'Null'
                commant_text = ''.join(response.xpath('//textarea[(@cols="80") and not(contains(@maxlength,"2048"))]')[i].xpath('.//text()').extract()) or 'Null'
                commant_link = re.findall("(?P<url>https?://[^\s]+)", commant_text)
		commant_links = ', '.join(commant_link)
		if commant_links == '':
		    commant_links = 'Null'
		if commant_link == []:
		    commant_links = 'Null'
                #re.findall('https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', commant_text)
                commant_author = re.sub(', \w+ \d+, \d+ - \d+:\d+ \w\w UTC','',commant_data).strip()
	  	if commant_author == '':
		    commant_author = 'Null'
                commant_date = re.sub('\w+, ','',commant_data)
                commant_date = time_to_epoch(commant_date.split(),'%b %Y - %I:%M %p UTC')*1000
                commant_doc = {
                    'comment_author': commant_author,
                    'comment_text':clean_text(commant_text).replace('\n', ''),
                    'comment_date':commant_date,
                    'comment_links': commant_links
                    }
                commants.append(commant_doc)
            except:
                pass

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
		'post_id':post_id,
		'post_title':post_title,
		'ord_in_thread':ord_in_thread,
		'post_url':post_url,
		'post_text':clean_text(text),
		'thread_title':thread_title,
		'thread_url':post_url,
		}
        doc = {
		'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
		'hostname':'4m6omb3gmrmnwzxi.onion',
                'domain': '4m6omb3gmrmnwzxi.onion',
		'sub_type':'darkweb',
		'type':'forum',
		'author':json.dumps(author_data),
		'title':thread_title,
		'text':clean_text(text),
		'url':post_url,
		'original_url':post_url,
	 	'fetch_time':round((time.time()- time.timezone)*1000),
		'publish_time':publish_epoch,
		'link.url':all_links,
		'post':post,
		'comment':json.dumps(commants)
		}
	sk = md5_val(post_url)
	#query={"query":{"match":{"_id":sk}}}
        #res = es.search(body=query)
        #if res['hits']['hits'] == []:
        es.index(index=month_year, doc_type='post', id=sk, body=doc, request_timeout=30)

