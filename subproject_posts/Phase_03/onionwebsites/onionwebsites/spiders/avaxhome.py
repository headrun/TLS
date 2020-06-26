from onionwebsites.utils import *

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
        links = sel.xpath('//div[@class="col-lg-12 article"]//h1//a[@class="title-link"]//@href').extract()
        for link in links:
            if 'http' not in link:
                link = "http://avaxhome5lcpcok5.onion/software/" + link
                yield Request(link, callback=self.parse_next)
        page_urls =  response.xpath('//div//li//a[@class="next"]//@href').extract_first()
        if page_urls:
            page_url = "http://avaxhome5lcpcok5.onion" + page_urls
            yield Request(page_url,callback=self.parse)
               

    def parse_next(self,response):
        sel = Selector(response)
        nodes = sel.xpath('//div[@class="col-md-12 article"]//div[@class="col-lg-12"]')
	ord_in_thread = 0
        for node in nodes:
	    ord_in_thread = ord_in_thread+1
            author = ''.join(node.xpath('.//div[@class="author visible-lg-inline-block visible-md-inline-block visible-sm-block visible-xs-block"]//a//text()').extract()) or 'Null'
            authorurl = ''.join(node.xpath('.//div[@class="author visible-lg-inline-block visible-md-inline-block visible-sm-block visible-xs-block"]//a//@href').extract())
	    if authorurl:

		author_url = 'http://avaxhome5lcpcok5.onion'+ authorurl
	    if author_url == '':
		author_url = 'Null'
            title = ''.join(response.xpath('//div[@class="row"]//h1[@class="title-link"]//text()').extract()).strip() or 'Null'
            text = clean_text('\n'.join(node.xpath('.//div[@class="text"]//text() | //div[@class="text download_links"]//text()').extract()).replace("\n","").replace("\t","").strip().replace('     Close   ','').replace(u'\xd7','')) or 'Null'
            image =   node.xpath('.//div[@class="text-center"]//a//img/@data-src').extract_first()
            links_ =  response.xpath('.//div[@class="text"]/a/@href | .//div[@class="text-center"]//a//img//@src | //div[@class="text"]//a[@target="_blank"]/@href | //div[@class="text"]//iframe[@class="embed-responsive-item"]/@src | //div[@class="text download_links"]//@href').extract()
            links = []
            for link in set(links_):
                if 'http' not in link: link = 'http://avaxhome5lcpcok5.onion'+link
                links.append(link)
	    all_links = ', '.join(links)
	    if all_links == '':
	        all_links = 'Null'
	    if links_ == []:
		all_links = 'Null'
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
	    if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
		    month_year = get_index(publish_epoch)
                else:
                    continue
	    else:
		import pdb;pdb.set_trace()

            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
	    author_data = {
			'name':author,
		 	'url':author_url
			}
	    post_url = response.request.url
	    post = {
		    'cache_link':'',
		    'author':json.dumps(author_data),
		    'section':'Null',
		    'language':'english',
		    'require_login':'false',
		    'sub_section':'Null',
		    'sub_section_url':'Null',
		    'post_id':'Null',
		    'post_title':title,
		    'ord_in_thread':ord_in_thread,
		    'post_url':post_url,
		    'post_text':text,
		    'thread_title':'Null',
		    'thread_url':post_url
		   }
	    doc = {
		    'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
		    'hostname':'avaxhome5lcpcok5.onion',
		    'domain':'avaxhome5lcpcok5.onion',
		    'sub_type':'darkweb',
		    'type':'forum',
		    'author':json.dumps(author_data),
		    'title':'Null',
		    'text':text,
		    'url':post_url,
		    'original_url':post_url,
		    'fetch_time':fetch_epoch,
		    'publish_time':publish_epoch,
		    'link.url':all_links,
		    'post':post
		    }
	    sk = hashlib.md5(post_url).hexdigest() 
            es.index(index=month_year, doc_type='post', id=sk, body=doc)

