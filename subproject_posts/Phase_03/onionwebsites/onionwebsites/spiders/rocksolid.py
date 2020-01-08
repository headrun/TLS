from onionwebsites.utils import *


class Rocksolid(Spider):
    name = "rocksolid"
    handle_httpstatus_list = [404]
    start_urls = ['http://bchz4vggexx63qvy.onion/rocksolid/']
    #Proxy_Rotation_Middleware
    custom_settings = {
    	'COOKIES_ENABLED':False,
	'DOWNLOAD_DELAY':6,
    #'DOWNLOADER_MIDDLEWARES':{ 'onionwebsites.middlewares.Proxy_Rotation_Middleware': 440}
        }

    def parse(self, response):
        urls = response.xpath('//div[@class="np_index_group"]//a[@target="content"]/@href').extract() 
        for url in urls:
	    #i 'nodes.help' not in url:
            url = "http://bchz4vggexx63qvy.onion/rocksolid/" + url
            yield Request(url, callback=self.parse_next)

    def parse_next(self, response):
        main_links = response.xpath('//span[@class="np_thread_line_text"]//a/@href').extract()
        for link in main_links:
            link = "http://bchz4vggexx63qvy.onion/rocksolid/" + link
            yield Request(link, callback=self.parse_data)
        page_nav = 'http://bchz4vggexx63qvy.onion/rocksolid/' + ''.join(response.xpath('//td[@class="np_pages"]//a[@class="np_pages_unselected"]/@href').extract())
        if page_nav:
            yield Request(page_nav, callback=self.parse_next)

    def parse_data(self, response):
        domain = 'bchz4vggexx63qvy.onion'
        title = ''.join(response.xpath('//h1[@class="np_article_headline"]/text()').extract())
        category = re.sub(' / (.*)', '', title)
	if category == '':
	    category = 'Null'
        thread_title = re.sub('(.*) / ', '', title)
	if thread_title == '':
	    thread_title = 'Null'
        thread_url = response.url
        nodes = response.xpath('//div[@class="np_article_header"]')
        nodes1 = response.xpath('//div[@class="np_article_body"]')
	ord_in_thread = 0
        for node, node1 in zip(nodes, nodes1):
	 	ord_in_thread = ord_in_thread+1
                post_title = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Subject:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"Subject:")]]]').extract()).strip() or 'Null'
                author = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"From:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"From:")]]]').extract()).replace('(', '').replace(')', '').strip() or 'Null'
                organization = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Organization:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"Organization:")]]]').extract()).strip() or 'Null'
                try:
                    date1 = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Date:")]]').extract()).replace('\n', '')
                    date = ''.join(re.findall('\d+ \w+ \d+ \d+:\d+',date1))
                    publish = datetime.datetime.strptime(date, '%d %b %Y %H:%M')
                    publish_epoch = time.mktime(publish.timetuple()) * 1000
                except:
                    date = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Date:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"Date:")]]]').extract()).replace('\n', '')
                    publish = datetime.datetime.strptime(date.strip(), '%b %d %Y %H:%M:%S')
                    publish_epoch = time.mktime(publish.timetuple()) * 1000
                if publish_epoch == False:
                   import pdb;pdb.set_trace()
	        if publish_epoch:
                    year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                    if year > '2011':
		        month_year = get_index(publish_epoch)
                    else:
                        continue
		else:
		    import pdb;pdb.set_trace()

                fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
                post_text = ' '.join(node1.xpath('.//text() | .//blockquote[@class="np_article_quote"]/@class').extract()).replace('\n', '').replace('np_article_quote', ' Quote: ') or 'Null'
		author_data = {
			'name':author,
			'url':'Null'
			}
		post = {
			'cache_link':'',
			'author':json.dumps(author_data),
			'section':category,
			'language':'english',
			'require_login':'false',
			'sub_section':'Null',
			'sub_section_url':'Null',
			'post_id':'Null',
			'post_title':post_title,
			'ord_in_thread':ord_in_thread,
			'post_url':'Null',
			'post_text':post_text,
			'thread_title':thread_title,
			'thread_url':thread_url
			}
                json_posts = { 'record_id':'Null',
			       'hostname':'bchz4vggexx63qvy.onion',
			       'domain': domain,
			       'sub_type':'darkweb',
			       'type':'forum',
			       'author':json.dumps(author_data),
			       'title':thread_title,
			       'text':post_text,
			       'url':'Null',
			       'original_url':'Null',
			       'fetch_time':fetch_time,
			       'publish_time':publish_epoch,
			       'link.url':'Null',
			       'post':post,
			       'organization':organization
			       }
                sk = md5_val(domain + post_text.encode('utf8'))
		#query={"query":{"match":{"_id":sk}}}
            	#res = es.search(body=query)
            	#if res['hits']['hits'] == []:
		es.index(index=month_year, doc_type='post', id=sk, body=json_posts)
                #doc_to_es(id=sk,body=json_posts,doc_type='post')
		
        nav = response.xpath('//span[@class="np_pages_selected"]//following-sibling::a[@class="np_pages_unselected"]/@href').extract_first()
        if nav:
            p_nav = 'http://bchz4vggexx63qvy.onion/rocksolid/' + nav
            yield Request(p_nav, callback=self.parse_data)
