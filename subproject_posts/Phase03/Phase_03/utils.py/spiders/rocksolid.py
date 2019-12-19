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
        thread_title = re.sub('(.*) / ', '', title)
        thread_url = response.url
        nodes = response.xpath('//div[@class="np_article_header"]')
        nodes1 = response.xpath('//div[@class="np_article_body"]')
        for node, node1 in zip(nodes, nodes1):
                post_title = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Subject:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"Subject:")]]]').extract()).strip()
                author = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"From:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"From:")]]]').extract()).replace('(', '').replace(')', '').strip()
                organization = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Organization:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"Organization:")]]]').extract()).strip()
                try:
                    date = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Date:")]]').extract()).replace('\n', '')
                    publish = datetime.datetime.strptime(date.strip(), '%b %d %Y %H:%M:%S')
                    publish_epoch = time.mktime(publish.timetuple()) * 1000
                except:
                    try:
                        date = ''.join(node.xpath('.//text()[preceding-sibling::b[contains(text(),"Date:")]][following-sibling::b[1][preceding-sibling::b[1][contains(text(),"Date:")]]]').extract()).replace('\n', '')
                        publish = datetime.datetime.strptime(date.strip(), '%b %d %Y %H:%M:%S')
                        publish_epoch = time.mktime(publish.timetuple()) * 1000
                    except:
                        import pdb;pdb.set_trace()
                fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
                post_text = '\n'.join(node1.xpath('.//text() | .//blockquote[@class="np_article_quote"]/@class').extract()).replace('\n', '').replace('np_article_quote', ' Quote: ')
                json_posts = { 'domain': domain,
                               'category': category,
                               'sub_category': '',
                               'thread_title': thread_title,
                               'post_title': post_title,
                               'thread_url': thread_url,
                               'post_id': '',
                               'post_url': '',
                               'publish_time': publish_epoch,
                               'fetch_time': fetch_time,
                               'author': author,
                               'author_url': '',
                               'text': post_text,
                               'links': [],
                               'organization': organization,
                               }
                sk = md5_val(domain + post_text.encode('utf8'))
                doc_to_es(id=sk,body=json_posts,doc_type='post')
        nav = response.xpath('//span[@class="np_pages_selected"]//following-sibling::a[@class="np_pages_unselected"]/@href').extract_first()
        if nav:
            p_nav = 'http://bchz4vggexx63qvy.onion/rocksolid/' + nav
            yield Request(p_nav, callback=self.parse_data)
