from mango.utils import *


class Fuckav1(Spider):
    name = "fuckav_posts"
    start_urls=["https://fuckav.ru/"]
    custom_settings = {
        'COOKIES_ENABLED':True
        }


    def __init__(self):
        self.conn = MySQLdb.connect(
            db='posts',
            host='localhost',
            user=DATABASE_USER,
            passwd=DATABASE_PASS,
            use_unicode='True',
            charset='utf8mb4')
        self.cursor = self.conn.cursor()
        self.author_crawl = generate_upsert_query_authors_crawl('fuckav')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        cooks = re.sub('";documen(.*)','',re.sub('(.*)cookie="','',''.join(response.xpath('//script//text()').extract()))).split('=')
        self.cook = {cooks[0]: cooks[1]}
        yield scrapy.Request('https://fuckav.ru/',callback=self.meta_data, cookies=self.cook, dont_filter=True)

    def meta_data(self, response):
        url_que = "select distinct(post_url) from fuckav_post_crawl"
        self.cursor.execute(url_que)
        post_url = self.cursor.fetchall()
        for url in post_url:
            #cookies= self.cook,dont_filter=True)
            yield Request(url[0], callback=self.parse_meta)

    def parse_meta(self, response):
        login = ''.join(response.xpath('//td[@align="right"]/input[contains(@accesskey,"r")]/@type').extract())
        if login:
            return
	#import pdb;pdb.set_trace()
        category = ''.join(response.xpath('//table[@class="tborder"]//td[@class="alt1"]//span[@itemprop="title"]/text()')[1].extract()) or 'Null'
        sub_category = ''.join(response.xpath('//table[@class="tborder"]//td[@class="alt1"]//span[@itemprop="title"]/text()').extract()[2]).encode('utf8') or 'Null'
	sub_category_url = response.xpath('//span[@class = "navbar"]//a//@href')[2].extract() or 'Null'
        thread_title = ''.join(response.xpath('//table[@class="tborder"]//td[@class="alt1"]//td[@class="navbar"]//text()').extract()).strip()or 'Null'
        thread_url = response.request.url.split('&')[0] or 'Null'
        nodes = response.xpath('//table[contains(@id,"post")]')
        for node in nodes:
            json_posts = {}
            post_title = ''.join(node.xpath('.//td[@class="alt1"]//div//strong//text()').extract()) or 'Null'
            post_url = ''.join(node.xpath('.//a[contains(@href,"showpost.php?")]//@href').extract()) or 'Null'
            post_url = urljoin("https://fuckav.ru/", post_url) or 'Null'
	    ord_in_thread = ''.join(node.xpath('.//a[@target = "new"]//strong//text()').extract()) or 'Null'
            post_id = post_url.split('=')[1] or 'Null'
            post_id = post_id.split('&')[0]
            publish_date = ''.join(node.xpath('.//td[@class="alt1"]//a[contains(@name,"post")]//following-sibling::text()').extract()).strip()or 'Null'
            publish_time = time_to_epoch(publish_date,'%d-%m-%Y,  %H:%M')
            if publish_time == None:
                publish = ''.join(re.findall('\d+-\d+-\d+', publish_date))
                publish_time = time_to_epoch(publish,'%d-%m-%Y')
            if publish_time == None:
                publish = ''.join(re.findall('\d+/\d+/\d+', publish_date))
                publish_time = time_to_epoch(publish,'%d/%m/%Y')
            if publish_time == None:
                pass
	    if publish_time:
                month_year = time.strftime("%Y-%m", time.localtime(int(publish_time/1000)))
            else:
                pass

            author = ''.join(node.xpath('.//a[@class="bigusername"]//text()').extract()) or 'Null'
            auth_url = ''.join(node.xpath('.//a[@class="bigusername"]/@href').extract()) or 'Null'
            if auth_url == '':
                continue
            if 'http' not in auth_url:
                author_url = urljoin("https://fuckav.ru/", auth_url)
            text =''.join(node.xpath('.//div[contains(@id,"post_message")]//text() |.//div[contains(@id,"post_message")]//@href |.//div[contains(@id,"post_message")]//@src').extract()).replace(u'\u0426\u0438\u0442\u0430\u0442\u0430:',u'\u0426\u0438\u0442\u0430\u0442\u0430: %s'%'Quote').strip() or 'Null'
            text = clean_text(text)
            links = node.xpath('.//div[contains(@id,"post_message")]//a[contains(@target,"_blank")]//@href |.//div[contains(@id,"post_message")]//a[contains(@target,"_blank")]//img/@src |.//div[contains(@id,"post_message")]//a[contains(@class,"highslide")]//@href |.//div[contains(@id,"post_message")]//a[contains(@class,"highslide")]//@src | .//a[contains(@href,"https:")]//@href | .//div[contains(@id,"post_message")]//a[contains(@href,"@")]/@href').extract()
	    if links == []:
                links = 'Null'
	    author_data ={
		'name':author,
                'url':author_url
            }
            json_posts.update({
		    'id' : post_url,
                    'hostname': 'www.fuckav.ru',
                    'domain': "fuckav.ru",
                    'sub_type':'openweb',
                    'type' : 'forum',
                    'author': json.dumps(author_data),
                    'title':clean_text(thread_title),
                    'text': text,
                    'url': response.url,
                    'original_url': response.url,
                    'fetch_time': fetch_time(),
                    'publish_time': publish_time,
                    'link_url':links,
                    'post':{
                        'cache_link':'',
			'author':json.dumps(author_data),
                 	'section':category,
                    	'language':'russian',
                    	'require_login':'false',
                   	'sub_section':sub_category,
                    	'sub_section_url':sub_category_url,
                    	'post_id': post_id,
                    	'post_title':post_title,
                    	'ord_in_thread': ord_in_thread,
                    	'post_url': post_url,
                    	'post_text':text,
                    	'thread_title':thread_title,
		    	'thread_url': thread_url
                    },
            })
            #pprint(json_values)
	    sk = md5_val(post_url)
	    #query={"query":{"match":{"_id":sk}}}
            #res = es.search(body=query)
            #if res['hits']['hits'] == []:
	    es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts,request_timeout=30)
	    '''else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)'''

            meta = {'publish_time': publish_time}
            if author_url:

                json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
                    }
                self.cursor.execute(self.author_crawl,json_crawl)
        next_page = response.xpath('//div[@class="pagenav"]//td[@class="alt1"]/a[@rel="next"]/@href').extract_first()
        if next_page:
            next_page = urljoin("https://fuckav.ru/", next_page)
            yield Request(next_page, callback=self.parse_meta)
