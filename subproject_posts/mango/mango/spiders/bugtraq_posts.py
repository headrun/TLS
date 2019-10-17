from mango.utils import *

class Bugtraq(Spider):
    name = "bugtraq_posts"
    start_urls = ["https://bugtraq.ru/forum/"]
    


    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_USER, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()



    def parse(self, response):
        url_que = 'select distinct(post_url) from bugtraq_crawl where crawl_status = 0 '
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback=self.parse_data)


    def parse_data(self, response):
        domain = "bugtraq.ru"
        category = response.xpath('//nobr//a[contains(@href,"type=sb")]/text()').extract_first() or 'Null'
        sub_category = '' or 'Null'
        sub_category_url = '' or 'Null'
	x=0
        x=x+1
	thread_title = '' or 'Null'
        thread = response.request.url 
        if '&page=' in thread:
            thread_url = re.sub('&page=(.*)', '', thread).strip()
        elif thread:
            thread_url = thread
        else:
            import pdb;pdb.set_trace()
	if thread_url == '':
	    thread_url = 'Null'
        post_title = ''.join(response.xpath('//td[@align="left"]//span[@class="ml"]/preceding-sibling::b/text()').extract()) or 'Null'
        post_id = re.sub('(.*)m=', '', response.request.url)or 'Null'
	post_url = '' or 'Null'
        author_url = '' or 'Null'
        auth = response.xpath('//td[@align="left"]//span[@class="ml"]/following-sibling::span[@class="ml"]/text()').extract_first() 
        if '<' in auth:
            auth_data = re.sub('(.*)<', '', auth)
            author = re.sub('>(.*)', '', auth_data)
        elif  u'\u0421\u0442\u0430\u0442\u0443\u0441' in auth:
            author = ''.join(re.findall(':(.*):',auth)).replace(u'\u0421\u0442\u0430\u0442\u0443\u0441', '').strip()
        elif auth:
            author = response.xpath('//span[@class="ml"]//following-sibling::font//text()').extract_first()
	if auth == '':
	    author ='Null'
        num_of_views = ''.join(response.xpath('//span[contains(., "%s")]/text()'%u'\u0427\u0438\u0441\u043b\u043e \u043f\u0440\u043e\u0441\u043c\u043e\u0442\u0440\u043e\u0432:').extract()) or 'Null'
        if num_of_views:
            views = num_of_views.replace(u'\u0427\u0438\u0441\u043b\u043e \u043f\u0440\u043e\u0441\u043c\u043e\u0442\u0440\u043e\u0432:', '').strip()
        else:
            import pdb;pdb.set_trace()

        date = response.xpath('//td[@align="left"]//span[@class="ml"]/i/text()').extract_first() or 'Null'
        publish_time = time_to_epoch(date, '%d.%m.%y %H:%M')
        if publish_time == False:
            import pdb;pdb.set_trace()
        if publish_time:
            month_year = time.strftime("%m_%Y", time.localtime(int(publish_time/1000)))
        else:
            import pdb;pdb.set_trace()

        text = ''.join(response.xpath('//td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//text() | //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//a[@target="_blank"]//text() | //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//u//text() |  //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//u//a[@target="_blank"]//text()').extract()).strip().replace('\n','') or 'Null'
        links =', '.join(response.xpath('//td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//a[@target="_blank"]/@href | //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//u//a[@target="_blank"]/@href').extract())
	if links == '':
            links = 'Null'
	author_data = {
                    'name':author,
                    'url':author_url
                    }

        json_posts = {'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                'hostname': 'www.bugtraq.ru',
                'domain': "bugtraq.ru",
                'sub_type':'openweb',
                'type' : 'forum',
		'author':json.dumps(author_data),
                'title':clean_text(thread_title),
                'text': text,
                'url': post_url,
                'original_url': post_url,
                'fetch_time': fetch_time(),
                'publish_time': publish_time,
                'link_url': links,
                'post':{
                    'cache_link':'',
                    'author': json.dumps(author_data),
                    'section':category,
                    'language':'russian',
                    'require_login':'false',
                    'sub_section':sub_category,
                    'sub_section_url':sub_category_url,
                    'post_id': post_id,
                    'post_title':post_title,
                    'ord_in_thread': x,
                    'post_url': post_url,
                    'post_text':text,
                    'thread_title':thread_title,
                    'thread_url': thread,
		    'views':views
		},
	}
        sk = md5_val(post_id + domain)
	#query={"query":{"match":{"_id":sk}}}
        #res = es.search(body=query)
        #if res['hits']['hits'] == []:
        es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts,request_timeout=30)
	'''else:
	    data_doc = res['hits']['hits'][0]
	    if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
	        es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)'''
