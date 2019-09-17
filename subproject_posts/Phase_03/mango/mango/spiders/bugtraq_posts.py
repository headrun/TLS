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
        category = response.xpath('//nobr//a[contains(@href,"type=sb")]/text()').extract_first()
        sub_category = ''
        thread = response.request.url
        if '&page=' in thread:
            thread_url = re.sub('&page=(.*)', '', thread).strip()
        elif thread:
            thread_url = thread
        else:
            import pdb;pdb.set_trace()
        post_title = ''.join(response.xpath('//td[@align="left"]//span[@class="ml"]/preceding-sibling::b/text()').extract())
        post_id = re.sub('(.*)m=', '', response.request.url)
        auth = response.xpath('//td[@align="left"]//span[@class="ml"]/following-sibling::span[@class="ml"]/text()').extract_first()
        if '<' in auth:
            auth_data = re.sub('(.*)<', '', auth)
            author = re.sub('>(.*)', '', auth_data)
        elif  u'\u0421\u0442\u0430\u0442\u0443\u0441' in auth:
            author = ''.join(re.findall(':(.*):',auth)).replace(u'\u0421\u0442\u0430\u0442\u0443\u0441', '').strip()
        elif auth:
            author = response.xpath('//span[@class="ml"]//following-sibling::font//text()').extract_first()
        else:
            import pdb;pdb.set_trace()

        num_of_views = ''.join(response.xpath('//span[contains(., "%s")]/text()'%u'\u0427\u0438\u0441\u043b\u043e \u043f\u0440\u043e\u0441\u043c\u043e\u0442\u0440\u043e\u0432:').extract())
        if num_of_views:
            views = num_of_views.replace(u'\u0427\u0438\u0441\u043b\u043e \u043f\u0440\u043e\u0441\u043c\u043e\u0442\u0440\u043e\u0432:', '').strip()
        else:
            import pdb;pdb.set_trace()

        date = response.xpath('//td[@align="left"]//span[@class="ml"]/i/text()').extract_first()
        publish_time = time_to_epoch(date, '%d.%m.%y %H:%M')
        if publish_time == False:
            import pdb;pdb.set_trace()

        text = ''.join(response.xpath('//td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//text() | //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//a[@target="_blank"]//text() | //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//u//text() |  //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//u//a[@target="_blank"]//text()').extract())
        links = response.xpath('//td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//a[@target="_blank"]/@href | //td[contains(@bgcolor,"#FAFAFC") and contains(@align,"left")]//u//a[@target="_blank"]/@href').extract()
        json_posts = {
                'domain':domain,
                'category':category,
                'sub_category':sub_category,
                'thread_title':'',
                'thread_url':thread_url,
                'post_title':post_title,
                'post_url':'',
                'post_id':post_id,
                'author':author,
                'author_url':'',
                'publish_time':publish_time,
                'fetch_time':fetch_time(),
                'text':text,
                'links':', '.join(links),
                'views':views
                }
        sk = md5_val(post_id + domain)
	query={"query":{"match":{"_id":sk}}}
        res = es.search(body=query)
        if res['hits']['hits'] == []:
            es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
	else:
	    data_doc = res['hits']['hits'][0]
	    if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
	        es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
