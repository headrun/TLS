from mango.utils import *


class Dute66(Spider):
    name = 'dute66_posts'
    que = generate_upsert_query_posts_crawl('dute66')
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN':1,
        'DOWNLOADER_MIDDLEWARES':{'mango.middlewares.MangoDownloaderMiddleware': 543}

        }
    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user=DATABASE_USER,
                                    passwd=DATABASE_PASS,
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        fetach_que = 'select post_url from dute66_crawl where crawl_status = 0 '#limit 1000'
        self.cursor.execute(fetach_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0],callback = self.parse_posts)

    def parse_posts(self,response):
        if response.xpath('//div[@id="messagetext"]//a[@target="_blank" and contains(@href,"plugin.php")]'):
	    up_que = 'update dute66_crawl set crawl_status = 9 where post_url = %(url)s'
            up_val = {'url':response.request.url.encode('utf8')}
            self.cursor.execute(up_que,up_val)
            return
        next_page = response.xpath('//div[@class="pg"]//a[@class="nxt"]/@href').extract_first()
        nodes = response.xpath('//div[@id="postlist"]//div[contains(@id,"post_")]//table[contains(@class,"plhin")]')
        if next_page:
            try:
                l_node = ''.join(nodes[-1].xpath('.//div[@class="pi"]//a[contains(@id,"postnum")]/@href').extract())
                if check_rec_in_es(md5_val(l_node)):
                    yield Request(next_page,callback = self.parse_forums)
            except:
                pass
        if nodes:
            up_que = 'update dute66_crawl set crawl_status = 1 where post_url = %(url)s'
            up_val = {'url':response.request.url.encode('utf8')}
            self.cursor.execute(up_que,up_val)
        try:
            category = response.xpath('//div[@id="pt"]//div[@class="z"]//a[contains(@href,"dute56")]/text()').extract()[1]
            sub_category = response.xpath('//div[@id="pt"]//div[@class="z"]//a[contains(@href,"dute56")]/text()').extract()[2:-1]
        except:
            category = ''
            sub_category = ''

        for node in nodes:
            domain = 'dute56.com'
            author = ''.join(node.xpath('.//div[@class="pi"]//div[@class="authi"]//a[@target="_blank" and contains(@href,"space-uid")]//text()').extract())
            if not author:
                author = u'\u533f\u540d\u8005'
            author_url = ''.join(node.xpath('.//div[@class="pi"]//div[@class="authi"]//a[@target="_blank" and contains(@href,"space-uid")]//@href').extract())
            links = ', '.join(set(node.xpath('.//div[@class="pct"]//div[@class="pcb"]//div[@class="t_fsz"]//following-sibling::*//a[@target="_blank"]/@href | .//div[@class="pct"]//div[@class="pcb"]//div[@class="t_fsz"]//following-sibling::*//img[contains(@id,"aimg_" and @class="zoom")]/@src' ).extract()))
            post_id = ''.join(node.xpath('.//div[@class="pi"]//a[contains(@id,"postnum")]/@id').extract()).replace('postnum','')
            post_title = ''
            post_url = ''.join(node.xpath('.//div[@class="pi"]//a[contains(@id,"postnum")]/@href').extract())
            publish = ''.join(node.xpath('.//div[@class="authi"]//em/text()').extract())

            publish_time_ = re.findall('\d+-\d+-\d+ \d+:\d+:\d+', publish )
            publish_time = time_to_epoch(''.join(publish_time_),'%Y-%m-%d %H:%M:%S')
	    if publish_time == None:
		publish_time = 0
            text = clean_text(' '.join(node.xpath('.//div[@class="pct"]//div[@class="pcb"]//div[@class="t_fsz"]//following-sibling::*//text()').extract()))
            thread_title = response.xpath('//div[@id="pt"]//div[@class="z"]//a[contains(@href,"dute56")]/text()').extract()[-1]
            thread_url = ''.join(response.xpath('//link[@rel="canonical"]/@href').extract())
            doc = {
                    'domain':domain,
                    'author':author,
                    'author_url':author_url,
                    'links':links,
                    'post_id':post_id,
                    'post_title':post_title,
                    'post_url':post_url,
                    'category':category,
                    'sub_category':sub_category,
                    'publish_time':publish_time,
                    'fetch_time':fetch_time(),
                    'text':text,
                    'thread_title':thread_title,
                    'thread_url':thread_url
                    }
            sk = md5_val(post_url)
	    query={"query":{"match":{"_id":sk}}}
            res = es.search(body=query)
            if res['hits']['hits'] == []:
            #es.index(index="new_data", doc_type='post', id=sk, body=doc)
                es.index(index="forum_posts", doc_type='post', id=sk, body=doc)
	    else:
		data_doc = res['hits']['hits'][0]
		if (doc['links'] != data_doc['_source']['links']) or (doc['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=doc)
