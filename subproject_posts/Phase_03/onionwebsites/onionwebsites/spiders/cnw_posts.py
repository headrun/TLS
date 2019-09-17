from onionwebsites.utils import *
query = generate_upsert_query_authors_crawl('cnw')

class CNW(scrapy.Spider):
    name = "cnw_posts"
    start_urls = ["http://cnw4evazulooacab.onion/?tckattempt=1"]

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        url_que = "select distinct(post_url) from cnw_crawl"
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_next)

    def parse_next(self, response):
        domain = "cnwv3ycmy4uc7vou.onion"
        category = response.xpath('//ul[@data-role="breadcrumbList"]//li//span//text()').extract()[1]
        sub_category = response.xpath('//ul[@data-role="breadcrumbList"]//li//span//text()').extract()[2].encode('utf8')
        thread_title = ''.join(response.xpath('//span[@class="ipsType_break ipsContained"]//span//text()').extract()).strip()
        nodes = response.xpath('//div[@id="elPostFeed"]//article[contains(@id,"elComment_")]')
        for node in nodes:
            author = ''.join(node.xpath('.//h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//a[@class="ipsType_break"]//span//text() | .//h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//s//text()').extract())
            author_url = ''.join(node.xpath('.//h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//a[@class="ipsType_break"]/@href').extract())
            post_title = ''
            post_url = ''.join(node.xpath('.//div[@class="ipsType_reset"]//a//@href').extract())
            post_id = ''.join(post_url).split('=')[-1]
            date = ''.join(node.xpath('.//a[@class="ipsType_blendLinks"]//time//@title').extract())
            publish_date = datetime.datetime.strptime(date,'%m/%d/%y %I:%M  %p')
            publish_epoch = time.mktime(publish_date.timetuple())*1000
            text = '\n'.join(node.xpath('.//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//img//@alt | .//span[@class="ipsType_reset ipsType_medium ipsType_light"]//strong//text() | .//blockquote[@class="ipsQuote"]//@data-ipsquote-timestamp | .//blockquote[@class="ipsQuote"]//@data-ipsquote-username | .//div[@class="ipsQuote_contents"]//p//img//@alt |.//blockquote/@class | .//span[@class="ipsType_reset ipsType_medium ipsType_light"]//text() | .//img[@class="ipsImage_thumbnailed"]//@alt').extract()).replace('ipsQuote','Quote On')
            t_author = node.xpath('.//blockquote[@class="ipsQuote"]//@data-ipsquote-username').extract()
            t_date = node.xpath('.//blockquote[@class="ipsQuote"]//@data-ipsquote-timestamp').extract()

            if t_author and t_date:
                for auth,t_da in zip(t_author,t_date):
                    a1 =   auth + ' said:'
                    try:
                        t1 = float(t_da)
                        t1 = time.strftime("%m/%d/%Y at %I:%M %p,", time.localtime(t1))
                        text = text.replace(auth,a1).replace(t_da,t1)
                    except:pass
            links = node.xpath('.//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//a//@href | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//h2//img/@src | .//img[@class="ipsImage_thumbnailed"]//@src').extract()
            all_links = ', '.join(links)
            reference_url = response.url
            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
            thread_url = response.url
            if '/page' in reference_url:
                test = re.findall('(.*)/page',reference_url)
                thread_url = ''.join(test)
            else:
                thread_url = reference_url
            json_posts = {'domain': domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'fetch_time': fetch_time,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'author': author,
                          'author_url': author_url,
                          'text': text,
                          'links': all_links,
            }
            sk = md5_val(post_url)
	    query={"query":{"match":{"_id":sk}}}
            res = es.search(body=query)
            if res['hits']['hits'] == []:
		es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
            #doc_to_es(id=sk,body=json_posts,doc_type='post')
	    else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)

            auth_meta = {'publish_time': publish_epoch}
            json_posts.update({
                    'post_id': post_id,
                    'auth_meta': json.dumps(auth_meta),
                    'crawl_status':0,
                    'links': author_url
                    })
            self.cursor.execute(query, json_posts)
            self.conn.commit()
        #inner_nav = response.xpath('//li[@class="ipsPagination_next"]//a//@href').extract_first()
        inner_nav = response.xpath('//p[@class="pagelink conl"]//a[@rel="next"]//@href').extract_first()
        if inner_nav:
            yield Request(inner_nav, callback=self.parse_next)
