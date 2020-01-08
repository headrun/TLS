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
	error = ''.join(response.xpath('//div[@id="elErrorMessage"]//text()').extract())
	if error:
	    return
        try:category = response.xpath('//ul[@data-role="breadcrumbList"]//li//span//text()').extract()[1] or 'Null'
        except:category = 'Null'
        try:sub_category = response.xpath('//ul[@data-role="breadcrumbList"]//li//span//text()').extract()[2].encode('utf8') or 'Null'
        except:sub_category = null
	sub_category_url = response.xpath('//ul[@data-role="breadcrumbList"]//li//a/@href').extract()[2]
	if sub_category_url == '':
	    sub_category_url = 'Null'
        thread_title = ''.join(response.xpath('//span[@class="ipsType_break ipsContained"]//span//text()').extract()).strip() or 'Null'
        nodes = response.xpath('//div[@id="elPostFeed"]//article[contains(@id,"elComment_")]')
        inner_nav = response.xpath('//p[@class="pagelink conl"]//a[@rel="next"]//@href').extract_first()
        if inner_nav:
            yield Request(inner_nav, callback=self.parse_next)
        thread_url = response.url
        if '/page' in thread_url:
            test = re.findall('(.*)/page',thread_url)
            thread_url = ''.join(test)
        else:
            thread_url = thread_url
        ord_in_thread = 0
        for node in nodes:
	    ord_in_thread = ord_in_thread+1
            author = ''.join(node.xpath('.//h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//a[@class="ipsType_break"]//span//text() | .//h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//s//text()').extract()) or 'Null'
            author_url = ''.join(node.xpath('.//h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//a[@class="ipsType_break"]/@href').extract()) or 'Null'
            post_title = 'Null'
            post_url = ''.join(node.xpath('.//div[@class="ipsType_reset"]//a//@href').extract()) or 'Null'
            post_id = ''.join(post_url).split('=')[-1] or 'Null'
            date = ''.join(node.xpath('.//a[@class="ipsType_blendLinks"]//time//@title').extract())
            publish_date = datetime.datetime.strptime(date,'%m/%d/%y %I:%M  %p')
            publish_epoch = time.mktime(publish_date.timetuple())*1000
	    if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
		    month_year = get_index(publish_epoch)
                else:
                    continue
	    else:
		import pdb;pdb.set_trace()

            text = '\n'.join(node.xpath('.//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//img//@alt | .//span[@class="ipsType_reset ipsType_medium ipsType_light"]//strong//text() | .//blockquote[@class="ipsQuote"]//@data-ipsquote-timestamp | .//blockquote[@class="ipsQuote"]//@data-ipsquote-username | .//div[@class="ipsQuote_contents"]//p//img//@alt |.//blockquote/@class | .//span[@class="ipsType_reset ipsType_medium ipsType_light"]//text() | .//img[@class="ipsImage_thumbnailed"]//@alt').extract()).replace('ipsQuote','Quote On') or 'Null'
            t_author = node.xpath('.//blockquote[@class="ipsQuote"]//@data-ipsquote-username').extract() or 'Null'
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
	    if all_links == '':
		all_links = 'Null'
	    if links == []:
	        all_links = 'Null'
            reference_url = response.url
            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
	    author_data = {
			'name':author,
			'url':author_url
			}
	    post = {
		'cache_link':'',
		'author':json.dumps(author_data),
		'section':category,
                'language':'german',
		'require_login':'false',
		'sub_section':sub_category,
		'sub_section_url': sub_category_url,
		'post_id':post_id,
		'post_title':post_title,
		'ord_in_thread':ord_in_thread,
		'post_url':post_url,
		'post_text':text,
		'thread_title':thread_title,
		'thread_url':thread_url
	     	}
            json_posts = {
			  'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
			  'hostname':'cnw4evazulooacab.onion',
			  'domain': domain,
			  'sub_type':'darkweb',
			  'type':'forum',
			  'author':json.dumps(author_data),
			  'title':thread_title,
			  'text':text,
			  'url':post_url,
			  'original_url':post_url,
			  'fetch_time':fetch_time,
			  'publish_time':publish_epoch,
			  'link_url':all_links,
			  'post':post
			  }
            sk = md5_val(post_url)
	    #query={"query":{"match":{"_id":sk}}}
            #res = es.search(body=query)
            #if res['hits']['hits'] == []:
	    es.index(index=month_year, doc_type='post', id=sk, body=json_posts)
            #doc_to_es(id=sk,body=json_posts,doc_type='post')

            auth_meta = {'publish_time': publish_epoch}
            json_posts.update({
                    'post_id': post_id,
                    'auth_meta': json.dumps(auth_meta),
                    'crawl_status':0,
                    'links': author_url
                    })
            self.cursor.execute(query, json_posts)
            self.conn.commit()
