from mango.utils import *

class Defendtheweb(Spider):
    name = "defendtheweb_posts"

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.query = generate_upsert_query_authors_crawl('defendtheweb')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        url_query = 'select distinct(post_url) from defendtheweb_post_crawl where crawl_status = 0'
        self.cursor.execute(url_query)
        urls = self.cursor.fetchall()
        for url in urls:
            yield Request(url[0], callback=self.parse_data)


#POSTS_DATA

    def parse_data(self, response):
        domain = 'defendtheweb.net'
        categories = response.xpath('//ul[@class="breadcrumb"]//li//a/text()').extract()
        try:
            if len(categories) == 4:
                category = categories[0]
                sub_category = categories[1] +', '+ categories[2]
            if len(categories) == 3:
                category = categories[0]
		sub_category = categories[1]
        except:
            pass

        category_urls = response.xpath('//ul[@class="breadcrumb"]//li//a/@href').extract()
        try:
            if len(category_urls) == 4:
                sub_category_url = urljoin('https://defendtheweb.net', category_urls[1]) +', '+ urljoin('https://defendtheweb.net', category_urls[2])
            if len(category_urls) == 3:
                sub_category_url = urljoin('https://defendtheweb.net', category_urls[1])
        except:
            pass

        thread_url = response.request.url
        thread_title = ''.join(response.xpath('//div[@class="twelve columns discussion-thread"]/h1/text()').extract()) or 'Null'
        nodes = response.xpath('//div[contains(@id, "post-")]')
        ord_in_thread = 0
        for node in nodes:
            post_url = 'Null'
	    post_title = 'Null'
            post_id = ''.join(node.xpath('./@data-post-id').extract()) or 'Null'
            if post_id == 'Null':
                continue
            author = ''.join(node.xpath('.//div[@class="discussion-thread-message-meta"]/a[@class="discussion-thread-message-author"]//text()').extract()) or 'Null'
            author_url = ''.join(node.xpath('.//div[@class="discussion-thread-message-meta"]/a[@class="discussion-thread-message-author"]/@href').extract()) or 'Null'
            ord_in_thread = ord_in_thread+1
            publish = node.xpath('.//div[@class="discussion-thread-message-meta"]//span/@title').extract_first().replace('(UTC)', '').strip()
            publish_time = time_to_epoch(publish, '%d/%m/%Y %H:%M')
            if publish_time:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_time)/1000))
            else:
                pass

            fetch_epoch = fetch_time()
            text = clean_text(' '.join(node.xpath('.//div[@class="discussion-thread-message-main"]//div[@class="discussion-thread-message-content"]/blockquote | .//div[@class="discussion-thread-message-main"]//div[@class="discussion-thread-message-content"]//following-sibling::p//text()').extract()).replace('<blockquote><p><strong>', '').replace('</strong>', '').replace('<strong>', '').replace('</p></blockquote></blockquote>', '').replace('<br>\n', '').replace('<blockquote><p>', '').replace('<blockquote>', 'Quote ').strip())

            all_links = []
            links = node.xpath('.//div[@class="discussion-thread-message-main"]//div[@class="discussion-thread-message-content"]//a[@target="_blank"]/@href').extract()
            for link in links:
                if '/defendtheweb.net/' in link:
                    continue
                all_links.append(link)
            links_url = ', '.join(all_links)
            if links_url == '':
		links_url = 'Null'
            if links == []:
                links_url = 'Null'
            author_data = {
                'name':author,
                'url':author_url
                }
            post = {
		'cache_link':'',
		'author':json.dumps(author_data),
		'section':category,
		'language':'english',
		'require_login':'false',
		'sub_section':sub_category,
	    	'sub_section_url':sub_category_url,
	    	'post_id':post_id,
	    	'post_title':post_title,
	    	'ord_in_thread':ord_in_thread,
	    	'post_url':post_url,
	    	'post_text':text,
	    	'thread_title':thread_title,
	    	'thread_url':thread_url
	    	}
            json_posts = {
	    	'record_id':post_url,
	    	'hostname':'defendtheweb.net',
	    	'domain':domain,
	    	'sub_type':'openweb',
	    	'type':'forum',
	    	'author':json.dumps(author_data),
	    	'title':thread_title,
	    	'text':text,
	    	'url':post_url,
	    	'original_url':post_url,
	    	'fetch_time':fetch_epoch,
	    	'publish_time':publish_time,
	    	'link.url':links_url,
	    	'post':post
                }
            sk = md5_val(domain + post_id)
            es.index(index='forum_posts_' + month_year, doc_type='post', id=sk, body=json_posts)
            if author_url == 'Null':
                continue
            auth_meta = {'publish_time':publish_time}
            json_author = {
            	'post_id':post_id,
                'auth_meta':json.dumps(auth_meta),
                'crawl_status':0,
                'links':author_url
                }
            self.cursor.execute(self.query, json_author)
