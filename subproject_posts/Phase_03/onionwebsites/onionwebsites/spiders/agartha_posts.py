from onionwebsites.utils import *

class Agartha(scrapy.Spider):
    name = "agartha_posts"
    start_urls = ["http://agartha2oooh2cxa.onion/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_ID,passwd=DATABASE_PASS , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        url_que = "select distinct(post_url) from agartha_crawl where crawl_status = 0 or 1"
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
	#data = [['http://agartha2oooh2cxa.onion/index.php?topic=5.0;prev_next=next']]
        for url in data:
            yield Request(url[0], callback = self.parse_next)

    def parse_next(self, response):
        domain = "agartha2oooh2cxa.onion"
        category = response.xpath('//div[@class="navigate_section"]//ul//li//a//span//text()').extract()[1] or 'Null'
        sub_category = ''.join(response.xpath('//div[@class="navigate_section"]//ul//li//a//span//text()').extract()[2]).encode('utf8') or 'Null'
        thread_title = response.xpath('//li[@class="last"]//a//span/text()').extract()[1].strip() or 'Null'
        nodes = response.xpath('//div[contains(@class,"post_wrapper")]')
        for node in nodes:
            author = ''.join(node.xpath('.//div[@class="poster"]//h4//a//text()').extract()) or 'Null'
            author_url = ''.join(node.xpath('.//div[@class="poster"]//h4//a//@href').extract())
	    if author_url == '':
		author_url = 'Null'
            post_title = ''.join(node.xpath('.//h5[contains(@id,"subject_")]//a//text()').extract()).strip() or 'Null'
            post_url = ''.join(node.xpath('.//h5[contains(@id,"subject_")]//a//@href').extract()) or 'Null'
            id_ = ''.join(re.findall('#msg\d+', post_url))
            post_id = id_.replace('#msg','') or 'Null'
            #date = ''.join(node.xpath('.//div[@class="smalltext"]/text()').extract()).encode('utf8').replace('Today',datetime.datetime.now().strftime(' %Y-%m-%d')).replace('Yesterday',(datetime.datetime.now() - timedelta(days=1)).strftime(' %Y-%m-%d')).replace('on:','').replace('at','')
	    date_ = re.sub('(.*) on: ','',''.join(node.xpath('.//div[@class="smalltext"]//text()').extract())).replace('Today at',datetime.datetime.now().strftime('%B %d, %Y,')).replace(u' \xbb','').strip()
            '''try:
                publish_date = datetime.datetime.strptime(date_,'%Y-%m-%d  %I:%M:%S %p')
                publish_epoch = time.mktime(publish_date.timetuple())*1000
            except:
                publish_date = datetime.datetime.strptime(date_,'%B %d, %Y, %I:%M:%S %p')
                publish_epoch = time.mktime(publish_date.timetuple())*1000
	    '''
	    publish_epoch = time_to_epoch(date_,'%Y-%m-%d  %I:%M:%S %p')
	    if publish_epoch ==False:
		publish_epoch = time_to_epoch(date_,'%B %d, %Y, %I:%M:%S %p')
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

            text = ' '.join(node.xpath('.//div[@class="post"]//text()').extract()).replace('\n', '') or 'Null'
            links = node.xpath('.//div[contains(@id,"msg_")]//a[@class="bbc_link"]//@href').extract()
            all_links = ', '.join(links)
	    if all_links == '':
		all_links = 'Null'
	    if links == []:
		all_links = 'Null'
            reference_url = response.url
            if 'prev_next' in reference_url:
                test = re.findall('(.*);',reference_url)
                thread_url = ''.join(test)
            else:
                thread_url = reference_url
	    author_data = {
			'name':author,
			'url':author_url,
			}
	    post = {
		'cache_link':'',
		'section':category,
		'language':'',
		'require_login':'false',
		'sub_section':sub_category,
		'sub_section_url':'',
		'post_id':post_id,
		'post_title':post_title,
		'ord_in_thread':'',
		'post_url':post_url,
		'post_text':clean_text(text),
		'thread_title':thread_title,
		'thread_url':thread_url
		}
		
            json_posts = {'domain': domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'fetch_time': fetch_time(),
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'author': author,
                          'author_url': author_url,
                          'text': clean_text(text),
                          'links': all_links,
            }
            sk = md5_val(domain + json_posts['post_id'])
	    #query={"query":{"match":{"_id":sk}}}
            #res = es.search(body=query)
            #if res['hits']['hits'] == []:
	    es.index(index=month_year, doc_type='post', id=sk, body=json_posts)
            #doc_to_es(id=sk,body=json_posts,doc_type='post')
	    '''else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)'''

        inner_nav = response.xpath('//div[@class="nextlinks_bottom"]//following-sibling::a/@href').extract_first()
        if inner_nav:
            yield Request(inner_nav, callback=self.parse_next)

