from mango.utils import *


class Cracked(Spider):
    name = "cracked_posts"
    start_urls = ['https://cracked.to/']


    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        scraper = cfscrape.create_scraper()
        res = scraper.get('https://cracked.to/')
        yield Request('https://cracked.to/', callback=self.parse_post)


    def parse_post(self, response):
        url_query = 'select distinct(post_url) from cracked_post_crawl where crawl_status = 0'
        self.cursor.execute(url_query)
        urls = self.cursor.fetchall()
        for url in urls:
            yield Request(url[0], callback=self.parse_data)


#POSTS_DATA

    def parse_data(self, response):
        hostname = 'cracked.to'
        domain = 'cracked.to'
        sub_type = 'openweb'
        category = response.xpath('//div[@class="navigation hide-mobile"]//a[@class="nav-seperator"]//text()').extract()[1] or 'Null'
        sub_category = response.xpath('//div[@class="navigation hide-mobile"]//a[@class="nav-seperator"]//text()').extract()[2] or 'Null'
        sub_categoryurl = response.xpath('//div[@class="navigation hide-mobile"]//a[@class="nav-seperator"]/@href').extract()[2]
        if sub_categoryurl:
            sub_category_url = urljoin('https://cracked.to/', sub_categoryurl)
        if sub_categoryurl == '':
            sub_category_url = 'Null'
        reference_url = response.request.url
        if '?page=' in reference_url:
            thread_url = re.sub('page=(.*)', '', reference_url).replace('?', '')
        else:
            thread_url = reference_url
        thread_title = ''.join(response.xpath('//div[@class="thread-header"]//h1/text()').extract()) or 'Null'
        nodes = response.xpath('//div[contains(@class,"post-set")]')
        for node in nodes:
            post_title = 'Null'
            posturl = ''.join(node.xpath('.//div[@class="right postbit-number"]//strong/a/@href').extract())
            if posturl:
                post_url = urljoin('https://cracked.to/', posturl)
            if posturl == '':
                post_url = 'Null'
            post_id = re.sub('(.*)#pid', '', post_url) or 'Null'
            record_id = re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", ""))
            ord_in_thread = ''.join(node.xpath('.//div[@class="float_right inline-block"]//span[@class="posturl"]//strong//a//text()').extract()).replace('#', '')
            author = ''.join(node.xpath('.//div[@class="post-username"]//a//text()').extract()) or 'Null'
            author_url = ''.join(node.xpath('.//div[@class="post-username"]//a/@href').extract()) or 'Null'
            fetchtime = fetch_time()
            date = ''.join(node.xpath('.//span[contains(@class,"post_date")]/span[@class="post-op"]/following-sibling::text() | .//span[contains(@class,"post_date")]/text()').extract()).strip()
	    try:
                if 'hour' in date:
                    date_time = ''.join(re.findall('\d+', date))
                    publish = datetime.datetime.now() - timedelta(hours=int(date_time))
                    publish_time = time.mktime(publish.timetuple())*1000
                if 'day' in date:
                    date_time = ''.join(re.findall('\d+', date))
                    publish = datetime.datetime.now() - timedelta(days=int(date_time))
                    publish_time = time.mktime(publish.timetuple())*1000
                if 'week' in date:
                    date_time = ''.join(re.findall('\d+', date))
                    publish = datetime.datetime.now() - timedelta(weeks=int(date_time))
                    publish_time = time.mktime(publish.timetuple())*1000
                if 'month' in date:
                    date_time = ''.join(re.findall('\d+', date))
                    now = datetime.datetime.now()
                    publish = now + dateutil.relativedelta.relativedelta(months=-int(date_time))
                    publish_time = time.mktime(publish.timetuple())*1000
                if 'year' in date:
                    date_time = ''.join(re.findall('\d+', date))
                    now = datetime.datetime.now()
                    publish = now + dateutil.relativedelta.relativedelta(years=-int(date_time))
                    publish_time = time.mktime(publish.timetuple())*1000
            except:
                pass

	    if publish_time:
                year = time.strftime("%Y", time.localtime(int(publish_time/1000)))
                if year > '2011':
		    month_year = time.strftime("%m_%Y", time.localtime(int(publish_time)/1000))
                else:
                    continue
	    else:
		pass
	    
	    text = clean_text(' '.join(node.xpath('.//div[@class="post_body scaleimages"]/text() | .//div[@class="post_body scaleimages"]/img/@title | .//div[@class="post_body scaleimages"]/a[@target="_blank"]/span/text() | .//div[@class="post_body scaleimages"]/div[@style="text-align: center;"]//text() | .//div[@class="post_body scaleimages"]/blockquote/cite//text() | .//div[@class="post_body scaleimages"]/blockquote/text() |.//div[@class="post_body scaleimages"]/blockquote/@class | .//div[@class="post_body scaleimages"]/span//text() | .//div[@class="post_body scaleimages"]/ol//text() | .//div[@class="post_body scaleimages"]/blockquote/span//text() | .//div[@class="post_body scaleimages"]/blockquote/ol//text() | .//div[@class="post_body scaleimages"]/a[@target="_blank"]/text() | .//div[@class="post_body scaleimages"]/div[@class="hidden-content"]//text() | .//div[@class="post_body scaleimages"]//div[contains(@class,"leak_rating leak_rating_")]//text() | .//div[@class="post_body scaleimages"]/blockquote//img/@title | .//div[@class="post_body scaleimages"]/blockquote/div[@style="text-align: center;"]//text()').extract())).replace('\n', ' ').replace('mycode_quote', 'Quote ').encode('ascii', 'ignore').strip() or 'Null'

            links_ = []

            all_links = node.xpath('.//div[@class="post_body scaleimages"]/a[@target="_blank"]/@href | .//div[@class="post_body scaleimages"]/div[@style="text-align: center;"]/a[@target="_blank"]/@href | .//div[@class="post_body scaleimages"]/blockquote/a[@target="_blank"]/@href | .//div[@class="post_body scaleimages"]/blockquote/span//a[@target="_blank"]/@href | .//div[@class="post_body scaleimages"]/blockquote/ol//a[@target="_blank"]/@href | .//div[@class="post_body scaleimages"]//div[@class="hidden-content"]//a/@href').extract()
            for link in all_links:
                if '/cracked.to/' in link:
                    continue
                links_.append(link)

            links = ', '.join(links_)
            if links == '':
                links = 'Null'
            if all_links == []:
                links = 'Null'

            author_data = {
                    'name':author,
                    'url':author_url
                    }
            post = {
                    'cache_link':'',
                    'section':category,
                    'author':json.dumps(author_data),
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
                    'record_id':record_id,
                    'hostname':hostname,
                    'domain':domain,
                    'sub_type':'openweb',
                    'type':'forum',
                    'author':json.dumps(author_data),
                    'title':thread_title,
                    'text':text,
                    'url':post_url,
                    'original_url':post_url,
                    'fetch_time':fetchtime,
                    'publish_time':publish_time,
                    'link.url':links,
                    'post':post
                    }

	    sk = md5_val(post_url)
            es.index(index='forum_posts_'+month_year, doc_type='post', id=sk, body=json_posts)
	    if author_url == 'Null':
		continue
            auth_meta = {'publish_time':publish_time}
            json_author = {
                    'post_id':post_id,
                    'auth_meta':json.dumps(auth_meta),
                    'crawl_status':0,
                    'links':author_url
                    }
            query = generate_upsert_query_authors_crawl('cracked')
            self.cursor.execute(query, json_author)
            self.conn.commit()
        page_nav = response.xpath('//div[@class="pagination"]//a[contains(@class,"pagination_next")]/@href').extract_first()
        if page_nav:
            next_page = urljoin('https://cracked.to/', page_nav)
            yield Request(next_page, callback=self.parse_data)
