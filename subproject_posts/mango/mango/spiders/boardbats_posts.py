from mango.utils import *

class Boardbats(Spider):
    name = "boardbat_posts"
    start_urls = ["https://board.b-at-s.info/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", host="localhost", user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode=True, charset='utf8')
        self.query_status = generate_upsert_query_authors_crawl('boardbat')
        self.cursor = self.conn.cursor()
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        url_que = "select distinct(post_url) from boardbat_post_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback=self.parse_next)

#POSTS_DATA

    def parse_next(self, response):
        domain = "www.board.b-at-s.info"
        categories = response.xpath('//ul[@data-role="breadcrumbList"]//li//span//text()').extract()
        try:
            if len(categories) == 6:
                category = categories[1].strip()
                sub_category = categories[-1].strip()
	    if len(categories) == 8:
                category = categories[1].strip()
                sub_category = categories[2] +', '+categories[3].strip()
        except:
            import pdb;pdb.set_trace()

        sub_category_urls = response.xpath('//ul[@data-role="breadcrumbList"]//li//a/@href').extract()
        try:
            if len(sub_category_urls) == 6:
                sub_category_url = sub_category_urls[-1]
            if len(sub_category_urls) == 8:
                sub_category_url = sub_category_urls[2]+', '+sub_category_urls[3]
        except:
            import pdb;pdb.set_trace()

        thread_title = ''.join(response.xpath('//span[@class="ipsType_break ipsContained"]//span//text()').extract()).strip() or 'Null'
        nodes = response.xpath('//div[@id="elPostFeed"]//article[contains(@id,"elComment_")]')
        for node in nodes:
            author = ''.join(node.xpath('.//strong//a[@class="ipsType_break"]//text()').extract()).strip() or 'Null'
            author_url = ''.join(node.xpath('.//strong//a[@class="ipsType_break"]//@href').extract()) or 'Null'
            post_title = 'Null'
            post_url = ''.join(node.xpath('.//ul[@class="ipsList_inline ipsComment_tools"]//li//a[@class="ipsType_blendLinks"]//@href').extract()) or 'Null'
            if "Comment&comment" in post_url:
                post_id = ''.join(re.findall('comment=\d+',post_url)).replace('comment=','') or 'Null'
            else:
                post_id = ''.join(re.findall('topic/\d+',post_url)).replace('topic/','') or 'Null'
            date = ''.join(node.xpath('.//div[@class="ipsType_reset"]//a//time/@title').extract())
            publish_epoch = time_to_epoch(date,'%m/%d/%Y %I:%M %p')
            if publish_epoch ==False:
		import pdb;pdb.set_trace()
	    if publish_epoch:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            else:
                import pdb;pdb.set_trace()

            text = ' '.join(node.xpath('.//blockquote[@class="ipsQuote"]//@data-ipsquote-timestamp | .//blockquote[@class="ipsQuote"]//@data-ipsquote-username | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//strong//text() | .//pre[@class="ipsCode ipsCode prettyprint linenums:0 prettyprinted"]//a//span//text() | .//blockquote[@class="ipsQuote"]/text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//strong//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//text() | .//pre[@class ="ipsCode prettyprint"]//text() | .//img[@class = "ipsImage ipsImage_thumbnailed"]//@alt | //div[@class= "ipsType_normal ipsType_richText ipsContained"]//@alt | .//pre[@class= "ipsCode"]//text() | .//blockquote[@class="ipsQuote"]/@class | .//blockquote[@class="ipsQuote"]/div//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]/div//text()').extract()).replace('\n','').replace('\t','').encode('ascii', 'ignore') or 'Null'
            t_datetime = node.xpath('.//blockquote[@class="ipsQuote"]//div[@class ="ipsQuote_citation"]//a//text()').extract()

            t_date = node.xpath('.//blockquote[@class="ipsQuote"]//@data-ipsquote-timestamp').extract()
            t_auth = node.xpath('.//blockquote[@class="ipsQuote"]//@data-ipsquote-username').extract()

            if t_auth and t_date:
                for au,t in zip(set(t_auth),t_date):
                    a1 = au +" said:"
                    try:
                        t1 = int(t)
                        t1 = time.strftime("On %m/%d/%Y at %I:%M %p, ", time.localtime(t1)) + a1
                        text = text.replace(au, '').replace(t, t1).replace('ipsQuote','Quote ')
                    except:pass

            if t_auth:
                a1 = ''.join(t_auth)
                auth = a1 + " said:"
                text = text.replace(a1, auth).replace('ipsQuote','Quote Quote')

            elif 'ipsQuote' in text:
                text = text.replace('ipsQuote','Quote ')

            Links = node.xpath('.//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//a//@href | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//a[@target="_blank"]/@href').extract()
	    Link = []
            for link_ in Links:
                if 'http' not in link_ :
                    link = response.urljoin(link_)
                    Link.append(link)
                else:
                    Link.append(link_)

            all_links = ', '.join(Link)
            if all_links == '':
                all_links = 'Null'
            if Links == []:
                all_links = 'Null'
            reference_url = response.url
            if '&p=' in reference_url:
                test = re.findall('(.*)&page',reference_url)
                thread_url = ''.join(test)
            else:
                thread_url = reference_url
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
                'ord_in_thread':'',
                'post_url':post_url,
                'post_text':clean_text(text),
                'thread_title':thread_title,
                'thread_url':thread_url
                }
            json_posts = {
                          'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                          'hostname':'board.b-at-s.info',
                          'domain': domain,
                          'sub_type':'openweb',
                          'type':'forum',
                          'author':json.dumps(author_data),
                          'title':thread_title,
                          'text':clean_text(text),
                          'url':post_url,
                          'original_url':post_url,
                          'fetch_time':fetch_time(),
                          'publish_time':publish_epoch,
                          'link.url':all_links,
                          'post':post
                          }
	    sk = md5_val(post_url)
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts)
	    if author_url == 'Null':
		continue
            auth_meta = {'publish_epoch': publish_epoch}
            json_posts.update({
                    'post_id': post_id,
                    'auth_meta': json.dumps(auth_meta),
                    'crawl_status':0,
                    'links': author_url
                    })
            self.cursor.execute(self.query_status, json_posts)
            self.conn.commit()
        inner_nav = response.xpath('//li[@class="ipsPagination_next"]//a//@href').extract_first()
        if inner_nav:
           yield Request(inner_nav, callback=self.parse_next)
