from mango.utils import *

class Cryptopro(Spider):
    name = "crypto_posts"
    start_urls = ["https://www.cryptopro.ru/forum2/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        url_que = "select distinct(post_url) from crypto_post_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_next)

    def parse_next(self, response):
        domain = "cryptopro.ru"
        category = response.xpath('//div[@class="yafPageLink breadcrumb"]//a//text()').extract()[1]
        sub_category = response.xpath('//div[@class="yafPageLink breadcrumb"]//a//text()').extract()[2].encode('utf8')
        thread_title = response.xpath('//span[@id="forum_ctl03_TopicTitle"]//text()').extract()
        nodes = response.xpath('//table[contains(@class,"content postContainer")] | //table[contains(@class,"content postContainer_")]')
        for node in nodes:
            if  "strong" not in node.extract():
                continue
            else:
                author = ''.join(node.xpath('.//a[@class="UserPopMenuLink"]//text()').extract()).strip()
                author_url = ''.join(node.xpath('.//a[@class="UserPopMenuLink"]//@href').extract())
                if author_url :
                    author_url = urljoin("https://www.cryptopro.ru", author_url)
                post_title = ''
                post_url = ''.join(node.xpath('.//strong//a[contains(@id,"post")]//@href').extract())
                if post_url:
                    post_url = urljoin("https://www.cryptopro.ru", post_url)
                post_id = ''.join(re.findall('#post(.*)',post_url))
                date = ''.join(node.xpath('.//div[@class="leftItem postedLeft"]//abbr[@class="timeago"]/@title').extract())
                date_ = ''.join(set(re.findall('\d+-\d+-\d+',date)))
                publish_epoch = time_to_epoch(date_,'%Y-%m-%d')
                if publish_epoch ==None:
                    date_ = ''.join(set(re.findall('\d+-\d+-\d+',date)))
                    publish_epoch = time_to_epoch(date_,'%Y-%m-%d')
                if publish_epoch ==None:
                    pass

		text = '\n'.join(node.xpath('.//div[@class="selectionQuoteable"]/text() | .//div[@class="spoilertitle"]//input//@value | .//div[@class="innerquote"]//strong//text() | .//div[@class="innerquote"]//text() | .//div[@class="spoilerbox"]//text() | .//div[@class="postdiv"]//img//@alt | .//span[@class="quotetitle"]//text() | .//a[@rel="nofollow"]//strong//text() | .//p[@class="MessageDetails"]//em//a//text() | .//p[@class="MessageDetails"]//abbr[@class="timeago"]//text() | .//span[@class="editedinfo"]//text() | .//div[@class="quote"]//span//@class | .//div[@class="innercode"]//text() | .//div[@class="code"]//strong//text() | .//div[@style="margin-left:40px"]//text() | .//div[@style="margin-left:40px"]//strong//text() | .//div[@class="postdiv"]//a[@rel="nofollow"]/text() | .//div[@class="selectionQuoteable"]//li//text()').extract()).replace('quotetitle','Quote')
                Links = node.xpath('.//div[@class="postdiv"]//img[not(contains(@src,"gif"))]//@src | .//div[@class="selectionQuoteable"]//a//@href |.//span[@class="quotetitle"]//a//@href | .//div[@class="selectionQuoteable"]//a[@rel="nofollow"]//@href | .//p[@class="MessageDetails"]//em//a/@href').extract()
                Link = []
                for link_ in Links:
		    if "#" == link_:
                        link = response.url + "#" 
                        Link.append(link)

                    elif 'http' not in link_ :
                        link = response.urljoin(link_)
                        Link.append(link)
		    else:
			Link.append(link_)
                   
                all_links = ', '.join(Link)
                reference_url = response.url
                if '&p=' in reference_url:
                    test = re.findall('(.*)&p',reference_url)
                    thread_url = ''.join(test)
                else:
                    thread_url = reference_url
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
                #pprint(json_posts)
                sk = md5_val(post_url)
		query={"query":{"match":{"_id":sk}}}
            	res = es.search(body=query)
            	if res['hits']['hits'] == []:		
                    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts) 
		else:
		    data_doc = res['hits']['hits'][0]
		    if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
			es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)

        inner_nav = response.xpath('//td[@align="left"]//span[@class="pagecurrent"]//following-sibling::a//@href').extract_first()
        if inner_nav:
            inner_nav = "https://www.cryptopro.ru" + inner_nav
            yield Request(inner_nav, callback=self.parse_next)
