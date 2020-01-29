import cfscrape
from mango.utils import *

class Antichat(scrapy.Spider):
    name = "antichat_posts"
       
    def __init__(self):
        self.conn =MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode=True, charset='utf8')
        self.cursor= self.conn.cursor()
        
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
       scraper = cfscrape.create_scraper()
       r = scraper.get('https://forum.antichat.ru/')
       #print r.content
       yield Request('https://forum.antichat.ru/',callback= self.parse_nxt)

    def parse_nxt(self,response):
        url_que = "select distinct(post_url) from antichat_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_ann)


    def parse_ann(self, response):
        sel = Selector(response)
        domain = "forum.antichat.ru"
        category = response.xpath('//span[@itemprop="title"]//text()').extract()[0]
        sub_category = response.xpath('//span[@itemprop="title"]//text()').extract()[1].encode('utf8')
        thread_title = ''.join(response.xpath('//div[@class="titleBar"]//h1//text()').extract()).strip()
        nodes = response.xpath('//ol[@class="messageList"]//li[contains(@id,"post-")]') 
        for node in nodes:
            author = ''.join(node.xpath('.//div[@class="uix_userTextInner"]//span//text() |  .//div[@class="uix_userTextInner"]//a//text()').extract())
            author_url = ''.join(node.xpath('.//div[@class="uix_userTextInner"]//a//@href').extract())
            if author_url:
                author_url =  urljoin("https://forum.antichat.ru/", author_url)
            post_title = ''
            post = ''.join(node.xpath('.//a[@class="item muted postNumber hashPermalink OverlayTrigger"]//@data-href').extract())
            if post : 
                post_ = urljoin(response.url, post)
            post_url = post_.replace('permalink','').replace('posts/','#post-')     
            post_id = ''.join(re.findall('\d+',post_url)[-1])
            date = ''.join(node.xpath('.//a[@class="datePermalink"]//abbr[@class="DateTime"]//@title | .//a[@class="datePermalink"]//span[@class="DateTime"]//@title | .//a[@class="datePermalink"]/abbr/text()').extract())
            date_ = ''.join(re.findall('(.*)at',date))
            publish_epoch = time_to_epoch(date_,'%d %b %Y ')
            if publish_epoch ==False:
                publish_epoch = time_to_epoch(date_,'%d %b %Y ')
                import pdb;pdb.set_trace()
            
            text = '\n'.join(node.xpath('.//div[@class="messageContent"]//text() | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img//@alt | .//div[@class="editDate item"]//text() | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//iframe//@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]/a/text() | .//div[@class="attribution type"]//text()').extract()).replace('said:','said:Quote')
            Links = node.xpath('.//div[@class="messageContent"]//a[@class="externalLink ProxyLink"]//@href  | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img[not(contains(@class,"mceSmilieSprite mceSmilie"))]//@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]/img[@class="bbCodeImage LbImage"]/@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//iframe//@src | .//div[@class="attribution type"]//a/@href | .//div[@class="quote"]//img[not(contains(@class,"mceSmilieSprite mceSmilie1"))]//@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a//@href').extract()
            Link = []
            for link_ in Links:
                if 'http' not in link_ :
                    link = urljoin("https://forum.antichat.ru/", link_)
                    Link.append(link)
                elif "?image" in link_ :
                    link = urljoin("https://forum.antichat.ru/", link_)
                    Link.append(link)
		else:
	            Link.append(link_)
            
            links = ', '.join(Link)
            reputation = ''.join(node.xpath('.//span[@class="brReputation"]//a//text()').extract())
            reference_url = response.url
            thread_url = response.url
            if 'page-' in thread_url:
                thread_url = re.findall('(.*)/page', thread_url)
            else:
                thread_url = reference_url
            json_posts = {'domain': domain,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'fetch_time': int(datetime.datetime.now().strftime("%s")) * 1000,
                          'post_title' : post_title,
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_time': publish_epoch,
                          'author': author,
                          'author_url': author_url,
                          'text': text,
                          'links': links,
            }
            sk = md5_val(post_url)
	    query={"query":{"match":{"_id":sk}}}
            res = es.search(body=query)
            if res['hits']['hits'] == []:		
                es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
	    else:
		data_doc = res['hits']['hits'][0]
		if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)

            auth_meta = {'publish_time': publish_epoch,'reputation':reputation}
            json_posts.update({
                    'post_id': post_id,
                    'auth_meta': json.dumps(auth_meta),
                    'crawl_status':0,
                    'links': author_url
                    })
            query = generate_upsert_query_authors_crawl('antichat')
            self.cursor.execute(query, json_posts)
            self.conn.commit()
        inner_nav = response.xpath('//div[@class="PageNav"]//a[contains(text(),"Next >")]/@href').extract_first()
        if inner_nav:
            inner_nav = urljoin("https://forum.antichat.ru/", inner_nav)
            yield Request(inner_nav, callback=self.parse_ann)

