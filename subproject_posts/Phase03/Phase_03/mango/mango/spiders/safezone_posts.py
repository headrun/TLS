from mango.utils import *

class Safezone1(scrapy.Spider):
    name = "safezone_posts"
    start_urls = ['https://safezone.cc/']

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close() 

    def parse(self, response):
        self.start_urls = self.cursor.execute(
            "select post_url from safezone_crawl")
        post_url = self.cursor.fetchall()
        for url in post_url:
            yield Request(url[0], callback=self.parse_meta)
    def parse_meta(self, response):
        sel = Selector(response)
        category = ''.join(set(response.xpath('//a[contains(@href,"/categories/")]/span[@itemprop="name"]//text()[1]').extract())).strip()
        sub_category = ''.join(set(response.xpath('//a[contains(@href,"/forums/")]/span[@itemprop="name"]//text()[1]').extract())).encode('utf8').strip()
        thread_title = ''.join(response.xpath('//h1[@class="p-title-value"]/span[not(contains(@dir,"auto"))]/following-sibling::text()').extract()) or ''.join(response.xpath('//h1[@class="p-title-value"]//text()').extract())
        thread_url = response.url.split('page')[0]
        nodes = response.xpath('//article[@class="message message--post js-post js-inlineModContainer  "]')
        next_page = ''.join(set(response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract())).strip()
        if next_page:
            next_page = urljoin("https://safezone.cc/",next_page)
            yield Request(next_page,callback=self.parse_meta)
        for node in nodes:
            json_values = {}
            post_title = ''
            post_url = ''.join(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//li[1]//a[@rel="nofollow"]//@href').extract()).strip()
            post_url = urljoin("https://safezone.cc/", post_url)
            post_id=post_url.split('-')[-1]
            publish_time = ''.join(node.xpath('.//div[@class="message-attribution-main"]//@data-time').extract()).strip()
            author = ''.join(node.xpath('.//h4[@class = "message-name"]//a//text() | .//h4[@class="message-name"]//span[@class="username "]//text()').extract())      
            author_url = ''.join(node.xpath('.//h4[@class = "message-name"]//a//@href').extract())
            if author_url:
                author_url = urljoin("https://safezone.cc/", author_url)
            text ='\n'.join(node.xpath('.//div[@class = "bbWrapper"]//text() |.//div[@class="bbWrapper"]//img[contains(@data-src,"/data/attachments/")]//@alt | .//div[@class = "bbWrapper"]//img[@class="smilie"]//@alt |.//div[@class = "bbWrapper"]//img[contains(@src,".gif")]//@alt |.//div[@class="bbWrapper"]//img[contains(@src,"data:image/gif;")]//@alt  |.//h4[@class="block-textHeader"]//text() |.//blockquote[@class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote"]/@class').extract()).strip().replace('bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote','Quote')
            text = clean_text(text)
            alllinks = node.xpath('.//div[@class = "bbWrapper"]//a[@class="link link--internal"]//@href | .//div[@class = "bbWrapper"]//img//@data-src | .//div[@class="attachment-name"]//a[@target="_blank"]//@href |.//div[@class = "bbWrapper"]//a[@class="bbCodeBlock-sourceJump"]//@href |.//div[@class = "bbWrapper"]//a[@class="link link--external"]//@href | .//div[@class="attachment-icon attachment-icon--img"]//img//@src |.//div[@class = "bbWrapper"]//a[@class="username"]//@href').extract()
            links=[]
            if alllinks:
                for link in alllinks:
                    if 'https:' not in link: 
                        links.append(urljoin("https://safezone.cc/", link))
                    else:
                        links.append(link)
            json_posts = ({
                'domain': "safezone.cc",
                'category': category,
                'sub_category': sub_category,
                'thread_title': clean_text(thread_title),
                'thread_url': thread_url,
                'post_title': post_title,
                'post_url': post_url,
                'post_id': post_id,
                'author': author,
                'author_url': author_url,
                'publish_time': publish_time,
                'text': text,
                'links': ', '.join(links),
                'fetch_time': fetch_time(),
                'reference_url': response.url
            })
            sk = md5_val(post_url)
	    query={"query":{"match":{"_id":sk}}}
            res = es.search(body=query)
            if res['hits']['hits'] == []:
                es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
            #doc_to_es(id=sk,doc_type='post',body=json_posts)
	    else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
                    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
