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
        self.start_urls = self.cursor.execute("select post_url from safezone_crawl")
        post_url = self.cursor.fetchall()
        for url in post_url:
            yield Request(url[0], callback=self.parse_meta)
    def parse_meta(self, response):
        sel = Selector(response)
        category = ''.join(set(response.xpath('//a[contains(@href,"/categories/")]/span[@itemprop="name"]//text()[1]').extract())).strip() or 'Null'
        sub_category = ''.join(set(response.xpath('//a[contains(@href,"/forums/")]/span[@itemprop="name"]//text()[1]').extract())).encode('utf8').strip() or 'Null'
	sub_category_url = 'https://safezone.cc'+response.xpath('//li[@itemprop = "itemListElement"]//a//@href')[2].extract() or 'Null'
        thread_title = ''.join(response.xpath('//h1[@class="p-title-value"]/span[not(contains(@dir,"auto"))]/following-sibling::text()').extract()) or ''.join(response.xpath('//h1[@class="p-title-value"]//text()').extract())  or 'Null'
        thread_url = response.url.split('page')[0]  or 'Null'
        nodes = response.xpath('//article[@class="message message--post js-post js-inlineModContainer  "]')  or 'Null'
        next_page = ''.join(set(response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract())).strip()
        if next_page:
            next_page = urljoin("https://safezone.cc/",next_page)
            yield Request(next_page,callback=self.parse_meta)
        for node in nodes:
            json_values = {}
            post_title = ''  or 'Null'
            post_url = ''.join(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//li[1]//a[@rel="nofollow"]//@href').extract()).strip()  or 'Null'
	    ord_in_thread = ''.join(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//a//text()').extract()).replace('\n\t','').replace('#','').replace('\t','') or 'Null'
            post_url = urljoin("https://safezone.cc/", post_url)  or 'Null'
            post_id=post_url.split('-')[-1]  or 'Null'
            publish_time = ''.join(node.xpath('.//div[@class="message-attribution-main"]//@data-time').extract()).strip()  or 'Null'
	    if publish_time:
                month_year = time.strftime("%Y_%m", time.localtime(int(publish_time)))
            else:
                import pdb;pdb.set_trace()
            author = ''.join(node.xpath('.//h4[@class = "message-name"]//a//text() | .//h4[@class="message-name"]//span[@class="username "]//text()').extract())  or 'Null'
            author_url = ''.join(node.xpath('.//h4[@class = "message-name"]//a//@href').extract())  or 'Null'
            if author_url:
                author_url = urljoin("https://safezone.cc/", author_url)
            text ='\n'.join(node.xpath('.//div[@class = "bbWrapper"]//text() |.//div[@class="bbWrapper"]//img[contains(@data-src,"/data/attachments/")]//@alt | .//div[@class = "bbWrapper"]//img[@class="smilie"]//@alt |.//div[@class = "bbWrapper"]//img[contains(@src,".gif")]//@alt |.//div[@class="bbWrapper"]//img[contains(@src,"data:image/gif;")]//@alt  |.//h4[@class="block-textHeader"]//text() |.//blockquote[@class="bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote"]/@class').extract()).strip().replace('bbCodeBlock bbCodeBlock--expandable bbCodeBlock--quote','Quote')  or 'Null'
            text = clean_text(text)
            alllinks = node.xpath('.//div[@class = "bbWrapper"]//a[@class="link link--internal"]//@href | .//div[@class = "bbWrapper"]//img//@data-src | .//div[@class="attachment-name"]//a[@target="_blank"]//@href |.//div[@class = "bbWrapper"]//a[@class="bbCodeBlock-sourceJump"]//@href |.//div[@class = "bbWrapper"]//a[@class="link link--external"]//@href | .//div[@class="attachment-icon attachment-icon--img"]//img//@src |.//div[@class = "bbWrapper"]//a[@class="username"]//@href').extract()
            links=[]
            if alllinks:
                for link in alllinks:
                    if 'https:' not in link: 
                        links.append(urljoin("https://safezone.cc/", link))
                    else:
                        links.append(link)
	    else:
                links.append('Null')
	    author_data = {
                        'name':author,
                        'url':author_url
                        }

            json_posts = ({
		    'id' : post_url,
                    'hostname': 'www.safezone.cc',
                    'domain': "safezone.cc",
                    'sub_type':'openweb',
                    'type' : 'forum',
                    'author': json.dumps(author_data),
                    'title':clean_text(thread_title),
                    'text': text,
                    'url': response.url,
                    'original_url': response.url,
                    'fetch_time': fetch_time(),
                    'publish_time': publish_time,
                    'link_url': ', '.join(links),
                    'post':{
                        'cache_link':'',
			'author':json.dumps(author_data),
                    	'section':category,
                    	'language':'russian',
                    	'require_login':'false',
                   	'sub_section':sub_category,
                    	'sub_section_url':sub_category_url,
                   	'post_id': post_id,
                    	'post_title':post_title,
                    	'ord_in_thread': ord_in_thread,
                    	'post_url': post_url,
                    	'post_text':text,
                    	'thread_title':thread_title,
                    	'thread_url': thread_url
		    },
            })
	    pprint(json_posts)
            sk = md5_val(post_url)
	    #query={"query":{"match":{"_id":sk}}}
            #res = es.search(body=query)
            #if res['hits']['hits'] == []:
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts,request_timeout=30)
            #doc_to_es(id=sk,doc_type='post',body=json_posts)
	    '''else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
                    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)'''
