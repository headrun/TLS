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
        category = response.xpath('//span[@itemprop="title"]//text()').extract()[0]  or 'Null'
        sub_category = response.xpath('//span[@itemprop="title"]//text()').extract()[1].encode('utf8')  or 'Null'
	sub_category_url = response.xpath('//span[@class = "crust"]//@href')[1].extract() or 'Null'
        thread_title = ''.join(response.xpath('//div[@class="titleBar"]//h1//text()').extract()).strip() or 'Null'
	#ord_in_thread = ''.join(response.xpath('//a[@class = "item muted postNumber hashPermalink OverlayTrigger"]//text()').extract()).replace('#','') or 'Null'
        nodes = response.xpath('//ol[@class="messageList"]//li[contains(@id,"post-")]') 
        for node in nodes:
            author = ''.join(node.xpath('.//div[@class="uix_userTextInner"]//span//text() |  .//div[@class="uix_userTextInner"]//a//text()').extract()) or 'Null'
            author_url = ''.join(node.xpath('.//div[@class="uix_userTextInner"]//a//@href').extract()) or 'Null'
	    ord_in_thread = ''.join(node.xpath('.//a[@class = "item muted postNumber hashPermalink OverlayTrigger"]//text()').extract()).replace('#','') or 'Null'
            if author_url:
                author_url =  urljoin("https://forum.antichat.ru/", author_url)
            post_title = ''  or 'Null'
            post = ''.join(node.xpath('.//a[@class="item muted postNumber hashPermalink OverlayTrigger"]//@data-href').extract()) 
            if post : 
                post_ = urljoin(response.url, post)
            post_url = post_.replace('permalink','').replace('posts/','#post-')
	    if post_url == '' or post == '':
		post_url = 'Null'
            post_id = ''.join(re.findall('\d+',post_url)[-1])  or 'Null'
            date = ''.join(node.xpath('.//a[@class="datePermalink"]//abbr[@class="DateTime"]//@title | .//a[@class="datePermalink"]//span[@class="DateTime"]//@title | .//a[@class="datePermalink"]/abbr/text()').extract())  or 'Null'
            date_ = ''.join(re.findall('(.*)at',date))
            publish_epoch = time_to_epoch(date_,'%d %b %Y ')
            if publish_epoch ==False:
                publish_epoch = time_to_epoch(date_,'%d %b %Y ')
            if publish_epoch:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            else:
		import pdb;pdb.set_trace()
            
            text = '\n'.join(node.xpath('.//div[@class="messageContent"]//text() | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img//@alt | .//div[@class="editDate item"]//text() | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//iframe//@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]/a/text() | .//div[@class="attribution type"]//text()').extract()).replace('said:','said:Quote').replace('\n','') or 'Null'
            Links = node.xpath('.//div[@class="messageContent"]//a[@class="externalLink ProxyLink"]//@href  | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img[not(contains(@class,"mceSmilieSprite mceSmilie"))]//@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]/img[@class="bbCodeImage LbImage"]/@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//iframe//@src | .//div[@class="attribution type"]//a/@href | .//div[@class="quote"]//img[not(contains(@class,"mceSmilieSprite mceSmilie1"))]//@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a//@href').extract() or 'Null'
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
	    if links == '':
		links = 'Null'
            reputation = ''.join(node.xpath('.//span[@class="brReputation"]//a//text()').extract())
            reference_url = response.url
            thread_url = response.url
            if 'page-' in thread_url:
                thread_url = re.findall('(.*)/page', thread_url)
            else:
                thread_url = reference_url
	    author_data= {
                        'name':author,
                        'url':author_url
                        }
            json_posts = {'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                    'hostname': 'forum.antichat.ru',
                    'domain': "forum.antichat.ru",
                    'sub_type':'openweb',
                    'type' : 'forum',
                    'author': json.dumps(author_data),
                    'title':clean_text(thread_title),
                    'text': text,
                    'url':post_url,
                    'original_url': post_url,
                    'fetch_time': fetch_time(),
                    'publish_time': publish_epoch,
                    'link_url':links,
                    'post':{
                        'cache_link':'',
                        'author': json.dumps(author_data),
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
                    	'thread_url': thread_url,
		    	'reputation': reputation
                    },

            }
            sk = md5_val(post_url)
	    #query={"query":{"match":{"_id":sk}}}
            #res = es.search(body=query)
            #if res['hits']['hits'] == []:		
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts)
	    '''else:
		data_doc = res['hits']['hits'][0]
		if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)'''

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

