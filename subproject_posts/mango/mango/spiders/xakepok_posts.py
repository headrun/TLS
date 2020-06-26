from mango.utils import *

class xakepok(scrapy.Spider):
    name = "xakepok_posts"
    start_urls = ['https://forum.xakepok.net/']

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        url_que = "select distinct(post_url) from xakepok_post_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0],callback=self.parse_next)
   
    def parse_next(self,response):
        domain = "forum.xakepok.net"
        category = response.xpath('//span[@class="navbar"]//a//text()').extract()[1] or 'Null'
        sub_category = response.xpath('//span[@class="navbar"]//a//text()').extract()[2] or 'Null'
        sub_category_url = "https://forum.xakepok.net/" + response.xpath('//span[@class="navbar"]//a//@href').extract()[1] or 'Null'
        thread_title = ''.join(response.xpath('//span[@class="smallfont"]//text() | //td[@class="alt1"]/strong/text()').extract()) or 'Null'
        inner_nav = response.xpath('//td[@class="alt1"]//a[@rel="next"]//@href').extract_first()
        if inner_nav:
            inner_nav_ = urljoin("https://forum.xakepok.net/",inner_nav)
            yield Request(inner_nav_,callback=self.parse_next)
        nodes = response.xpath('//div[@id="posts"]//div[contains(@class,"tborder_big_right")]')
        for node in nodes:
            author =''.join(node.xpath('.//a[@class="bigusername"]//text()').extract()) or 'Null'
            author_url = "https://forum.xakepok.net/"+''.join(node.xpath('.//a[@class="bigusername"]//@href').extract()) or 'Null'
            post_title = ''.join(node.xpath('.//table[@class="tborder"]//td/h1[@class="threadtitle"]//text() | .//table[@class="tborder"]//td/div[@class="smallfont"]/strong/text() | .//table[@class="tborder"]//td/h1[@class="threadtitle"]//@alt | .//table[@class="tborder"]//td/div[@class="smallfont"]/img/@alt').extract()) or 'Null'
            post_url = "https://forum.xakepok.net/" + ''.join(node.xpath('.//a[@target="new"]//@href').extract()) or 'Null'
            post_id = ''.join(re.findall('&p=(.*&)',post_url)).replace('&','') or 'Null' 
            ord_in_thread = ''.join(node.xpath('.//a[@target="new"]//text()').extract()).replace('#','') or 'Null'
            date = ' '.join(node.xpath('.//div[@class="tborder_big_right"]//table[@class="tborder"]//td[@class="thead"]/text() | .//table[contains(@id,"post")]/tr/td[@class="thead"]/text()').extract()).replace('\n\n','').replace('\n','').encode('utf-8').strip().replace(' \xc2','').replace('\xa0#','')
            date_=  ''.join(set(re.findall('\d+.\d+.\d+',date)))
            publish_epoch = time_to_epoch(date_,'%d.%m.%Y')
            if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
                    month_year =  time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                else:
                    continue
            else:
                publish_epoch = 'Null'
            text = ''.join(node.xpath('.//div[contains(@id,"post_message_")]//td//text() | .//div[contains(@id,"post_message_")]//text() | .//div[contains(@id,"post_message_")]//@alt | .//div[contains(@id,"post_message_")]//div[contains(@class,"smallfont")]//@class | .//div[contains(@id,"post_message_")]//img//@alt | .//div[contains(@id,"post_message_")]//img//@title').extract()).replace('\n','').replace('smallfont','Quote ') or 'Null'
            Links = ', '.join(node.xpath('.//div[contains(@id,"post_message_")]//a//@href | .//div[@class="smallfont"]/img[not(contains(@class,"inlineimg"))]/@src | .//div[contains(@id,"post_message_")]/img[not(contains(@class,"inlineimg"))]/@src').extract()) or 'Null'
            reference_url = response.url
            if '&page=' in reference_url:
                test = re.findall('&page=\d+',reference_url)
                thread_url = reference_url.replace(''.join(test),"")
            else:
                thread_url = reference_url

            author_data = {
                    'name':author,
                    'url':author_url
                    }
            post  =   {
                            'cache_link':'',
                            'author': json.dumps(author_data),
                            'section':category,
                            'language':'russian',
                            'require_login':'false',
                            'sub_section':sub_category,
                            'sub_section_url': sub_category_url,
                            'post_id': post_id,
                            'post_title':post_title,
                            'ord_in_thread': ord_in_thread,
                            'post_url': post_url,
                            'post_text':text,
                            'thread_title':thread_title,
                            'thread_url': thread_url
                    }
            json_posts = {
                        'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                        'hostname': 'forum.xakepok.net',
                        'domain':domain,
                        'sub_type':'openweb',
                        'type' : 'forum',
                        'author': json.dumps(author_data),
                        'title':thread_title,
                        'text': text,
                        'url': post_url,
                        'original_url': post_url,
                        'fetch_time': fetch_time(),
                        'publish_time': publish_epoch,
                        'link.url':Links,
                        'post':post
            }
            sk = md5_val(post_url)
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts)
            auth_meta = {'publish_time':publish_epoch}
            json_posts.update({
                    'post_id': post_id,
                    'auth_meta': json.dumps(auth_meta),
                    'crawl_status':0,
                    'links': author_url
                    })
            query = generate_upsert_query_authors_crawl('xakepok')
            self.cursor.execute(query, json_posts)
            self.conn.commit()
