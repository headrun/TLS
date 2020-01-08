from mango.utils import *

class bitshacking(scrapy.Spider):
    name = 'bitshacking_posts'
    start_urls = ['https://bitshacking.com/']

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        url_que = "select distinct(post_url) from bitshacking_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0],callback = self.parse_next)
    def parse_next(self,response):
        domain = 'bitshacking.com'
        category = response.xpath('//span[@itemprop="name"]//text()').extract()[1]
        sub_category =  response.xpath('//span[@itemprop="name"]//text()').extract()[2]
        sub_category_url =  response.xpath('//a[@itemprop="item"]//@href').extract()[2]
        if sub_category_url:
            sub_category_url = urljoin('https://bitshacking.com/',sub_category_url)
        else:
            sub_category_url = 'Null'
        thread_title = ''.join(response.xpath('//h1[@class="p-title-value"]//text()').extract())
        nodes = response.xpath('//div[@class="message-inner"]')
        for node in nodes:
            author = ''.join(node.xpath('.//span[@class="username--style2"]//text()').extract())
            author_url = 'https://bitshacking.com/'+''.join(node.xpath('.//h4[@class="message-name"]//@href').extract())
            ord_in_thread = ''.join(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//a//text()').extract()).replace('#','').replace('\t','').replace('\n','')
            post_url = 'https://bitshacking.com/'+node.xpath('.//a[@rel="nofollow"]//@href').extract()[1]
            post_id = ''.join(re.findall('post-(.*)',post_url)) or 'Null'
            post_title = '' or 'Null'
            publish_epoch = node.xpath('.//time[@class="u-dt"]//@data-time').extract()[0]
            if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
                    month_year = time.strftime("%m-%Y", time.localtime(int(publish_epoch)))
                else:
                    continue
            else:
                publish_epoch = 'Null'
            text =''.join(node.xpath('.//article[@class="message-body js-selectToQuote"]//text() | .//div[@class = "bbWrapper"]//@alt | .//div[@class="bbWrapper"]//blockquote/div/@class').extract()).replace('\n','').replace('\t','').replace(u'\xa0','').replace('bbCodeBlock-title','Quote ')
            links = ', '.join(node.xpath('.//div[@class="bbWrapper"]//a//@href').extract())
            reference_url = response.url
            if '/page' in reference_url:
                test = re.findall('(.*)/page',reference_url)
                thread_url = ''.join(test)
            else:
                thread_url = reference_url
            author_data = {
                    'name':author,
                    'url':author_url
                    }
            json_posts = {
                        'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                        'hostname': 'bitshacking.com',
                        'domain':"bitshacking.com",
                        'sub_type':'openweb',
                        'type' : 'forum',
                        'author': json.dumps(author_data),
                        'title':thread_title,
                        'text': text,
                        'url': post_url,
                        'original_url': post_url,
                        'fetch_time': fetch_time(),
                        'publish_time': publish_epoch,
                        'link.url':links,
                        'post':{
                            'cache_link':'',
                            'author': json.dumps(author_data),
                            'section':category,
                            'language':'english',
                            'require_login':'false',
                            'sub_section':sub_category,
                            'sub_section_url': sub_category_url,
                            'post_id': post_id,
                            'post_title':post_title,
                            'ord_in_thread': ord_in_thread,
                            'post_url': post_url,
                            'post_text':text,
                            'thread_title':thread_title,
                            'thread_url': thread_url,
                    },
            }
            sk = md5_val(post_url)
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts)
        page_nav = response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract_first()
        if page_nav:
            page_nav = urljoin("https://bitshacking.com/",page_nav)
            yield Request(page_nav,callback = self.parse_next)
