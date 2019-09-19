from mango.utils import *

class Cryptopro(scrapy.Spider):
    name = "crypto_crawl"
    start_urls = ["https://www.cryptopro.ru/forum2/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query_status = generate_upsert_query_posts_crawl('crypto')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        main_urls=response.xpath('//div[@class="forumheading"]//a//@href').extract()
        for main_url in main_urls:
            main_url = urljoin("https://www.cryptopro.ru", main_url)
            yield Request(main_url,callback= self.parse_nxt)

    def parse_nxt(self, response):
        thread_urls = response.xpath('//td[@class="topicMain"]//a[@class="post_link"]/@href').extract()
        for thread_url in thread_urls:
            post_url = urljoin("https://www.cryptopro.ru", thread_url)
            sk = ''.join(re.findall('&t=\d+',post_url)).replace('&t=','')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
             }
            self.cursor.execute(self.query_status, json_posts)
        num = response.xpath('//td[@align="left"]//span[@class="pagecurrent"]//following-sibling::a//@href').extract_first()
	if num:
            num_ =  urljoin("https://www.cryptopro.ru", num)
            yield Request(num_, callback=self.parse_nxt)







