from mango.utils import *

class bitshacking(scrapy.Spider):
    name = "bitshacking_crawl"
    start_urls = ['https://bitshacking.com/']

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query_status = generate_upsert_query_posts_crawl('bitshacking')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        urls = response.xpath('//h3[@class="node-title"]//a//@href').extract()
        for url in urls:
            url = urljoin("https://bitshacking.com/",url)
            yield Request(url,callback = self.parse_next)

    def parse_next(self,response):
        thread_urls = response.xpath('//div[@class="structItem-title"]//a//@href').extract()
        for post_url in thread_urls:
            post_url = urljoin("https://bitshacking.com/",post_url)
            sk = post_url.split('.')[-1].replace('/','')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            pprint(json_posts)
            self.cursor.execute(self.query_status,json_posts)
        page_nav = response.xpath('//ul[@class="pageNav-main"]//a//@href').extract_first()
        if page_nav:
            page_nav = urljoin("https://bitshacking.com/",page_nav)
            yield Request(page_nav, callback = self.parse_next)
