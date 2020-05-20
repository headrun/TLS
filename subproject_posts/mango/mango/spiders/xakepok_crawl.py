from mango.utils import *

class xakepok(scrapy.Spider):
    name = 'xakepok_crawl'
    start_urls = ['https://forum.xakepok.net']

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query_status = generate_upsert_query_posts_crawl('xakepok')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        urls = response.xpath('//td[contains(@align,"left")]//following-sibling::div/a/@href | //div[@class="smallfont"]//tr//a/@href').extract()
        for url in urls:
            url = urljoin("https://forum.xakepok.net/",url)
            yield Request(url,callback=self.parse_next)

    def parse_next(self,response):
        thread_urls = response.xpath('//a[contains(@id ,"thread_title_")]//@href').extract()
        for thread in thread_urls:
            post_url = urljoin("https://forum.xakepok.net/",thread)
            sk = post_url.split('=')[-1]
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(self.query_status, json_posts)
        inner_nav = response.xpath('//a[@class="smallfont"]//@href').extract_first()
        if inner_nav:
            inner_nav_ = urljoin("https://forum.xakepok.net/",inner_nav)
            yield Request(inner_nav_,callback=self.parse_next)

