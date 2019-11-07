from mango.utils import *

class sinister(scrapy.Spider):
    name = 'sinister_crawl'
    start_urls = ['https://sinister.ly']

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query_status = generate_upsert_query_posts_crawl('sinister')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        urls =  response.xpath('//tbody[@id="cat_4_e"]//td[contains(@class,"trow")]//strong//a//@href').extract()
        for url in urls:
            if ("Forum-Upgraded-Lounge--85" not in url) and ("Forum-Not-Safe-For-Work--839" not in url) and ("Forum-Giveaways-Freebies" not in url):
                url = urljoin("https://sinister.ly/",url)
                yield Request(url,callback = self.parse_next)

    def parse_next(self,response):
        thread_urls = response.xpath('//a[contains(@id, "thread_")]//@href').extract()
        for thread in thread_urls:
            post_url = urljoin("https://sinister.ly/",thread)
            sk = post_url.split('/')[-1]
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
             }
            self.cursor.execute(self.query_status,json_posts)
        page_nav = response.xpath('//a[@class="pagination_next"]//@href').extract_first()
        if page_nav:
            page_navi = urljoin("https://sinister.ly/",page_nav)
            yield Request(page_navi,callback = self.parse_next)
