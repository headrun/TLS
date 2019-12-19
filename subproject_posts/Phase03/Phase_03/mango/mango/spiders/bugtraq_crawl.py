from mango.utils import *


class Bugtraq(Spider):
    name = "bugtraq_crawl"
    start_urls = ["https://bugtraq.ru/forum/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query = generate_upsert_query_posts_crawl('bugtraq')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
	import pdb;pdb.set_trace()
        main_urls = set(response.xpath('//td[@class="text"]/a/@href').extract())
        for main_url in main_urls:
            main_url = urljoin('https://bugtraq.ru', main_url)
            yield Request(main_url, callback=self.parse_next)


    def parse_next(self, response):
        main_links = response.xpath('//li[@class="old"]/a[contains(@href, "?type=sb")]/@href').extract()
        for main_link in main_links:
            link = urljoin('https://bugtraq.ru', main_link)
            sk = re.sub('(.*)m=', '', link)
            json_posts = {'sk':sk,
                          'post_url':link,
                          'crawl_status':0,
                          'reference_url':response.request.url
             }
            self.cursor.execute(self.query, json_posts)
            self.conn.commit()
        page_nav = response.xpath('//td[@align="center"]//span[@class="ml"]/following-sibling::a/@href').extract_first()
        if page_nav:
            next_page = urljoin('https://bugtraq.ru', page_nav)
            yield Request(next_page, callback=self.parse_next)
