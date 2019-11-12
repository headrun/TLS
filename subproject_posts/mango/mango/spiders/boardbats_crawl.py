from mango.utils import *

class Boardbats(scrapy.Spider):
    name = "boardbat_crawl"
    start_urls = ["https://board.b-at-s.info/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", host="localhost", user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()
        self.query_status = generate_upsert_query_posts_crawl('boardbat')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        main_urls = response.xpath('//div[@class="ipsDataItem_main"]//h4//a//@href').extract()
        for main_url in main_urls:
            yield Request(main_url, callback=self.parse_nxt)

    def parse_nxt(self, response):
        thread_urls = response.xpath('//span[@class="ipsType_break ipsContained"]//a//@href').extract()
        for post_url in thread_urls:
            sk = ''.join(re.findall('/topic/\d+', post_url)).replace('/topic/', '')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
             		}
            self.cursor.execute(self.query_status, json_posts)
        num = response.xpath('//li[@class="ipsPagination_next"]//a[@rel="next"]//@href').extract_first()
	if num:
            yield Request(num, callback=self.parse_nxt)






