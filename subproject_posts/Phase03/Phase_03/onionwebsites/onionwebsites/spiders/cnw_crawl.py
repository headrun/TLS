from onionwebsites.utils import *
query = generate_upsert_query_posts_crawl('cnw')


class CNW(scrapy.Spider):
    name = "cnw_crawl"
    start_urls = ["http://cnw4evazulooacab.onion/?tckattempt=1"]

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        main_urls=response.xpath('//div[@class="ipsDataItem_main"]//h4[@class="ipsDataItem_title ipsType_large ipsType_break"]//a/@href').extract()
        for main_url in main_urls:
            yield Request(main_url,callback= self.parse_nxt)

    def parse_nxt(self, response):
        thread_urls = response.xpath('//span[@class="ipsType_break ipsContained"]//a/@href').extract()
        for post_url in thread_urls:
            sk = ''.join(re.findall('topic/\d+',post_url)).replace('topic/','')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
                 }
            self.cursor.execute(query, json_posts)
        inner_nav = response.xpath('//li[@class="ipsPagination_next"]//a/@href').extract_first()
        if inner_nav:
            yield Request(inner_nav,callback= self.parse_nxt)
