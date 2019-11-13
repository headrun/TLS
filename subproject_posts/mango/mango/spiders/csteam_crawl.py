from mango.utils import *

class csteam(scrapy.Spider):
    name = 'csteam_crawl'
    start_urls = ['https://cs-team.ir/forums/']

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query_status = generate_upsert_query_posts_crawl('csteam')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        urls = response.xpath('//h4[@class="ipsDataItem_title ipsType_large ipsType_break"]//a//@href').extract()
        for url in urls:
            yield Request(url,callback=self.parse_next)

    def parse_next(self,response):
        thread_urls = response.xpath('//span[contains(@class,"ipsType_break ipsContained")]//a//@href').extract()
        for thread in thread_urls:
            post_url = thread
            sk= ''.join(re.findall('\d\d\d\d',post_url))
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
            }
            self.cursor.execute(self.query_status,json_posts)


