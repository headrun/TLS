from mango.utils import *

class Virusinfo(scrapy.Spider):
    name = "virusinfo_crawl"
    start_urls = ["https://virusinfo.info/forumdisplay.php?f=92"]
    
    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query = generate_upsert_query_posts_crawl('virusinfo')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        main_urls=response.xpath('//h2[@class="forumtitle"]//a//@href').extract()
        for main_url in main_urls:
            main_url = urljoin("https://virusinfo.info/", main_url)
            yield Request(main_url,callback= self.parse_nxt)

    def parse_nxt(self, response):
        thread_urls = response.xpath('//h3[@class="threadtitle"]//a//@href | //div[@class="announcement"]//dl//dd//a//@href').extract()
        for thread_url in thread_urls:
            post_url = urljoin("https://virusinfo.info/", thread_url)
            sk = ''.join(re.findall('t=\d+',post_url)).replace('t=','')
            json_posts = {'sk':sk,
                          'post_url':post_url,
                          'crawl_status':0,
                          'reference_url':response.url
             }
            self.cursor.execute(self.query, json_posts)
            self.conn.commit()
        num = response.xpath('//div[@class="threadpagenav"]//span/a[@rel="next"]/@href').extract_first()
	if num:
            num_ =  urljoin("https://virusinfo.info/", num)
            yield Request(num_, callback=self.parse_nxt)

