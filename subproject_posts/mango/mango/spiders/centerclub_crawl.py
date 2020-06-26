from mango.utils import *

class Centerclub(scrapy.Spider):
    name = "centerclub_crawl"
    start_urls = ["https://center-club.ws/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8') 
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self,response):
        urls = response.xpath('//div[@class="node-main js-nodeMain"]//h3[@class="node-title"]//a//@href').extract()
        for url in urls:
            url = "https://center-club.ws" + url
            yield Request(url,callback= self.parse_next)

    def parse_next(self,response):
        links = response.xpath('//div[@class="structItem-title"]//a[@data-tp-primary="on"]//@href').extract()
        for link in links:
            link = "https://center-club.ws" + link
            sk = ''.join(re.findall('\d+',link)[-1])
            query_status = generate_upsert_query_posts_crawl('centerclub')
            json_posts = {'sk':sk,
                           'post_url':link,
                           'crawl_status':0,
                           'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
        page_navs = response.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract_first()
        if page_navs:
            page_navs = "https://center-club.ws" + page_navs
            yield Request(page_navs, callback=self.parse_next)
