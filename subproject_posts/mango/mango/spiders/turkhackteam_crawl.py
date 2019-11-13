from mango.utils import *

class Turkhackteam(scrapy.Spider):
    name = "turkhackteam_crawl"
    start_urls = ["https://www.turkhackteam.org/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self,response):
        urls = response.xpath('//div[@class="forum-row"]//a[@class="forum-2-name"]//@href').extract()
        for url in urls:
            if url not in ['https://twitter.com/thtstatus']:
                yield Request(url,callback= self.parse_next)

    def parse_next(self,response):
        links = response.xpath('//div//a[contains(@id,"thread_title_")]//@href').extract()
        for link in links:
            sk = ''.join(re.findall('\d+(.*).html',link))
            query_status = generate_upsert_query_posts_crawl('turkhackteam')
            json_posts = {'sk':sk,
                           'post_url':link,
                           'crawl_status':0,
                           'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
        page_navs = response.xpath('//div[@class="pagenav"]//a[@rel="next"]//@href').extract_first()
        if page_navs:
            page_navs = "https://www.turkhackteam.org/" + page_navs
            page_nav = ''.join(re.findall('(.*)" title',page_navs))
            yield Request(page_nav, callback=self.parse_next)
