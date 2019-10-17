import cfscrape
from mango.utils import *

class Antichat(scrapy.Spider):
    name = "antichat_crawl"

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
       scraper = cfscrape.create_scraper()
       r = scraper.get('https://forum.antichat.ru/')
       #print r.content
       yield Request('https://forum.antichat.ru/',callback= self.parse_nxt) 

    def parse_nxt(self,response):
        urls = response.xpath('//div[@class="nodelistBlock nodeText"]//h3[@class="nodeTitle"]//a//@href').extract()
        for url in urls:
            url = "https://forum.antichat.ru/" + url
            yield Request(url,callback= self.parse_next)

    def parse_next(self,response):
        links = response.xpath('//div[@class="titleText"]//h3[@class="title"]//a//@href | //h3[@class="nodeTitle"]//a//@href').extract()
        for link in links:
            post_url = "https://forum.antichat.ru/" + link
            sk = ''.join(re.findall('\d+',post_url)[-1])
            query_status = generate_upsert_query_posts_crawl('antichat')
            json_posts = {'sk':sk,
                           'post_url':post_url,
                           'crawl_status':0,
                           'reference_url':response.url
            }
            self.cursor.execute(query_status, json_posts)
        page_navs = response.xpath('//div[@class="PageNav"]//a[contains(text(),"Next >")]/@href').extract_first()
	if page_navs:
	    page_navs = "https://forum.antichat.ru/" + page_navs
            yield Request(page_navs, callback=self.parse_next) 
