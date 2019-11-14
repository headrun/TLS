from mango.utils import *


class Defendtheweb(Spider):
    name = "defendtheweb_crawl"
    start_urls = ["https://defendtheweb.net/discussions"]


    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        self.query = generate_upsert_query_posts_crawl('defendtheweb')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()



    def parse(self, response):
        links = response.xpath('//div[@class="feed-item-header"]//div[@class="feed-item-details"]//a[@data-text="title"]/@href').extract()
        for link in links:
            sk = re.sub('(.*)/', '', link)
            json_posts = {
                    'sk':sk,
                    'post_url':link,
                    'crawl_status':0,
                    'reference_url':response.request.url
                    }
            self.cursor.execute(self.query, json_posts)
        pagenav = ''.join(response.xpath('//div[@class="pagination"]/a[contains(text(),"More")]/@href').extract())
        if pagenav:
            nav = urljoin('https://defendtheweb.net', pagenav)
            yield Request(nav, callback=self.parse)
