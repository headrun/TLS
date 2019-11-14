from mango.utils import *

class Cracked(Spider):
    name = "cracked_crawl"


    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()
        self.query = generate_upsert_query_posts_crawl('cracked')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        scraper = cfscrape.create_scraper()
        res = scraper.get('https://cracked.to/')
        yield Request('https://cracked.to/', callback=self.parse_links)


    def parse_links(self, response):
        links = response.xpath('//tr[@class="forum"]//strong/a[contains(@class,"largetext")]/@href').extract()
        for link in links:
            link = urljoin('https://cracked.to/', link)
            yield Request(link, callback=self.parse_next)


    def parse_next(self, response):
        post_urls = response.xpath('//span[contains(@id, "tid_")]/a/@href').extract()
        for post_url in post_urls:
            post_url = urljoin('https://cracked.to/', post_url)
            sk = re.sub('(.*)/', '', post_url)
	    values = {
                    'sk':sk,
                    'post_url':post_url,
                    'crawl_status':0,
                    'reference_url':response.request.url
                    }
            self.cursor.execute(self.query, values)
        page_nav = response.xpath('//div[@class="pagination"]//a[contains(@class,"pagination_next")]/@href').extract_first()
        if page_nav:
            next_page = urljoin('https://cracked.to/', page_nav)
            yield Request(next_page, callback=self.parse_next)
