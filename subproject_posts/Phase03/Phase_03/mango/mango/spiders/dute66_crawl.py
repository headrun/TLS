from mango.utils import *


class Dute66(Spider):
    name = 'dute66_crawl'
    start_urls = ['https://www.dute56.com/']
    custom_settings = {
	'CONCURRENT_REQUESTS_PER_DOMAIN':1,
	'DOWNLOADER_MIDDLEWARES':{'mango.middlewares.MangoDownloaderMiddleware': 543}
	
	}

    que = generate_upsert_query_posts_crawl('dute66')
    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",
                                    host="localhost",
                                    user=DATABASE_USER,
                                    passwd=DATABASE_PASS,
                                    use_unicode=True,
                                    charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        forums = response.xpath('//div[@class="fl_icn_g"]//a[contains(@href,"forum")]/@href').extract()
        for forum in forums:
            yield Request(forum,callback = self.parse_forums)

    def parse_forums(self,response):
        next_page = response.xpath('//div[@class="pg"]//a[@class="nxt"]/@href').extract_first()
        if next_page:
            yield Request(next_page,callback = self.parse_forums)
        threads = response.xpath('//table[@id="threadlisttableid"]//th[@class="common"]//a[contains(@href,"thread")]/@href | \
		//tbody[contains(@id,"normalthread_")]//th//a[contains(@href,"thread")]/@href ').extract()
        for thread in threads:
            val= {
                    'sk': re.sub('(.*)thread-','',thread).replace('.html',''),
                    'post_url':thread,
                    'crawl_status':0,
                    'reference_url':response.url
                    }
            self.cursor.execute(self.que,val)
