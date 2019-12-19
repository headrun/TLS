from onionwebsites.utils import *
query = generate_upsert_query_posts_crawl('darkmoney')



class Darkmoney(Spider):
    name = "darkmoney"
    start_urls = ['https://y3pggjcimtcglaon.onion/']

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)
       
    def close_conn(self, spider):
	self.conn.commit()
        self.conn.close()
    
    def parse(self, response):
        main_urls = response.xpath('//tbody[contains(@id,"collapseobj_forumbit_")]//tr[@align="center"]//td[@class="alt1Active" and @align="left"]//a/@href').extract()
        for url in main_urls:
            url = urljoin("https://y3pggjcimtcglaon.onion/" , url)
            yield Request(url, callback = self.parse_next)


    def parse_next(self, response):
        page_nav = response.xpath('//table[@class="tborder"]//td[@class="alt1"]//a[@rel="next"]/@href').extract_first()
        if page_nav:
            page_nav = urljoin("https://y3pggjcimtcglaon.onion/", page_nav)
            yield Request(page_nav,callback = self.parse_next)
        thread_urls = response.xpath('//tbody[contains(@id,"threadbits_forum")]//td[contains(@id,"td_threadtitle")]//a[contains(@id,"thread_title")]/@href').extract()
        for thread in thread_urls:
            th = thread.split('/')[-2]
	    sk = re.sub('(.*)-','',th)
            thread = urljoin("https://y3pggjcimtcglaon.onion/",thread)
            data = {
                    'sk':sk,
                    'post_url':thread,
                    'crawl_status':0,
                    'reference_url':response.url
                    }
            self.cursor.execute(query,data)
