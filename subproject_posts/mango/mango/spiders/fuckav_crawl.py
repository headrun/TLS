from mango.utils import *



class Fuckav(Spider):
    name= "fuckav_crawl"
    start_urls=["https://fuckav.ru/"]
    custom_settings = {
	'COOKIES_ENABLED':True
	}

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts',host='localhost',user=DATABASE_USER,passwd=DATABASE_PASS, use_unicode = True, charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.query = generate_upsert_query_posts_crawl('fuckav')
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        yield scrapy.Request('https://fuckav.ru/',callback = self.next_page)

    def next_page(self,response):
        cooks = re.sub('";documen(.*)','',re.sub('(.*)cookie="','',''.join(response.xpath('//script//text()').extract()))).split('=')
        self.cook = {cooks[0]:cooks[1]}
        yield scrapy.Request('https://fuckav.ru/', callback=self.meta_data, cookies = self.cook, dont_filter=True )

    def meta_data(self,response):
        sur= response.xpath('//td[@class="boxcolorbar"]//a[contains(@href,"forumdisplay")]//@href').extract()
        for surl in sur:
            surl = urljoin("https://fuckav.ru/", surl)
            yield Request(surl, callback=self.meta_data1)

    def meta_data1(self,response):
        sur= response.xpath('//td[@class="alt1Active"]//a[contains(@href,"forumdisplay")]//@href').extract()
        for surl in sur:
            surl = urljoin("https://fuckav.ru/",surl)
            yield Request(surl, callback=self.meta_data2)
            
    def meta_data2(self,response):
        sur= response.xpath('//div[contains(@id, "hint")]//a[contains(@id,"thread_title_")]/@href').extract()
        for surl in sur:
            surl = urljoin("https://fuckav.ru/",surl)
            surl = surl.split('#')[0]
            surl = surl.split('&')[0]
            sk = surl.split('=')[-1]
            values =  {
                    'sk':sk,
                    'post_url':surl,
                    'crawl_status':0,
                    'reference_url':response.url
                    }
            self.cursor.execute(self.query, values)
            self.conn.commit()
        next_page = response.xpath('//div[@class="pagenav"]//td/a[@rel="next"]/@href').extract_first()            
        if next_page:
            page_nav = urljoin("https://fuckav.ru/", next_page)
            yield Request(page_nav, callback=self.meta_data2)
