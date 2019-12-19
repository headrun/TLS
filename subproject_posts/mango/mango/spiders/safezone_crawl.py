from mango.utils import *
query = generate_upsert_query_posts_crawl('safezone')

class Safezone(scrapy.Spider):
    name= "safezone_crawl"
    start_urls=["https://safezone.cc/"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close() 

    def parse(self, response):
        sel=Selector(response)
        url="https://safezone.cc/"
        urls = response.xpath('//h2[@class="block-header"]//@href').extract()
        for urlm in urls:
            urlm = urljoin("https://safezone.cc/",urlm)
            yield Request(urlm,callback = self.parse_next)

    def parse_next(self,response):
        sel=Selector(response)
        sur =  response.xpath('//h3[@class="node-title"]/a/@href').extract()
        for surl in sur:
            surl = urljoin("https://safezone.cc/",surl)
            yield Request(surl,callback = self.parse_thread_title)

    def parse_thread_title(self,response):
        sel=Selector(response)
        yur=  sel.xpath('//div[@class="structItem-title"]/a[@data-tp-primary="on"]/@href').extract()
        for yurl in yur:
            yurl = urljoin("https://safezone.cc/",yurl)
            sk = yurl.split('.')[-1]
            values =  {'sk':sk,'post_url':yurl,'crawl_status':0,'reference_url':response.url}
            self.cursor.execute(query,values)
            self.conn.commit()
        next_page= ''.join(set(sel.xpath('//a[@class = "pageNav-jump pageNav-jump--next"]/@href').extract()))
        if next_page:
            next_page = urljoin("https://safezone.cc/",next_page)
            yield Request(next_page,callback=self.parse_thread_title)
