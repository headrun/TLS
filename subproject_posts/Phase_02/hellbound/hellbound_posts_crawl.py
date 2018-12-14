import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.selector import Selector
import utils
import MySQLdb

QUE = utils.generate_upsert_query_posts_crawl('hellbound')

class Hellbound(Spider):
    name="hellbound_thread_crawl"
    start_urls = ["https://www.hellboundhackers.org/forum/index.php"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="hellbound",host="localhost",user="root",passwd="" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        forums = response.xpath('//h2[@class="hlink"]//a//@href').extract()
        for forum in forums:
            yield Request("https://www.hellboundhackers.org/forum/"+forum, callback = self.parse_forum)


    def parse_forum(self,response):
        threads = response.xpath('//h2[@class="hlink"]//a//@href').extract()
        for thread in threads:
            json_val = {}
            thread_url = "https://www.hellboundhackers.org/forum/"+thread
            json_val = {
                    'sk': thread.replace('.html',''),
                    'post_url': thread_url,
                    'crawl_status': 0,
                    'reference_url': response.url,
                    }
            self.cursor.execute(QUE,json_val)
        try:
            nextpage = response.xpath('//td[@class="tbl2"]/a[contains(text(),">")]/@href').extract()[0].replace('./','https://www.hellboundhackers.org/forum/')
            yield Request(nextpage, callback = self.parse_forum, meta = {'crawl_type':'catch up'})
        except:pass
