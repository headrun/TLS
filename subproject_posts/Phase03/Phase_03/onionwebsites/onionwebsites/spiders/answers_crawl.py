from onionwebsites.utils import *


class Answers(Spider):
    name = "answers_crawl"
    #start_urls = ['http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion']
    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()
    def start_requests(self):
	headers = {
    'Host': 'answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:60.0) Gecko/20100101 Firefox/60.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}
	yield Request('http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion',callback = self.parse,headers = headers, errback=self.pte)

    def pte(self,response):
	import pdb;pdb.set_trace()

    def parse(self, response):
	import pdb;pdb.set_trace()
        main_links = response.xpath('//div[@class="qa-nav-main"]//ul//li//a[@class="qa-nav-main-link"]/@href').extract()
        for main_link in main_links:
            if ('./users' not in main_link) and ('./ask' not in main_link) and ('./rules' not in main_link) and ('./contact-us' not in main_link):
                main_link = main_link.replace('./', 'http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion/')
                yield Request(main_link, callback=self.parse_next)


    def parse_next(self, response):
        if '/tags' in response.url:
            tag_links = response.xpath('//span[@class="qa-top-tags-label"]//a[@class="qa-tag-link"]/@href').extract()
            for tag_link in tag_links:
                tag_link = tag_link.replace('../', 'http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion/')
                yield Request(tag_link, callback=self.parse_meta)
            page_nav = ''.join(response.xpath('//div[@class="qa-page-links"]//ul//li//a[@class="qa-page-next"]/@href').extract()).replace('../', 'http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion/')
            if page_nav:
                yield Request(page_nav, callback=self.parse_next)
        else:
            links = response.xpath('//div[@class="qa-q-list-item"]//div[@class="qa-q-item-title"]//a/@href').extract()
            for link in links:
                link = link.replace('../', 'http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion/')
                try:
                    sk = ''.join(re.findall('/\d+/', link)).replace('/', '')
                    if sk == '':
                        continue
                except:
                    pass
                query_status = generate_upsert_query_posts_crawl('answers')
                doc = {
                    'sk':sk,
                    'post_url':link,
                    'crawl_status':0,
                    'reference_url':response.url
                    }
                self.cursor.execute(query_status, doc)
                self.conn.commit()
            nav = ''.join(response.xpath('//div[@class="qa-page-links"]//ul//li//a[@class="qa-page-next"]/@href').extract()).replace('../', 'http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion/')
            if nav:
                yield Request(nav, callback=self.parse_next)


    def parse_meta(self, response):
        tag_main_links = response.xpath('//div[@class="qa-q-list-item"]//div[@class="qa-q-item-title"]//a/@href').extract()
        for link in tag_main_links:
            link = link.replace('../../', 'http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion/')
            try:
                sk = ''.join(re.findall('/\d+/', link)).replace('/', '')
            except:
                pass
            query_status = generate_upsert_query_posts_crawl('answers')
            doc = {
                'sk':sk,
                'post_url':link,
                'crawl_status':0,
                'reference_url':response.url
                }
            self.cursor.execute(query_status, doc)
            self.conn.commit()
        nav = ''.join(response.xpath('//div[@class="qa-page-links"]//ul//li//a[@class="qa-page-next"]/@href').extract()).replace('../../', 'http://answerszuvs3gg2l64e6hmnryudl5zgrmwm3vh65hzszdghblddvfiqd.onion/')
        if nav:
            yield Request(nav, callback=self.parse_meta)
