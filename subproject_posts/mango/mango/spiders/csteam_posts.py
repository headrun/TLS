from mango.utils import *

class csteam(scrapy.Spider):
    name = 'csteam_posts'
    start_urls = ['https://cs-team.ir/forums/']

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode=True, charset='utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        url_que = "select distinct(post_url) from csteam_post_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_next)

    def parse_next(self,response):
        category = response.xpath('//nav[@class="ipsBreadcrumb ipsBreadcrumb_1 ipsFaded_withHover"]//ul[contains(@data-role,"breadcrumbList")]//span//text()').extract()[1] or 'Null'
        sub_category = response.xpath('//nav[@class="ipsBreadcrumb ipsBreadcrumb_1 ipsFaded_withHover"]//ul[contains(@data-role,"breadcrumbList")]//span//text()').extract()[2] or 'Null'
        sub_category_url = response.xpath('//nav[@class="ipsBreadcrumb ipsBreadcrumb_1 ipsFaded_withHover"]//ul[contains(@data-role,"breadcrumbList")]/li/a/@href').extract()[2] or 'Null'
        thread_title = ''.join(response.xpath('//nav[@class="ipsBreadcrumb ipsBreadcrumb_1 ipsFaded_withHover"]/ul[contains(@data-role,"breadcrumbList")]/li/text()').extract()).replace('\r','').replace('\n','').replace('\t' ,'') or 'Null'
        inner_nav = response.xpath('//li[contains(@class,"ipsPagination_page")]/a/@href').extract_first()
        if inner_nav:
            yield Request(inner_nav,callback=self.parse_next)
        nodes = response.xpath('//article[contains(@id,"elComment_")]')
        for node in nodes:
            author = ''.join(node.xpath('.//strong[@itemprop="name"]//span[contains(@class,"userM")]/text()').extract()) or 'Null'
            author_url = '' or 'Null'
            post_title = '' or 'Null'
            post_url = node.xpath('.//ul[@class="ipsList_inline ipsComment_tools"]//li//a/@href').extract()[1] or 'Null'
            ord_in_thread =  ''.join(node.xpath('.//ul[@class="ipsList_inline ipsComment_tools"]//li//text()').extract()).replace('\n','').replace('\t','').replace('#','').replace('\r','').replace('ID:','').replace('\xc2\xa0','').encode('utf-8').strip() or 'Null'
            post_id  = ''.join(re.findall('&comment=(.*)',post_url))
            date = ''.join(node.xpath('.//a[@class="ipsType_blendLinks"]//time//@title').extract()) 
            date_ = ''.join(set(re.findall('\d+/\d+/\d+',date)))
            publish_epoch = time_to_epoch(date_,'%d/%m/%y') 
            if publish_epoch:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            else:
                publish_epoch = 'Null'
            text = ''.join(node.xpath('.//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//img//@alt | .//div[@class="ipsQuote_citation"]//text() | .//div[@class="cPost_contentWrap ipsPad"]//blockquote/@class | .//div[@class="ipsQuote_contents ipsClearfix"]//p//text() | .//blockquote[@class="ipsQuote"]//p//text()').extract()).replace('\n','').replace('\t','').replace('ipsQuote', 'Quote ') or 'Null'
            links = '' or 'Null'
            reference_url = response.url
            if '?' in reference_url:
                test = re.findall('(.*)?page=',reference_url)
                thread_url = ''.join(test)
            else:
                thread_url = reference_url

            author_data = {
                    'name':author,
                    'url':author_url
                    }
            json_posts = {
                        'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                        'hostname': 'cs-team.ir',
                        'domain':"cs-team.ir",
                        'sub_type':'openweb',
                        'type' : 'forum',
                        'author': json.dumps(author_data),
                        'title':thread_title,
                        'text': text,
                        'url': post_url,
                        'original_url': post_url,
                        'fetch_time': fetch_time(),
                        'publish_time': publish_epoch,
                        'link.url':links,
                        'post':{
                            'cache_link':'',
                            'author': json.dumps(author_data),
                            'section':category,
                            'language':'persian',
                            'require_login':'false',
                            'sub_section':sub_category,
                            'sub_section_url': sub_category_url,
                            'post_id': post_id,
                            'post_title':post_title,
                            'ord_in_thread': ord_in_thread,
                            'post_url': post_url,
                            'post_text':text,
                            'thread_title':thread_title,
                            'thread_url': thread_url,
                    },
            }
            sk = md5_val(post_url)
            es.index(index="forum_posts_"+month_year,doc_type = "post",id=sk, body = json_posts,request_timeout=1)


