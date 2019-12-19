from mango.utils import *

class sinister(Spider):
    name = "sinister_posts"
    start_urls = ["https://sinister.ly"]

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        self.start_urls = self.cursor.execute("select post_url from sinister_post_crawl")
        post_url = self.cursor.fetchall()
        for url in post_url:
            yield Request(url[0], callback = self.parse_next)

    def parse_next(self,response):
        domain = "sinister.ly"
        category = response.xpath('//div[@class="breadcrumb"]//li[not(contains(@class,"first_crumb"))]//a/span/text()').extract()[1] or 'Null'
        sub_category =response.xpath('//div[@class="breadcrumb"]//li[not(contains(@class,"first_crumb"))]//a/span/text()').extract()[2] or 'Null'
        sub_category_url = "https://sinister.ly/"+ response.xpath('//div[@class="breadcrumb"]//li[not(contains(@class,"first_crumb"))]//a/@href').extract()[2] or 'Null'
        thread_title =''.join(response.xpath('//td[@class="thead"]//strong//text()').extract()) or 'Null'
        nodes = response.xpath('//div[contains(@class,"post classic tborder tfixed ")]')
        inner_nav = response.xpath('//a[@class="pagination_page"]//@href').extract_first()
        if inner_nav:
            inner_nav_ = urljoin("https://sinister.ly/",inner_nav)
            yield Request(inner_nav_,callback=self.parse_next)
        for node in nodes:
            author = ''.join(node.xpath('.//div[@class="author_information"]//span//a//text()').extract()) or 'Null'
            author_url = ''.join(node.xpath('.//div[@class="author_information"]//span//a//@href').extract()) or 'Null'
            post_title = ''.join(node.xpath('.//span[@class="post_title postbit_title"]//text()').extract()).replace('\n','').replace('\t','').replace('\r','') or 'Null'
            post_url = ''.join(node.xpath('.//a[@class="thead posturl"]//@href').extract())
            if post_url:
                post_url = urljoin("https://sinister.ly/",post_url)
            else:
                post_url = 'Null'
            ord_in_thread = ''.join(node.xpath('.//a[@class="thead posturl"]//text()').extract()).replace('\n','').replace('\t','').replace('\r','').replace('#','') or 'Null'
            post_id = ''.join(re.findall('#pid(.*)',post_url)) or 'Null'
            date = ''.join(node.xpath('.//span[@class="post_date postbit_date"]//text() | .//span[@class="post_date postbit_date"]//@title').extract())
            date_ = ''.join(set(re.findall('\d+-\d+-\d+',date)))
            publish_epoch = time_to_epoch(date_,'%m-%d-%Y')
            if publish_epoch:
                month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            else:
                publish_epoch = 'Null'
            text = ''.join(node.xpath('.//div[@class="post_body scaleimages"]//text() | .//img[contains(@class,"smilie smilie_")]//@alt |.//div[@class="post_body scaleimages"]/blockquote/cite/text() | .//div[@class="post_body scaleimages"]/blockquote[@class="mycode_quote"]//text() |.//div[@class="post_body scaleimages"]/blockquote/@class | .//div[@class="post_body scaleimages"]//img[@class="mycode_img"]//@alt').extract()).replace('\r','').replace('\n','  ').replace('\t','').replace('mycode_quote', 'Quote ') or 'Null'
            links = ', '.join(node.xpath(' .//div[@class="post_body scaleimages"]//img[@class="mycode_img"]//@src | .//div[@class="post_body scaleimages"]/a[@target="_blank"]//@href').extract())
            if links == '':
                links = 'Null'
            reference_url = response.url
            if '?pid' in reference_url:
                test = re.findall('(.*)?pid=',reference_url)
                thread_url_ = ''.join(test)
            elif '?' in reference_url:
                test = re.findall('(.*)?page',reference_url)
                thread_url_ = ''.join(test)
            else:
                thread_url_ = reference_url
            author_data = {
                    'name':author,
                    'url':author_url
                    }
            json_posts = {
                        'record_id' : re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
                        'hostname': 'sinister.ly',
                        'domain':"sinister.ly",
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
                            'language':'english',
                            'require_login':'false',
                            'sub_section':sub_category,
                            'sub_section_url': sub_category_url,
                            'post_id': post_id,
                            'post_title':post_title,
                            'ord_in_thread': ord_in_thread,
                            'post_url': post_url,
                            'post_text':text,
                            'thread_title':thread_title,
                            'thread_url': thread_url_,
                    },
            }
            sk = md5_val(post_url)
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts)

