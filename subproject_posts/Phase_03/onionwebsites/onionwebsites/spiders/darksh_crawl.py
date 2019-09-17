import deathbycaptcha
from onionwebsites.utils import *
domain = 'http://darksh4d35kjp7fl.onion'
query = generate_upsert_query_posts_crawl('dark')


class Dark(Spider):
    name = "darksh_crawl"
    start_urls = ["http://darksh4d35kjp7fl.onion"]

    def __init__(self, *args, **kwargs):
        super(Dark,  self).__init__(*args, **kwargs)
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()


    def close_conn(self, spider):
        self.conn.commit()
        sel.conn.close()



    def parse(self, response):
        image = ''.join(response.xpath('//img[@class="captcha"]/@src').extract()) 
        i_captcha = urljoin(domain, image)
        captcha_0 = ''.join(response.xpath('//input[@name="captcha_0"]/@value').extract())
        csrfmiddlewaretoken = ''.join(response.xpath('//input[@name="csrfmiddlewaretoken"]/@value').extract())
        cook = response.headers.get('Set-Cookie', '').split(';')[0]
        cookie = {cook.split('=')[0]:cook.split('=')[1]}
        if captcha_0 and csrfmiddlewaretoken:
            yield Request(i_captcha, callback=self.parse_captcha, meta={'captcha_0':captcha_0, 'csrfmiddlewaretoken':csrfmiddlewaretoken, 'cookie':cookie})
 


    def parse_captcha(self, response):
        with open('dark1.jpeg', 'w') as fp:fp.write(response.body)
        client = deathbycaptcha.SocketClient("Innominds", "Helloworld1234")
        captcha = client.decode('dark1.jpeg', type=2)
        cookie = response.meta.get('cookie', '')
        cap_0 = response.meta.get('captcha_0', '')
        middletoken = response.meta.get('csrfmiddlewaretoken', '')
        headers = {
            'Host': 'darksh4d35kjp7fl.onion',
            'User-Agent': response.request.headers['User-Agent'],
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Encoding':'gzip, deflate',
            'Accept-Language': 'en-US,en;q=0.5',
            'Referer': 'http://darksh4d35kjp7fl.onion/accounts/login/',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Content-Length':'196',
        }
        val = captcha.get('text','')
        data = {
            'captcha_1':val.upper(),
            'captcha_0':cap_0,
            'csrfmiddlewaretoken':middletoken,
            'login':'login',
            'password':'tls@2019',
            'username':'saai',
            }
        print val.upper()

        yield FormRequest('http://darksh4d35kjp7fl.onion/accounts/login/', callback=self.parse_next, cookies=cookie, headers=headers, formdata=data)

    
    def parse_next(self, response):
        cat_urls = response.xpath('//nav[@class="ym-vlist"]//li[contains(@class,"active")]//strong//following-sibling::ul//li//a/@href').extract()
        for url in cat_urls:
            url = urljoin(domain, url)
            yield Request(url, callback=self.parse_next)

        main_urls = ['/products/category/49/','/products/category/50/','/products/category/51/','/products/category/47/']
        if 'category/48' in response.url:
            for main_url in main_urls:
                main_url = urljoin(domain, main_url)
                yield Request(main_url, callback=self.parse_next)
        
        main_links = response.xpath('//div[@class="ym-g80 ym-gr"]//div[@class="ym-gbox product-item-name"]//a/@href').extract()
        for link in main_links:
            link = urljoin(domain, link)
            sk = link.split('/')[-2]
            doc = {
                    'sk':sk,
                    'post_url':link,
                    'crawl_status':0,
                    'reference_url':response.url
                    }
            self.cursor.execute(query, doc)
        page_nav = ''.join(response.xpath('//span[@class="step-links"]//span[@class="current"]//following-sibling::a/@href').extract())
        if '?page=' in response.request.url:
            ref_url = re.sub('page=(.*$)', '', response.request.url).replace('?', '')
            page_next = ref_url + page_nav
            yield Request(page_next, callback=self.parse_next)
        else:
            nav = response.request.url + page_nav
            yield Request(nav, callback=self.parse_next)
