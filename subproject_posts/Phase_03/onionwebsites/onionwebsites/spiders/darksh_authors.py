import deathbycaptcha
from onionwebsites.utils import *
domain = 'http://darksh4d35kjp7fl.onion/'


class Dark(Spider):
    name = "dark_authors"
    start_urls = ["http://darksh4d35kjp7fl.onion"]

    def __init__(self):
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
        import pdb;pdb.set_trace()
        url_que = "select distinct(links) from dark_authors_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_data)

    def parse_data(self, response):
        domain = 'darksh4d35kjp7fl.onion'
        reference_url = response.request.url
        user_name = ''.join(response.xpath('//div[@class="ym-grid"]//div[@class="ym-gl"]/h2/text()').extract())
        username = re.sub("'s profile(.*)", '', user_name).strip()
        vendor = ''.join(response.xpath('//table[@class="ym-table"]//td[contains(text(),"Vendor since:")]//following-sibling::td/text()').extract())
        ven_date = datetime.datetime.strptime(vendor, '%b %d. %Y')
        vendor_since = time.mktime(ven_date.timetuple())*1000
        last = ''.join(response.xpath('//table[@class="ym-table"]//td[contains(text(),"Last seen:")]//following-sibling::td/text()').extract())
        last_date = datetime.datetime.strptime(last, '%b %d. %Y')
        last_seen = time.mktime(last_date.timetuple())*1000
        numb_of_sales = ''.join(response.xpath('//table[@class="ym-table"]//td[contains(text(),"Number of sales:")]//following-sibling::td/text()').extract())
        avg_rating = ''.join(response.xpath('//table[@class="ym-table"]//td[contains(text(),"Average rating:")]//following-sibling::td/text()').extract())
        vendor_statistics = {
                'vendor_since':vendor_since,
                'last_seen':last_seen,
                'number_of_sales':numb_of_sales,
                'average_rating':avg_rating
                }
        featured_products = []
        nodes = response.xpath('//div[@class="ym-g50 ym-gr"]//div[@class="ym-cbox"]')
        for node in nodes:
            table = node.xpath('.//tr')
            for i in table:
                product_name = ''.join(i.xpath('.//td/a/text()').extract())
                value = ''.join(i.xpath('.//td/text()').extract())
                featured_products.append({product_name:value})
        description = clean_text(''.join(response.xpath('//div[@class="ym-fbox"]//h3[contains(text(),"Description")]//following-sibling::text()').extract())).strip()
        pgp_key = clean_text(''.join(response.xpath('//div[@class="ym-fbox"]//h3[contains(text(),"PGP Key")]//following-sibling::text()').extract())).strip()
        json_posts = {
                'domain':domain,
                'username':username,
                'vendor_statistics':vendor_statistics,
                'fetch_time':int(datetime.datetime.now().strftime("%s")) * 1000,
                'featured_products':featured_products,
                'description':description,
                'pgp_key':pgp_key,
                'reference_url':reference_url
                }
        import pdb;pdb.set_trace()
        sk = md5_val(json_posts['username'])
        doc_to_es(id=sk,body=json_posts,doc_type='author')
