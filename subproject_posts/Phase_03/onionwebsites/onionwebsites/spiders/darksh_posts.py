import deathbycaptcha
from onionwebsites.utils import *
domain = 'http://darksh4d35kjp7fl.onion'


class Dark(Spider):
    name = "darksh_posts"
    start_urls = ["http://darksh4d35kjp7fl.onion/accounts/login/"]
    custom_settings = {
        'DOWNLOAD_DELAY':0.25
        }

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()



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
        #print val.upper()

        yield FormRequest('http://darksh4d35kjp7fl.onion/accounts/login/', callback=self.parse_next, cookies=cookie, headers=headers, formdata=data)

    def parse_next(self,response):
        url_que = "select distinct(post_url) from dark_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_data)



    def parse_data(self, response):
        crawl_status = 0
        category_ = clean_text(''.join(response.xpath('//div[@class="ym-col3"]//div[@class="ym-form"]/div[@class="ym-fbox"]//text()').extract())).split('>')
        category = category_[1].strip()
        sub_category = category_[2::]
        auth = ''.join(response.xpath('//div[@class="fm-fbox"]//p[contains(text(),"sold by ")]//following-sibling::a/text()').extract())
        author = re.sub(' (.*)(\d+)', '', auth).replace(')', '')
        auth_url = ''.join(response.xpath('//div[@class="fm-fbox"]//p[contains(text(),"sold by ")]//following-sibling::a/@href').extract())
        if auth_url:
            author_url = urljoin(domain, auth_url)
        post_url = response.request.url
        post_id = post_url.split('/')[-2]
        title = ''.join(response.xpath('//div[@class="fm-fbox"]//h3/text()').extract())
        product_info = {}
        ships_from = ''.join(response.xpath('//div[@class="ym-g66 ym-gl product-infos"]//div[@class="ym-cbox"]//td[contains(text(),"Ships From:")]//following-sibling::td/text()').extract())
        if ships_from:
            product_info.update({'ships_from':ships_from})
        ships_to = ''.join(response.xpath('//div[@class="ym-g66 ym-gl product-infos"]//div[@class="ym-cbox"]//td[contains(text(),"Ships to:")]//following-sibling::td/text()').extract())
        if ships_to:
            product_info.update({'ships_to':ships_to})
        numb_of_sales = ''.join(response.xpath('//div[@class="ym-g66 ym-gl product-infos"]//div[@class="ym-cbox"]//td[contains(text(),"Number of sales:")]//following-sibling::td/text()').extract())
        if numb_of_sales:
            product_info.update({'number_of_sales':numb_of_sales})
        avg_rating = ''.join(response.xpath('//div[@class="ym-g66 ym-gl product-infos"]//div[@class="ym-cbox"]//td[contains(text(),"Average rating:")]//following-sibling::td/text()').extract())
        if avg_rating:
            product_info.update({'average_rating':avg_rating})
        text = ''.join(response.xpath('//div[@class="ym-gbox"]//h4[contains(text(),"Product description")]//following-sibling::p//text()').extract()).strip().encode('ascii', 'ignore')
        pgp_key = ''.join(response.xpath('//div[@class="ym-form"]//h4[contains(text(),"PGP Key")]//following-sibling::p//text()').extract()).strip()
        query='insert into dark_authors_crawl(post_id, links, crawl_status) values(%s,%s,%s)'
        import pdb;pdb.set_trace()
        values=(post_id, author_url, crawl_status)
        self.cursor.execute(query,values)
        self.conn.commit
        json_posts = {
                'domain':'darksh4d35kjp7fl.onion',
                'category':category,
                'sub_category':sub_category,
                'author':author,
                'author_url':author_url,
                'post_url':post_url,
                'post_id':post_id,
                'fetch_time':int(datetime.datetime.now().strftime("%s")) * 1000,
                'title':title,
                'product_info':product_info,
                'text':text,
                'pgp_key':pgp_key
                }
        sk = md5_val(post_url)
	query={"query":{"match":{"_id":sk}}}
        res = es.search(body=query)
        if res['hits']['hits'] == []:
	    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
        #doc_to_es(id=sk,body=json_posts,doc_type='post')
	else:
	    data_doc = res['hits']['hits'][0]
	    if json_posts['text'] != data_doc['_source']['text']:
		es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)
