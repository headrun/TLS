from mango.utils import *

class Centerclub(scrapy.Spider):
    name = "centerclub_authors"


    def __init__(self):
         self.conn = MySQLdb.connect(host="localhost", user="root", passwd="qwerty123", db="posts", charset="utf8", use_unicode=True)
         self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = 'select DISTINCT(links) from centerclub_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select auth_meta from centerclub_authors_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))
            meta = {'publish_epoch':set(activetime)}
            if url:
                yield Request(url, callback=self.parse_author,meta = meta)

    def parse_author(self, response):
        user_name = ''.join(response.xpath('//h1[@class="memberHeader-name"]//text()').extract())
        joindate = ''.join(response.xpath('//div[@class="memberHeader-blurb"]//dl//dd//time//@data-time').extract()[0])
        totalposts = response.xpath('//a[@class="fauxBlockLink-linkRow u-concealed"]//text()').extract()[0]
        groups = ','.join(response.xpath('//div[@class="memberHeader-banners"]//em//strong//text() | //div//strong//text()').extract())
        last_active = ''.join(response.xpath('//div[@class="memberHeader-blurb"]//dl//dd//time//@data-time').extract()[-1]).strip()
        activetimes_ =  response.meta.get('publish_epoch')
        activetimes = []
        credits = response.xpath('//a[@class="fauxBlockLink-linkRow u-concealed"]//text()').extract()[1]
        activetimes = activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        url_ = ''.join(response.xpath('//a[@id="about"]//@href').extract())
        url = "https://center-club.ws" + url_
        meta = {'user_name':user_name,'joindate':joindate,'totalposts':totalposts,'groups':groups,'last_active':last_active,'activetimes':activetimes,'credits':credits,'fetch_time':fetch_time}
        yield Request(url,callback = self.get_meta,meta =meta)

    def get_meta(self,response):
        json_posts = {}
        domain = 'center-club.ws'
        user_name = response.meta.get('user_name')
        joindate = response.meta.get('joindate')
        totalposts = response.meta.get('totalposts')
        groups = response.meta.get('groups')
        last_active = response.meta.get('last_active')
        activetimes = response.meta.get('activetimes')
        credits = response.meta.get('credits')
        fetch_time = response.meta.get('fetch_time')
        auth_sign = ''.join(response.xpath('//div[@class="block-row block-row--separated"]//div[@class="bbWrapper"]//text() | //div[@class="block-row block-row--separated"]//div[@class="bbWrapper"]//a//img//@src').extract()).strip()
        if u'[email\xa0protected]' in auth_sign:
            mails = response.xpath('//a[@class="__cf_email__"]/@data-cfemail').extract_first()
            email = decode_cloudflareEmail(mails)
            auth_sign = auth_sign.replace(u'[email\xa0protected]', email)
        website = ''.join(response.xpath('//dl//dd//a//text()').extract()).strip()
        contact_info = []
        if website:
           contact_info.append({'user_id':website,'channel':'WEBSITE'})
        json_posts = {'username': user_name,
                     'domain': domain,
                     'auth_sign':auth_sign,
                     'join_date': joindate,
                     'lastactive': last_active,
                     'totalposts': totalposts,
                     'fetch_time': fetch_time,
                     'groups': groups,
                     'reputation': '',
                     'credits': credits,
                     'awards': '',
                     'rank': '',
                     'reference_url':response.url,
                     'activetimes': activetimes,
                     'contact_info':str(contact_info),
        }
        sk = hashlib.md5(domain + json_posts['username']).hexdigest()
        es.index(index="amazon",doc_type='agartha_posts',id= sk, body=json_posts)
