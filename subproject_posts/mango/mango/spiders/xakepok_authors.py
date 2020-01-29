from mango.utils import *

class xakepok(scrapy.Spider):
    name = 'xakepok_authors'
    start_urls = ['https://forum.xakepok.net']

    def __init__(self):
         self.conn = MySQLdb.connect(db='posts',host='localhost', user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode='True',charset='utf8mb4')
         self.cursor = self.conn.cursor()
         dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()
    
    def parse(self,response):
        select_que = 'select DISTINCT(links) from xakepok_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select auth_meta from xakepok_authors_crawl where links = "%s"' %url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_time',''))
            meta = {'publish_time':set(activetime)}
            if url:
                yield Request(url, callback=self.parse_author,meta=meta)

    def parse_author(self, response):
        json_posts = {}
        domain = "forum.xakepok.net"
        user_name = ''.join(response.xpath('//div[@class="floatcontainer"]//h1//text()').extract()).replace('\n','')
        join_date = ''.join(response.xpath('//ul[contains(.,"%s")]//following-sibling::li/text()' %u"\u0420\u0435\u0433\u0438\u0441\u0442\u0440\u0430\u0446\u0438\u044f:").extract()[1])
        join = datetime.datetime.strptime(join_date,' %d.%m.%Y')
        join1 = joindate = time.mktime(join.timetuple())*1000
        first = ''.join(response.xpath('//ul[contains(.,"%s")]//following-sibling::li/text()' %u"\u0420\u0435\u0433\u0438\u0441\u0442\u0440\u0430\u0446\u0438\u044f:").extract()[0])
        last = ''.join(response.xpath('//ul[contains(.,"%s")]/li/span[contains(@class,"time")]/text()' %u"\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u044f\u044f \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c:").extract())
        last_activity = first + last
        auth_sig =  ''.join(response.xpath('//dd[@id="signature"]//text() | //dd[@id="signature"]//a[contains(@target,"_blank")]//@href').extract()).replace('\t','').replace('\n',' ').replace('\r','')
        if u'[email\xa0protected]' in auth_sig:
            mails = response.xpath('//a[@class="__cf_email__"]/@data-cfemail').extract_first()
            sign = decode_cloudflareEmail(mails)
            auth_sign  = auth_sig.replace(u'[email\xa0protected]', sign)
        else:
            auth_sign = auth_sig
        if u'\u0412\u0447\u0435\u0440\u0430' in last_activity:
            last = last_activity.replace(u' \u0412\u0447\u0435\u0440\u0430 ','').strip()
            lastactive = datetime.datetime.now() - timedelta(weeks=int(last))
            last_active1 = time.mktime(lastactive.timetuple())*1000
        else:
            last1 = datetime.datetime.strptime(last_activity,' %d.%m.%Y %H:%M')
            last_active1 = time.mktime(last1.timetuple())*1000
 
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000  
        total_posts = ''.join(response.xpath('//ul[contains(.,"%s")]/li//text()' %u"\u0412\u0441\u0435\u0433\u043e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439:").extract()[1])
        active_times = response.meta.get('publish_time')
        activetimes = []
        activetimes = activetime_str(active_times,total_posts)
        json_posts.update({'username': user_name,
                     'author_url':response.url,
                     'domain': 'forum.xakepok.net',
                     'auth_sign': auth_sign,
                     'join_date': join1,
                     'lastactive': last_active1,
                     'totalposts': total_posts,
                     'fetch_time': fetch_time,
                     'groups': '',
                     'reputation': '',
                     'credits': '',
                     'awards': '',
                     'rank': '',
                     'activetimes': activetimes,
                     'contact_info':'',
        })
        sk = md5_val(json_posts['username'])
        doc_to_es(id=sk,body=json_posts,doc_type='author')
