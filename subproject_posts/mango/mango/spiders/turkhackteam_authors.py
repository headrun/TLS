from mango.utils import *

class Turkhackteam(scrapy.Spider):
    name = "turkhackteam_authors"


    def __init__(self):
         self.conn = MySQLdb.connect(db='posts',host='localhost', user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode='True',charset='utf8mb4')
         self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = 'select DISTINCT(links) from turkhackteam_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select auth_meta from turkhackteam_authors_crawl where links = "%s"'%url.encode('utf8')
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
        json_posts = {}
        domain = 'turkhackteam.org'
        user_name = ''.join(response.xpath('//h1[@class="member-h1"]//span//text()').extract())
        author_signature = ''.join(response.xpath('//dl[@class="list_no_decoration profilefield_list"]//dd[@id="signature"]//div//text() | //dl[@class="list_no_decoration profilefield_list"]//dd[@id="signature"]//text()').extract())
        join_date = ''.join(response.xpath('//div[@class="min-st-col"]//text()').extract()[-3])
        join = datetime.datetime.strptime(join_date,'%d-%m-%Y %H:%M')
        joindate = time.mktime(join.timetuple())*1000
        totalposts = ''.join(response.xpath('//div[@class="min-st-col"]//text()').extract()[-1])
        groups = ''.join(response.xpath('//h2[@class="member-h2"]//text()').extract())
        last_active = ''.join(response.xpath('//div[@class="last-ac-col"]//text()').extract()[-1]).strip()
        if u'bir' in last_active:
            last_active = 1
            last = datetime.datetime.now() - timedelta(weeks=int(last_active))
            lastactive = time.mktime(last.timetuple())*1000
        elif u'Bir' in last_active:
            last_active = 3
            last = datetime.datetime.now() - timedelta(hours=int(last_active))
            lastactive = time.mktime(last.timetuple())*1000
        elif 'Saat' in last_active:
            date = ''.join(re.findall('\d+', last_active))
            last = datetime.datetime.now() - timedelta(hours=int(date))
            lastactive = time.mktime(last.timetuple())*1000
        elif 'Hafta' in last_active:
            date = ''.join(re.findall('\d+', last_active))
            last = datetime.datetime.now() - timedelta(weeks=int(date))
            lastactive = time.mktime(last.timetuple())*1000
        elif u'g\xfcn' in last_active:
            date = ''.join(re.findall('\d+', last_active))
            last = datetime.datetime.now() - timedelta(days=int(date))
            lastactive = time.mktime(last.timetuple())*1000
        elif u'G\xfcn' in last_active:
            date = ''.join(re.findall('\d+', last_active))
            last = datetime.datetime.now() - timedelta(weeks=int(date))
            lastactive = time.mktime(last.timetuple())*1000
        else:
            last = datetime.datetime.strptime(last_active,'%d-%m-%Y %H:%M')
            lastactive = time.mktime(last.timetuple())*1000
        activetimes_ =  response.meta.get('publish_epoch')
        activetimes = []
        activetimes = activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        contact_info = []
        json_posts.update({'username': user_name,
                          'domain': domain,
                          'auth_sign':author_signature,
                          'join_date': joindate,
                          'lastactive': lastactive,
                          'totalposts': totalposts,
                          'fetch_time': fetch_time,
                          'groups': groups,
                          'reputation': '',
                          'credits': '',
                          'awards': '',
                          'rank': '',
                          'author_url':response.url,
                          'activetimes': activetimes,
                          'contact_info': str(contact_info),
        })
        sk = md5_val(json_posts['username'])
        doc_to_es(id=sk,body=json_posts,doc_type='author')
