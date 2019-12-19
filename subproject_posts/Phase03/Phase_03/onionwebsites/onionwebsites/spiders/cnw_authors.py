from onionwebsites.utils import *

class CNW(scrapy.Spider):
    name = "cnw_authors"
    


    def __init__(self):
         self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
         self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def start_requests(self):
        select_que = 'select DISTINCT(links) from cnw_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select auth_meta from cnw_authors_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_time',''))
            meta = {'publish_time':set(activetime)}
            if url:
                yield Request(url, callback=self.parse_author,meta = meta)

    def parse_author(self, response):
        json_posts = {}
        domain = 'cnwv3ycmy4uc7vou.onion'
        username = ''.join(response.xpath('//h1[@class="ipsType_reset ipsPageHead_barText"]//text()').extract())
        author_signature = ''
        join_date = ''.join(response.xpath('//ul[@class="ipsList_inline ipsPos_left"]//li/time/@title').extract())
        join = datetime.datetime.strptime(join_date,'%m/%d/%y %I:%M %p')
        joindate = time.mktime(join.timetuple())*1000
        totalposts = ''.join(response.xpath('//li//h4[@class="ipsType_minorHeading"]/following-sibling::text()').extract()).strip()
        groups = ''.join(response.xpath('//span[@class="ipsPageHead_barText"]//span//text()').extract())
        reputation = ''.join(response.xpath('//span[@class="cProfileRepScore_points"]//text()').extract())
        last_active = ''.join(response.xpath('//li/span/time/@title').extract())
        try:
            last = datetime.datetime.strptime(last_active,'%m/%d/%y %I:%M %p')
            lastactive = time.mktime(last.timetuple())*1000
        except:
            pass
        rank = ''.join(response.xpath('//div[@class="ipsDataItem_generic ipsType_break"]//text()').extract())
        activetimes_ =  response.meta.get('publish_time')
        activetimes = []
        activetimes = activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        contact_info = ''
        json_posts.update({'username': clean_text(username),
                     'domain': domain,
                     'auth_sign':author_signature,
                     'join_date': joindate,
                     'lastactive': lastactive,
                     'totalposts': totalposts,
                     'fetch_time': fetch_time,
                      'groups': groups,
                     'reputation': reputation,
                     'credits': '',
                     'awards': '',
                     'rank': rank,
                     'activetimes': activetimes,
                     'contact_info':contact_info,
        })
        sk = md5_val(username)
        doc_to_es(id=sk,body=json_posts,doc_type='author')
