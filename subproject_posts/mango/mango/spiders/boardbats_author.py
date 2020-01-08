from mango.utils import *

class Boardbats(scrapy.Spider):
    name = "boardbat_authors"
    start_urls = ["https://board.b-at-s.info/"]


    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", host="localhost", user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode=True, charset='utf8mb4')
        self.cursor = self.conn.cursor()
	dispatcher.connect(self.close_conn, signals.spider_closed) 

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        select_que = 'select DISTINCT(links) from boardbat_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for link in data:
            url = link[0]
            meta_query = 'select auth_meta from boardbat_authors_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_epoch',''))
            meta = {'publish_epoch':set(activetime)}
            if url:
                yield Request(url, callback=self.parse_author, meta=meta, dont_filter=True)

#AUTHORS_DATA

    def parse_author(self, response):
        json_posts = {}
        domain = 'board.b-at-s.info'
	author_link = response.request.url
        user_name = ''.join(response.xpath('//h1[@class="ipsType_reset ipsPageHead_barText"]//text()').extract()).replace('\n\t','').replace('\t','').replace('\n', '').strip()
        author_signature = ''
        join_date = ''.join(response.xpath('//ul[@class="ipsList_inline ipsPos_left"]//li/time/@title').extract())
        join = datetime.datetime.strptime(join_date,'%m/%d/%y %I:%M  %p')
        joindate = time.mktime(join.timetuple())*1000
        totalposts = ''.join(response.xpath('//li//h4[@class="ipsType_minorHeading"]/following-sibling::text()').extract()).strip()
        groups = ''.join(response.xpath('//span[@class="ipsPageHead_barText"]//span//text()').extract()).strip()
        reputation = ''.join(response.xpath('//span[@class="cProfileRepScore_points"]//text()').extract()).strip()
        last_active_ = ''.join(response.xpath('//li/span/time/@title').extract())
        try:
            last = datetime.datetime.strptime(last_active_,'%m/%d/%y %I:%M  %p')
            last_active = time.mktime(last.timetuple())*1000
        except:
            pass
	if last_active_ == '':
            last_active = 0

        rank = ''.join(response.xpath('//div[@class="ipsDataItem_generic ipsType_break"]//text()').extract()).replace('\n\t','').replace('\t','')
        activetimes_ =  response.meta.get('publish_epoch')
        activetimes = []
        activetimes = activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        website_url = response.xpath('//strong[contains(text(),"Website URL")]//..//following-sibling::div//text()').extract()
        MSN = response.xpath('//strong[contains(text(),"MSN")]//..//following-sibling::div//text()').extract()
        AIM = response.xpath('//strong[contains(text(),"AIM")]//..//following-sibling::div//text()').extract()
        skype = response.xpath('//strong[contains(text(),"Skype")]//..//following-sibling::div//text()').extract()
        yahoo = response.xpath('//strong[contains(text(),"Yahoo")]//..//following-sibling::div//text()').extract()
	icq = response.xpath('//strong[contains(text(),"ICQ")]//..//following-sibling::div//text()').extract()
        contact_info = []
        if website_url or icq or skype or yahoo or MSN or AIM:
            contact_info.append({'user_id':"website_url:","channel":website_url})
            contact_info.append({'user_id':"icq:",'channel':icq})
            contact_info.append({'user_id':"MSN:",'channel':MSN})
            contact_info.append({'user_id':"AIM:",'channel':AIM})
            contact_info.append({'user_id':"skype:",'channel':skype})
            contact_info.append({'user_id':"yahoo:",'channel':yahoo})

        json_posts.update({'username': user_name,
                     'domain': domain,
                     'author_sign':author_signature,
                     'join_date': joindate,
                     'lastactive': last_active,
                     'total_posts': totalposts,
                     'fetch_time': fetch_time,
                     'groups': groups,
                     'reputation': reputation,
                     'credits': '',
                     'awards': '',
                     'rank': rank,
                     'activetimes': activetimes,
                     'contact_info':str(contact_info),
        })
	sk = md5_val(json_posts['username'])
        doc_to_es(id=sk, body=json_posts, doc_type='author')
