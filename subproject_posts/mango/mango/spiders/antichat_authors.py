import cfscrape
from mango.utils import *

class Antichat(scrapy.Spider):
    name = "antichat_authors"

    def __init__(self):
         self.conn = MySQLdb.connect(db='posts',host='localhost', user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode='True',charset='utf8mb4')
         self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        scraper = cfscrape.create_scraper()
        r = scraper.get('https://forum.antichat.ru/')
        #print r.content
        yield Request('https://forum.antichat.ru/',callback= self.parse_nxt)

    def parse_nxt(self,response):
        select_que = 'select DISTINCT(links) from antichat_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select auth_meta from antichat_authors_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetimes = []
            reputation = ''
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetimes.append(meta.get('publish_time',''))
                reputation = meta.get('reputation','')
            meta = {'publish_time':set(activetimes), 'reputation':reputation}
            if url:
                yield Request(url, callback=self.parse_author,meta = meta)

    def parse_author(self, response):
        json_posts = {}
        domain = 'forum.antichat.ru'
        username = ''.join(response.xpath('//h1[@itemprop="name"]//span//text() | //h1[@class="username"]//text()').extract())
        author_text = ' '.join(response.xpath('//div[@class="baseHtml signature ugc"]//text() | //div[@class="baseHtml signature ugc"]//a//@href').extract())
        auth_sign = ''.join(response.xpath('//div[@class="baseHtml signature ugc"]//img[@class="bbCodeImage"]//@src').extract())
        if "proxy.php?image" in auth_sign:
            auth_sign = urljoin("https://forum.antichat.ru/", auth_sign) + author_text
        else:
            auth_sign = author_text
        join_date = ''.join(response.xpath('//div[@class="secondaryContent pairsJustified"]//dl//dt[contains(text(),"Joined:")]/following-sibling::dd/text()').extract())
        
        join = datetime.datetime.strptime(join_date,'%d %b %Y')
        joindate = time.mktime(join.timetuple())*1000
        totalposts = ''.join(response.xpath('//div[@class="secondaryContent pairsJustified"]//dl//dt[contains(text(),"Messages:")]/following-sibling::dd/text()').extract()).strip()
        groups = ''.join(response.xpath('//p[@class="userBlurb"]//span[@class="userTitle"]//text()').extract())
        lastactive = ''.join(response.xpath('//div[@class="secondaryContent pairsJustified"]//dd//span[@class="DateTime"]//text() | //div[@class="secondaryContent pairsJustified"]//dd//abbr//@data-datestring').extract())
        try:
            last = datetime.datetime.strptime(lastactive,'%d %b %Y')
            lastactive = time.mktime(last.timetuple())*1000
        except:
            last = datetime.datetime.strptime(lastactive,'%d %b %Y')
            lastactive = time.mktime(lastactive.timetuple())*1000
        rank = ''
        activetimes_ =  response.meta.get('publish_time')
        reputation = response.meta.get('reputation')
        activetimes = []
        activetimes = activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        icq = ''.join(response.xpath('//dl//dt[contains(text(),"ICQ:")]/following-sibling::dd/text()').extract()).strip()
        contact_info = []
        if icq:
           contact_info.append({'user_id':icq,'channel':'ICQ'})
        json_posts = {'username': username,
                     'domain': domain,
                     'auth_sign':auth_sign,
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
                     'contact_info':str(contact_info),
                }
        sk = md5_val(json_posts['username'])
        doc_to_es(id=sk,body=json_posts,doc_type='author')
