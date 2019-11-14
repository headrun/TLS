from mango.utils import *

class Defendtheweb(Spider):
    name = "defendtheweb_authors"


    def __init__(self):
        self.conn = MySQLdb.connect(db="posts", user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        url_query = 'select distinct(links) from defendtheweb_authors_crawl where crawl_status = 0'
        self.cursor.execute(url_query)
        data = self.cursor.fetchall()
        for urls in data:
            url = urls[0]
            meta_query = 'select distinct(auth_meta) from defendtheweb_authors_crawl where links="%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime = []
            for link in meta_query:
                meta = json.loads(link[0])
                activetime.append(meta.get('publish_time', ''))
            publish_time = set(activetime)
            if url:
                yield Request(url, callback=self.parse_author, meta={'publish_time':publish_time})


#AUTHORS_DATA

    def parse_author(self, response):
        domain = 'defendtheweb.net'
        publish_time = response.meta.get('publish_time', '')
        username = ''.join(response.xpath('//div[@class="block-heading"]//a/h2/text()').extract())
        nodes = response.xpath('//div[@class="eight columns"]//..//..//div[@class="block"]//div[@class="block-content"]//ul//li[@class="hide"] | //div[@class="eight columns"]//..//..//div[@class="block"]//div[@class="block-content"]//ul//li')
        for node in nodes:
            join = clean_text(''.join(node.xpath('.//div[@class="right"]/span/@title | .//div[@class="right"]//..//following-sibling::text()').extract()).replace('\n', '').replace('(UTC)', '')).strip()
            try:
                if 'Joined the site' in join:
                    join_date = join.replace('Joined the site', '').strip()
                    joindate = time_to_epoch(join_date, '%d/%m/%Y %H:%M')
            except:
                pass

        lastactive = 0
        rank = ''.join(response.xpath('//div[@class="profile-stat"]//div[contains(text(),"Rank")]//following-sibling::h3/text()').extract())
        reputation = ''.join(response.xpath('//div[@class="profile-stat"]//div[contains(text(),"Reputation")]//following-sibling::h3/text()').extract())
        totalposts = ''.join(response.xpath('//div[@class="profile-stat"]//div[contains(text(),"Discussion posts")]//following-sibling::h3/text()').extract())
        activetimes = activetime_str(publish_time, totalposts)
        fetchtime = fetch_time()
        group = ''.join(response.xpath('//div[@class="block-heading"]//div[@class="tags"]//div[@class="tag"]//i//..//following-sibling::text()').extract()).replace('\n', '').strip()
        groups = re.sub('\s\s+', ', ', group)
        awards = []
        medals = response.xpath('//div[@class="block-heading"]//h3[contains(text(),"Medals")]//..//following-sibling::div[@class="block-content"]//ul[@class="list"]//li//div[@class="right"]')
        for medal in medals:
            name = ''.join(medal.xpath('.//..//following-sibling::h4/text()').extract())
            colour = ', '.join(medal.xpath('./i/@class').extract())
	    awards.append({'name':name, 'colour':colour})
        contact_info = []
        json_author = {
            'username':username,
            'domain':domain,
            'auth_sign':'',
            'join_date':joindate,
            'lastactive':lastactive,
            'totalposts':totalposts,
            'fetch_time':fetchtime,
            'groups':groups,
            'reputation':reputation,
            'credits':'',
            'awards':json.dumps(awards),
            'rank':rank,
            'activetimes':activetimes,
            'contact_info':contact_info,
            'reference_url':response.request.url
            }
        sk = md5_val(json_author['username'])
        doc_to_es(id=sk, body=json_author, doc_type='author')
