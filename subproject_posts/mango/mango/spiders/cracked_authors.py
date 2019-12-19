from mango.utils import *


class Cracked(Spider):
    name = "cracked_authors"
    start_urls = ['https://cracked.to/']


    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', host='localhost', user=DATABASE_USER, passwd=DATABASE_PASS, charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        scraper = cfscrape.create_scraper()
        res = scraper.get('https://cracked.to/')
        yield Request('https://cracked.to/', callback=self.parse_author_links)



    def parse_author_links(self, response):
        query = 'select distinct(links) from cracked_authors_crawl where crawl_status = 0'
        self.cursor.execute(query)
        data = self.cursor.fetchall()
        for urls in data:
            url = urls[0]
            meta_query = 'select distinct(auth_meta) from cracked_authors_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for link in meta_query:
		meta = json.loads(link[0])
                activetime.append(meta.get('publish_time',''))

            publish_time = set(activetime)
            if url:
                yield Request(url, callback=self.parse_author, meta={'publish_time':publish_time})


#AUTHORS_DATA

    def parse_author(self, response):
        domain = 'cracked.to'
        publish_time = response.meta.get('publish_time', '')
        username = ''.join(response.xpath('//div[@class="profile-cov"]//span[@class="x-largetext"]//text()').extract()).replace('\n', '').strip()
        if u'[email\xa0protected]' in username:
            mails = response.xpath('//a[@class="__cf_email__"]/@data-cfemail').extract_first()
            email = decode_cloudflareEmail(mails)
            username = username.replace(u'[email\xa0protected]', email)
        join_date = ''.join(response.xpath('//td[contains(text(),"Registration Date:")]//following-sibling::span/text()').extract())
        joindate = time_to_epoch(join_date, '%d %B, %Y')
        last_visit = ''.join(response.xpath('//td[contains(text(),"Last Visit:")]//following-sibling::span/text()').extract())
        try:
            if 'minute' in last_visit:
                date_time = ''.join(re.findall('\d+', last_visit))
                last = datetime.datetime.now() - timedelta(minutes=int(date_time))
                lastactive = time.mktime(last.timetuple())*1000
            if 'second' in last_visit:
                date_time = ''.join(re.findall('\d+', last_visit))
                last = datetime.datetime.now() - timedelta(seconds=int(date_time))
                lastactive = time.mktime(last.timetuple())*1000
            if 'hour' in last_visit:
		date_time = ''.join(re.findall('\d+', last_visit))
                last = datetime.datetime.now() - timedelta(hours=int(date_time))
                lastactive = time.mktime(last.timetuple())*1000
            if 'day' in last_visit:
                date_time = ''.join(re.findall('\d+', last_visit))
                last = datetime.datetime.now() - timedelta(days=int(date_time))
                lastactive = time.mktime(last.timetuple())*1000
            if 'week' in last_visit:
                date_time = ''.join(re.findall('\d+', last_visit))
                last = datetime.datetime.now() - timedelta(weeks=int(date_time))
                lastactive = time.mktime(last.timetuple())*1000
            if 'month' in last_visit:
                date_time = ''.join(re.findall('\d+', last_visit))
                now_ = datetime.datetime.now()
                last = now_ + dateutil.relativedelta.relativedelta(months=-int(date_time))
                lastactive = time.mktime(last.timetuple())*1000
            if 'year' in last_visit:
                date_time = ''.join(re.findall('\d+', last_visit))
                now_ = datetime.datetime.now()
                last = now_ + dateutil.relativedelta.relativedelta(years=-int(date_time))
                lastactive = time.mktime(last.timetuple())*1000
        except:
            pass

        totalposts = ''.join(response.xpath('//tr[@class="d-flex flex-wrap text-center"]//td[@class="trow1 flex-one"]/a[contains(@href, "finduser&uid")]/text()').extract())
	credits = ''.join(response.xpath('//tr[@class="d-flex flex-wrap text-center"]//td[@class="trow1 flex-one"]/a[contains(@href, "/newpoints.php")]/text()').extract()).encode('ascii', 'ignore').strip()
        activetimes = activetime_str(publish_time, totalposts)
        fetchtime = fetch_time()
        reputation = ''.join(response.xpath('//div[@class="flex-one profile-stats"]/div[@class="profile-top"]/span[@class="x-largetext"]//following-sibling::strong//text()').extract())
        groups = ', '.join(response.xpath('//div[@class="grpimg"]/img/@title').extract())
        awards = ', '.join(response.xpath('//td[@class="thead"]/strong[contains(text(),"Awards")]//..//..//..//following-sibling::td[@class="trow1"]/a/@title').extract())
        auth_sig = ' '.join(response.xpath('//td[@class="trow1 scaleimages"]//text() | //td[@class="trow1 scaleimages"]//a/@href').extract()).replace('\n', '').strip().encode('ascii', 'ignore')
        author_sign = ' '.join(response.xpath('//td[@class="trow1 scaleimages"]//img[@class="mycode_img"]/@src').extract())
        if author_sign:
            auth_signature = author_sign + auth_sig
        else:
            auth_signature = auth_sig
        contact_info = []
        json_author = {
                'username':username,
                'domain':domain,
                'auth_sign':auth_signature,
                'join_date':joindate,
                'lastactive':lastactive,
                'totalposts':totalposts,
                'fetch_time':fetchtime,
                'groups':groups,
                'reputation':reputation,
                'credits':credits,
                'awards':awards,
                'rank':'',
                'activetimes':activetimes,
                'contact_info':contact_info,
                }
        sk = md5_val(json_author['username'])
        doc_to_es(id=sk, body=json_author, doc_type='author')
