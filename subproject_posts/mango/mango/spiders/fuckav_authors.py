from mango.utils import *

class Fuckav_authors(Spider):
    name = "fuckav_authors"
    start_urls = ['https://fuckav.ru/']
    custom_settings = {
        'COOKIES_ENABLED':True
        }

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts',host='localhost', user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode='True',charset='utf8mb4')
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        cooks = re.sub('";documen(.*)','',re.sub('(.*)cookie="','',''.join(response.xpath('//script//text()').extract()))).split('=')
        self.cook = {cooks[0]: cooks[1]}
        yield scrapy.Request('https://fuckav.ru/', callback=self.meta_data, cookies=self.cook, dont_filter=True)

    def meta_data(self,response):
        select_que = 'select DISTINCT(links) from fuckav_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select auth_meta from fuckav_authors_crawl where links = "%s"'%url.encode('utf8')
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
        domain = "fuckav.ru"
        user_name = ''.join(response.xpath('//div[@id="main_userinfo"]//h1//text()').extract()).strip()
        join_date = clean_text(''.join(response.xpath('//span[contains(.,"%s")]/following-sibling::text()'%u'\u0420\u0435\u0433\u0438\u0441\u0442\u0440\u0430\u0446\u0438\u044f:').extract())).strip()
        joindate = time_to_epoch(join_date,'%d-%m-%Y')
        if joindate == None:
            joindate = time_to_epoch(join_date,'%d/%m/%Y')
        if joindate == None:
            joindate = 0

        total = response.xpath('//span[contains(.,"%s")]/following-sibling::text()'%u'\u0412\u0441\u0435\u0433\u043e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439:').extract_first()
        totalposts = total.strip()
        groups = ''
        signature = ''.join(response.xpath('//dd[@id="signature"]//text()').extract())
        last_active = response.xpath('//span[contains(.,"%s")]/following-sibling::text()'%u'\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u044f\u044f \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c:').extract_first() or ''
        if u'\u043d\u0435\u0434\u0435\u043b\u044e \u043d\u0430\u0437\u0430\u0434' in last_active:
            last = last_active.replace(u'\u043d\u0435\u0434\u0435\u043b\u044e \u043d\u0430\u0437\u0430\u0434', '').strip()
            lastactive = datetime.datetime.now() - timedelta(weeks=int(last))
            last_activity = time.mktime(lastactive.timetuple())*1000
        elif u'\u0447\u0430\u0441 \u043d\u0430\u0437\u0430\u0434' in last_active:
            last = last_active.replace(u'\u0447\u0430\u0441 \u043d\u0430\u0437\u0430\u0434', '').strip()
            lastactive = datetime.datetime.now() - timedelta(hours=int(last))
            last_activity = time.mktime(lastactive.timetuple())*1000
        elif u'\u0447\u0430\u0441(\u043e\u0432) \u043d\u0430\u0437\u0430\u0434' in last_active:
            last = last_active.replace(u'\u0447\u0430\u0441(\u043e\u0432) \u043d\u0430\u0437\u0430\u0434', '')
            lastactive = datetime.datetime.now() - timedelta(hours=int(last))
            last_activity = time.mktime(lastactive.timetuple())*1000
        elif u'\u043c\u0438\u043d\u0443\u0442(\u044b) \u043d\u0430\u0437\u0430\u0434' in last_active:
            last = last_active.replace(u'\u043c\u0438\u043d\u0443\u0442(\u044b) \u043d\u0430\u0437\u0430\u0434', '').strip()
            lastactive = datetime.datetime.now() - timedelta(minutes=int(last))
            last_activity = time.mktime(lastactive.timetuple())*1000
        elif u'\u043d\u0435\u0434\u0435\u043b\u044c(\u0438) \u043d\u0430\u0437\u0430\u0434' in last_active:
            last = last_active.replace(u'\u043d\u0435\u0434\u0435\u043b\u044c(\u0438) \u043d\u0430\u0437\u0430\u0434', '').strip()
            lastactive = datetime.datetime.now() - timedelta(weeks=int(last))
            last_activity = time.mktime(lastactive.timetuple())*1000
        elif u'\u0434\u043d. \u043d\u0430\u0437\u0430\u0434' in last_active:
            last = last_active.replace(u'\u0434\u043d. \u043d\u0430\u0437\u0430\u0434', '').strip()
            lastactive = datetime.datetime.now() - timedelta(days=int(last))
            last_activity = time.mktime(lastactive.timetuple())*1000
        elif u'\u0434\u0435\u043d\u044c \u043d\u0430\u0437\u0430\u0434' in last_active:
            last = last_active.replace(u'\u0434\u0435\u043d\u044c \u043d\u0430\u0437\u0430\u0434', '').strip()
            lastactive = datetime.datetime.now() - timedelta(days=int(last))
            last_activity = time.mktime(lastactive.timetuple())*1000
        elif last_active:
            last_activity = time_to_epoch(last_active.strip(), '%d-%m-%Y')
        elif last_active:
            last_activity = time_to_epoch(last_active.strip(), '%d/%m/%Y')
        elif last_active == '':
            last_active = 0
            last_activity = last_active
        else:
            import pdb;pdb.set_trace()

        activetimes_ = response.meta.get('publish_time')
        activetimes = []
        activetimes = activetime_str(activetimes_,totalposts)
        thanks = clean_text(response.xpath('//fieldset[@class="statistics_group"]//img[contains(@src,".gif")]//following-sibling::text()').extract_first()).replace('=','').replace(',','').strip()
        total_thanks = clean_text(response.xpath('//fieldset[@class="statistics_group"]//img[contains(@src,".gif")]//following-sibling::text()').extract()[-1]).replace('=','').replace(',','').strip()
        credits = [{'All thanks of this user':thanks, 'Total Number of Thanks':total_thanks}]
        contact_info = []
        jabber = ''.join(response.xpath('//dt[contains(text(),"Jabber")]//following-sibling::dd/text()').extract())
        if jabber:
            contact_info.append({'user_id':jabber,'channel':'Jabber'})
        json_posts.update({'username': user_name,
                          'domain': domain,
                          'auth_sign':signature,
                          'join_date': joindate,
                          'lastactive': last_activity,
                          'total_posts': totalposts,
                          'fetch_time': fetch_time(),
                          'groups': '',
                          'reputation': '',
                          'credits': json.dumps(credits),
                          'awards': '',
                          'rank': '',
                          'activetimes': activetimes,
                          'contact_info': str(contact_info),
                          #'reference_url': response.request.url
        })
        sk = md5_val(json_posts['username'])
        doc_to_es(id=sk,body=json_posts,doc_type='author')
