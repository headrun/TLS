from mango.utils import *

class Turkhackteam(scrapy.Spider):
    name = "turkhackteam_posts"
    start_urls = ["https://www.turkhackteam.org/"] 

    def __init__(self):
        self.conn =MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode=True, charset='utf8')
        self.crawl_query = generate_upsert_query_authors_crawl('turkhackteam')
        self.cursor= self.conn.cursor()
        
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self,response):
        url_que = "select distinct(post_url) from turkhackteam_post_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_ann)


    def parse_ann(self, response):
        crawl_type = response.meta.get('crawl_type')
        sel = Selector(response)
        try:
            category = response.xpath('//div[@class="pagenavit"]//div[@class="container"]//a//text()').extract()[1]
        except:
            category = "Null"

        try:
            sub_category = ' , '.join(response.xpath('//div[@class="pagenavit"]/div[@class="container"]//a/text()').extract()[2::]) or 'Null'
        except:
            sub_category = 'Null'
        
        sub_categoryurl = response.xpath('//div[@class="pagenavit"]//div[@class="container"]//a//@href').extract()
        try:
            if len(sub_categoryurl) == 4:
                subcategoryurl_ = sub_categoryurl[-1]
            if len(sub_categoryurl) == 5:
                subcategoryurl_ = sub_categoryurl[-2] +', '+ sub_categoryurl[-1] 
        except:
            subcategoryurl_ = 'Null'
        thread_title = ''.join(response.xpath('//div[@class="container"]//h1[@class="spe-posth1"]//text() ').extract()).strip() or 'Null'
        nodes = sel.xpath('//div[@class="container"]//div[contains(@id,"edit")]')
        page = sel.xpath('//div[@class="pagenav"]//a[@rel="next"]//@href').extract_first()
        if page:
            page = "https://www.turkhackteam.org/" + page
            page_nav = ''.join(re.findall('(.*)" title',page))
            yield Request(page_nav, callback = self.parse_ann)
        if page:
           pno = ''.join(re.findall('(.*)&page',page))
           if crawl_type == 'keep_up':
               page = response.url + pno
           else:
               page = re.sub('(.*)&page',pno,response.url) 
        count = 0
        for node in nodes:
            count +=1
            author_url = ''.join(node.xpath('.//div[contains(@id,"postmenu_")]//a[@class="npl-bigusername"]//@href').extract())
            if author_url == '':
                author_url = 'Null'
            Posttitle = ''.join(node.xpath('.//div[@class="pb-two npl-thread-title d-flex df-center"]//div[@class="pbt-col"]//text()').extract()).strip() or 'Null'
            Post_url = ''.join( node.xpath('.//div[@class="pnt-col pnt-fix"]//a[@class="spe-button10"]//@href').extract())
            if Post_url == '':
                post_url = 'Null'
            ord_in_thread = count
            post_id = ''.join(node.xpath('.//div[@class="postbit-message"]/@id').extract()).replace('post_message_','').strip() or 'Null'
            publish= ''.join(node.xpath('.//div[@class="pnt-col"]//text() | .//div[@class="pnt-row d-flex mob-d-flex"]//text()').extract()).strip()
            try:
                if 'den' in publish:
                    publish_date = ''.join(re.findall('(.*)den',publish))
                    publishdate = datetime.datetime.strptime(publish_date,'%d-%m-%Y %H:%M ')
                    publish_epoch =time.mktime(publishdate.timetuple())*1000
                elif u'bir' in publish:
                    publish = 1
                    publishdate = datetime.datetime.now() - timedelta(weeks=int(publish))
                    publish_epoch = time.mktime(publishdate.timetuple())*1000
                elif u'Bir' in publish:
                    publish = 3
                    publishdate = datetime.datetime.now() - timedelta(hours=int(publishdate))
                    publish_epoch = time.mktime(publish.timetuple())*1000
                elif 'Saat' in publish:
                    publish_date = ''.join(re.findall('\d+', publish))
                    publishdate = datetime.datetime.now() - timedelta(hours=int(publish_date))
                    publish_epoch = time.mktime(publishdate.timetuple())*1000
                elif 'Hafta' in publish:
                    publish_date = ''.join(re.findall('\d+', last_active))
                    publishdate = datetime.datetime.now() - timedelta(weeks=int(publish_date))
                    publish_epoch = time.mktime(publishdate.timetuple())*1000
                elif u'g\xfcn' in publish:
                    publish_date = ''.join(re.findall('\d+', publish))
                    publishdate = datetime.datetime.now() - timedelta(days=int(publish_date))
                    publish_epoch = time.mktime(publishdate.timetuple())*1000
                elif u'G\xfcn' in publish:
                    publish_date = ''.join(re.findall('\d+', publish))
                    publishdate = datetime.datetime.now() - timedelta(weeks=int(publish_date))
                    publish_epoch = time.mktime(publishdate.timetuple())*1000
                else:
                    publishdate = datetime.datetime.strptime(publish,'%d-%m-%Y %H:%M')
                    publish_epoch =time.mktime(publishdate.timetuple())*1000
                if publish_epoch:
                    year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                    if year > '2011':
                        month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                    else:
                        continue
            except:
                publish_epoch = 0
            FetchTime = int(datetime.datetime.now().strftime("%s")) * 1000
            Author =  ''.join(node.xpath('.//div[contains(@id,"postmenu_")]//a[@class="npl-bigusername"]//text()').extract()) or 'Null'
            text = ' '.join(node.xpath('.//div[contains(@id,"post_message_")]//b//text() | .//div[contains(@id,"post_message_")]//text() | .//div[contains(@id,"post_message_")]//blockquote//text() | .//div[contains(@id,"post_message_")]//font//text() | .//div[contains(@id,"post_message_")]//td[@class="alt2 quote-type"]//div//text() | .//div[@style="margin:20px; margin-top:5px; "]//text() | .//div[contains(@id,"post_message_")]//ul//li//text() | .//div[@class="npl-editnote"]//text()').extract()).replace('Al\xc4\xb1nt\xc4\xb1:','Al\xc4\xb1nt\xc4\xb1:Quote') or 'Null'
            Text = re.sub('\s\s+', ' ', text)
            thread_url = response.url
            domain = 'turkhackteam.org'
            Links = node.xpath('.//div[contains(@id,"post_message_")]//blockquote//@href | .//div[contains(@id,"post_message_")]//img[contains(text(),".jpg")]//@src | .//div[contains(@id,"post_message_")]//td[@class="alt2 quote-type"]//a[@rel="nofollow"]//@href | .//div[contains(@id,"post_message_")]//a[@target="_blank"]//@href | .//div[contains(@id,"post_message_")]//a//img//@src ').extract()

            links = str(Links)
            all_links = ', '.join(Links)
            if all_links == '':
                all_links = 'Null'
            if Links == []:
                all_links = 'Null'

            reference_url = response.url
            author_data = {
                        'name':Author,
                        'url':author_url
                        }
            post_data = {
                        'cache_link':'',
                        'author': json.dumps(author_data),
                        'section':category,
                        'language':'turkish',
                        'require_login':'false',
                        'sub_section':sub_category,
                        'sub_section_url':subcategoryurl_,
                        'post_id':post_id,
                        'post_title':Posttitle,
                        'ord_in_thread':ord_in_thread,
                        'post_url':Post_url,
                        'post_text':clean_text(Text).replace('\n', ''),
                        'thread_title':thread_title,
                        'thread_url':thread_url
                        }
            json_posts = {
                          'record_id':re.sub(r"\/$", "", Post_url.replace(r"https", "http").replace(r"www.", "")),
                          'hostname':'www.turkhackteam.org',
                          'domain': domain,
                          'sub_type':'openweb',
                          'type':'forum',
                          'author':json.dumps(author_data),
                          'title':thread_title,
                          'text':clean_text(Text).replace('\n', ''),
                          'url':Post_url,
                          'original_url':Post_url,
                          'fetch_time':FetchTime,
                          'publish_time':publish_epoch,
                          'link.url':all_links,
                          'post':post_data
                        }
            sk = md5_val(Post_url)
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts)
	    if author_url == 'Null':
		continue
            meta = {'publish_epoch': publish_epoch}
            json_crawl = {}
            json_crawl = {
                    'post_id': post_id,
                    'auth_meta': json.dumps(meta),
                    'crawl_status':0,
                    'links': author_url
            }
            self.cursor.execute(self.crawl_query, json_crawl)
            self.conn.commit()
