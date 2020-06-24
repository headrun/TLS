from mango.utils import *

class Centerclub(scrapy.Spider):
    name = "centerclub_posts"

    def __init__(self):
        self.conn = MySQLdb.connect(db='posts', user=DATABASE_USER, passwd=DATABASE_PASS, host='localhost', use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
        self.crawl_query = generate_upsert_query_authors_crawl('centerclub')
        self.cursor= self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        url_que = "select distinct(post_url) from centerclub_post_crawl where crawl_status = 0"
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0] , callback = self.parse)


    def parse(self , response):
        crawl_type = response.meta.get('crawl_type')
        sel = Selector(response)
        try:
            category = response.xpath('//span[@itemprop="name"]//text()').extract()[1]
        except:
            category = "Null"

        try:
            sub_category = ''.join(response.xpath('//span[@itemprop="name"]//text()').extract()[-2]) or 'Null'
        except:
            sub_category = 'Null'

        sub_categoryurl = response.xpath('//a[@itemprop="item"]//@href').extract()[-2]
        if sub_categoryurl:
            sub_categoryurl =  urljoin("https://center-club.ws", sub_categoryurl)
        thread_title = ''.join(response.xpath('//div[@class="p-title "]//h1[@class="p-title-value"]//text()').extract()).strip() or 'Null'
        reference_url = response.url
        count = 0 
        nodes = sel.xpath('//article[contains(@id,"js-post-")]')
        thread_url = response.url
        if '/page' in reference_url:
            test = ''.join(re.findall('(.*)/page',reference_url)).replace('/page','')
            tes = ''.join(test)
        else:
            tes = reference_url
        for node in nodes:
            author_url = ''.join(node.xpath('.//a[@class="username "]//@href').extract())
            if author_url:
                author_url =  urljoin("https://center-club.ws", author_url)
            Posttitle = 'Null'
            Post_url = ''.join(node.xpath('.//ul[@class="message-attribution-opposite message-attribution-opposite--list"]//li/a[contains(text(),"#")]/@href').extract())
            if Post_url:
                Post_url = urljoin("https://center-club.ws", Post_url)
            post_id = ''.join(re.findall('post-\d+',Post_url)).replace('post-','') or 'Null'
            publish = ''.join(node.xpath('.//div[@class="message-attribution-main"]//a//time//@datetime').extract()).strip()
            publish_date = ''.join(re.findall('(.*)T',publish)).replace('T','')
            publishdate = datetime.datetime.strptime(publish_date ,'%Y-%m-%d')
            publish_epoch = time.mktime(publishdate.timetuple())*1000
            if publish_epoch:
                year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                if year > '2011':
                    month_year =  time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                else:
                    continue
            count = count+1
            FetchTime = int(datetime.datetime.now().strftime("%s")) * 1000
            Author = ''.join(node.xpath('.//h4[@class="message-name"]//a[@class="username "]//text()').extract()) or 'Null'
            text = ' '.join(node.xpath('.//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//text() | .//div[@class="attachment-icon attachment-icon--img"]//img//@alt | .//article[@class="message-body js-selectToQuote"]//section[@class="message-attachments"]//h4[@class="block-textHeader"]//text() | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//blockquote//div[@class="bbCodeBlock-title"]//a//text() | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//blockquote//div[@class="bbCodeBlock-content"]//div[@class="bbCodeBlock-expandContent"]//text() | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//blockquote//div[@class="bbCodeBlock-title"]//a[@data-xf-click="attribution"]//@class | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//img//@alt | .//div[@class="blockMessage blockMessage--error blockMessage--iconic"]//text()').extract()).replace('bbCodeBlock-sourceJump','Quote:') or 'Null'
            Text = re.sub('\s\s+', '', text)
            domain = 'center-club.ws'
            Links = node.xpath('.//div[@class="bbWrapper"]//a[@rel="nofollow noopener"]//@href | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//img[@class="bbImage"]//@src | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//a[@class="link link--external"]//@href').extract()

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
                        'language':'russian',
                        'require_login':'false',
                        'sub_section':sub_category,
                        'sub_section_url':sub_categoryurl,
                        'post_id':post_id,
                        'post_title':Posttitle,
                        'ord_in_thread':count,
                        'post_url':Post_url,
                        'post_text':clean_text(Text).replace('\n', ''),
                        'thread_title':thread_title,
                        'thread_url':tes
                        }
            json_posts = {
                          'record_id':re.sub(r"\/$", "", Post_url.replace(r"https", "http").replace(r"www.", "")),
                          'hostname':'center-club.ws',
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
            sk = md5_val(post_id)
            es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts)

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
        page = sel.xpath('//a[@class="pageNav-jump pageNav-jump--next"]//@href').extract_first()
        if page:
            page = urljoin("https://center-club.ws", page)
            yield Request(page, callback = self.parse)
