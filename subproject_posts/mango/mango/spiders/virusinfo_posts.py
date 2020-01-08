from mango.utils import *

class Virusinfo(Spider):
    name = "virusinfo_posts"
    start_urls = ["https://virusinfo.info/forumdisplay.php?f=92"]

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts",host="localhost",user=DATABASE_USER, passwd=DATABASE_PASS, use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def parse(self, response):
        url_que = "select distinct(post_url) from virusinfo_crawl where crawl_status = 0 "
        self.cursor.execute(url_que)
        data = self.cursor.fetchall()
        for url in data:
            yield Request(url[0], callback = self.parse_next)

    def parse_next(self, response):
        domain = "virusinfo.info"
        if "t=" in response.url:
            category = response.xpath('//li[@class="navbit"]//a//text()').extract()[2]
        else :
            category = ''.join(response.xpath('//li[@class="navbit lastnavbit"]//span//text()').extract()) or 'Null'
        if "?t=" in response.url:
            sub_category = ''.join(response.xpath('//li[@class="navbit"]//a//text()').extract()[-1]).encode('utf8') 
        else :
            sub_category = '' or 'Null'
        thread_title = ''.join(response.xpath('//h1//span[@class="threadtitle"]//a//text() | //div[@id="pagetitle"]//h1//text()').extract()).strip() or 'Null'
	sub_category_url = ''  or 'Null'
        nodes = response.xpath('//div[@id="postlist"]//li[contains(@id,"post_")]')
        for node in nodes:
            if  "nodecontrols" not in node.extract():
                continue
            else:
                author = ''.join(node.xpath('.//a[@class="username offline popupctrl"]//strong//font//b//text() | .//a[@class="username offline popupctrl"]//strong//text() | .//a[@class="username offline popupctrl"]//strong//font//text()').extract()).strip() or 'Null'
                author_url = ''.join(node.xpath('.//a[@class="username offline popupctrl"]//@href').extract()) 
                if author_url :
                    author_url = urljoin("https://virusinfo.info/", author_url)
		else:
		    author_url = 'Null'

                post_title = ''.join(node.xpath('.//h2[@class="title icon"]//img//@alt | .//h2[@class="title icon"]/text()').extract()).strip() or 'Null'
                post_url = ''.join(node.xpath('.//span[@class="nodecontrols"]//a[contains(@name,"post")]//@href').extract()) 
                if post_url:
                    post_url = urljoin("https://virusinfo.info/", post_url)
		else:
		    post_url = 'Null'
		record_id = re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", ""))
	        ord_in_thread = ''.join(node.xpath('.//span[@class="nodecontrols"]//a//text()').extract()).replace('#','') or 'Null'
                post_id = ''.join(re.findall('#post(.*)',post_url)) or 'Null'
                date = ''.join(node.xpath('.//span[@class="date"]/text()').extract()) or 'Null'
                date_ = ''.join(set(re.findall('(.*),',date)))
                publish_epoch = time_to_epoch(date_,'%d.%m.%Y')
                if publish_epoch ==None:
                    import pdb;pdb.set_trace()
		if publish_epoch:
                    year = time.strftime("%Y", time.localtime(int(publish_epoch/1000)))
                    if year > '2011':
                        month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
                    else:
                        continue
                else:
                    import pdb;pdb.set_trace()

                text = ' '.join(node.xpath('.//blockquote[@class="postcontent restore "]//text() | .//blockquote[@class="postcontent lastedited"]//text() | .//div[@class="bbcode_postedby"]//img//@alt | .//div[@class="information-box"]//text()').extract()).replace(u'\u0426\u0438\u0442\u0430\u0442\u0430',u'\u0426\u0438\u0442\u0430\u0442\u0430 %s'%'Quote').replace('\n','').replace('\r', '').replace('\t', '').strip() or 'Null'
                Links = node.xpath('.//div[@class="bbcode_postedby"]//a[@rel="nofollow"]//@href | .//blockquote[@class="information-box"]//a[@rel="nofollow"]//@href | .//font//a//@href | .//div[@style="text-align: center;"]//a//img//@src | .//blockquote[@class="postcontent restore "]//a//@href | .//div[@style="text-align: center;"]//a//@href').extract()
                Link = []
                for link_ in Links:
                    if 'http' not in link_ :
                        link = response.urljoin(link_)
                        Link.append(link)
                    else:
                        Link.append(link_)

                all_links = ', '.join(Link)
		if all_links =='':
		    all_links ='Null'
		if Links == []:
		    all_links = 'Null'

                reference_url = response.url
                if '&p=' in reference_url:
                    test = re.findall('(.*)&page',reference_url)
                    thread_url = ''.join(test)
                else:
                    thread_url = reference_url
		author_data =  {
                            'name':author,
                            'url':author_url
                            }

                json_posts = {
		    'record_id' : record_id,
                    'hostname': 'virusinfo.info',
                    'domain':"virusinfo.info",
                    'sub_type':'openweb',
                    'type' : 'forum',
                    'author': json.dumps(author_data),
                    'title':clean_text(thread_title),
                    'text': clean_text(text),
                    'url': post_url,
                    'original_url':post_url,
                    'fetch_time': fetch_time(),
                    'publish_time': publish_epoch,
                    'link.url': all_links,
                    'post':{
                        'cache_link':'',
                        'author':json.dumps(author_data),
                        'section':category,
                        'language':'russian',
                        'require_login':'false',
                        'sub_section':sub_category,
                        'sub_section_url':sub_category_url,
                        'post_id': post_id,
                        'post_title':post_title,
                        'ord_in_thread': ord_in_thread,
                        'post_url': post_url,
                        'post_text':text,
                        'thread_title':thread_title,
                        'thread_url': thread_url
                    },

                }
                sk = md5_val(post_id)
		#query={"query":{"match":{"_id":sk}}}
            	#res = es.search(body=query)
            	#if res['hits']['hits'] == []:
                es.index(index="forum_posts_"+month_year, doc_type='post', id=sk, body=json_posts,request_timeout=30)
                #doc_to_es(id=sk,doc_type='post',body=json_posts)
		'''else:
                    data_doc = res['hits']['hits'][0]
                    if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
                        es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts)'''

        inner_nav = response.xpath('//div[contains(@id,"pagination_")]//span/a[@rel="next"]/@href').extract_first()
        if inner_nav:
            inner_nav = urljoin("https://virusinfo.info/",inner_nav)
            yield Request(inner_nav, callback=self.parse_next)
