from onionwebsites.utils import *


class Darkmoney_posts(Spider):
    name = "darkmoney_posts"

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self,spider):
        self.cursor.close()
        self.conn.close()

    def start_requests(self):
        self.cursor.execute('select distinct(post_url) from darkmoney_crawl')
        data = self.cursor.fetchall()
	for da in data:
            yield Request(da[0],callback = self.parse_next)

    def parse_next(self, response):
        nodes = response.xpath('//div[@id="posts"]//table[contains(@id,"post")]') #response.xpath('//div[@id="posts"]//div[@align="center"]')
        domain = 'y3pggjcimtcglaon.onion'
        try:
            category = response.xpath('//tr[@valign="bottom"]//span[@class="navbar"]//a/text()').extract()[1]
            sub_category = response.xpath('//tr[@valign="bottom"]//span[@class="navbar"]//a/text()').extract()[2]
            post_title = response.xpath('//tr[@valign="bottom"]//span[@class="navbar"]//a/text()').extract()[-1]
        except:
            post_title = 'Null'
            category = 'Null'
            sub_category = 'Null'
	sub_categoryurl = response.xpath('//tr[@valign="bottom"]//span[@class="navbar"]//a/@href').extract()[-1]
	if sub_categoryurl:
	    sub_category_url = 'https://y3pggjcimtcglaon.onion'+ sub_categoryurl
	if sub_categoryurl == '':
	    sub_category_url = 'Null'
        for node in nodes:
	    ord_in_thread = ''.join(node.xpath('.//a[contains(@id,"postcount")]/@name').extract())
            author = ''.join(node.xpath('.//div[contains(@id,"postmenu_")]//a[@class="bigusername"]//text()').extract()) or 'Null'
            post_id = node.xpath('./@id').extract_first() 
            post_id = post_id.replace('post','')
	    if post_id == '':
		post_id = 'Null'
            post_url = urljoin('https://y3pggjcimtcglaon.onion/',node.xpath('.//a[contains(@id,"postcount")]/@href').extract_first())
	    if post_url == '':
		post_url = 'Null'
            publish_date = ''.join(node.xpath('.//td[@style="font-weight:normal; border: 1px solid #000099; border-right: 0px"]//text()').extract()).strip()
            publish_epoch = time_to_epoch(publish_date,'%d.%m.%Y, %H:%M')
	    if publish_epoch:
		month_year = get_index(publish_epoch)
	    else:
		import pdb;pdb.set_trace()

            text = ''.join(node.xpath('.//div[contains(@id,"post_message_")]//text() | .//div[@class="vbseo_buttons"]//text() | .//div[contains(@id,"post_message_")]//td[@class="alt2"]/@class').extract()).strip().replace('alt2','Quote\n ').replace('\n', '')
            author_url = urljoin('https://y3pggjcimtcglaon.onion/', ''.join(node.xpath('.//div[contains(@id,"postmenu_")]//a[@class="bigusername"]/@href').extract()))
	    if author_url == '':
		author_url = 'Null'
            thread_url = re.sub('index\d+.html','',response.url)
            links = set(node.xpath('.//div[contains(@id,"post_message_")]//a[@rel="nofollow"]/@href | .//div[@class="vbseo_buttons"]//a[contains(@href,"members")]/@href').extract())
            all_links = []
            for link in links:
		if link and 'http' not in link:
                    link = urljoin('https://y3pggjcimtcglaon.onion/',link)
                    all_links.append(link)
                else:
                    all_links.append(link)
	    links_url = ', '.join(all_links)
	    if links_url == '':
		links_url = 'Null'
	    if links == []:
		links_url = 'Null'
            thread_title = post_title
	    author_data = {
			'name':author,
			'url':author_url
			}
	    post = {
		'cache_link':'',
		'author':json.dumps(author_data),
		'section':category,
		'language':'russian',	
		'require_login':'false',
		'sub_section':sub_category,
		'sub_section_url':sub_category_url,
		'post_id':post_id,
		'post_title':post_title,
		'ord_in_thread':int(ord_in_thread),
		'post_url':post_url,
		'post_text':clean_text(text),
		'thread_title':thread_title,
		'thread_url':thread_url
		}
	    json_posts = {
		'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
		'hostname':'y3pggjcimtcglaon.onion',
		'domain':domain,
		'sub_type':'darkweb',
		'type':'forum',
		'author':json.dumps(author_data),
		'title':thread_title,
		'text':clean_text(text),
		'url':post_url,
		'original_url':post_url,
		'fetch_time':fetch_time(),
		'publish_time':publish_epoch,
		'link_url':links_url,
		'post':post
		}
	    sk = md5_val(post_url)
	    #query={"query":{"match":{"_id":sk}}}
            #res = es.search(body=query)
            #if res['hits']['hits'] == []:
	    es.index(index=month_year, doc_type='post', id=sk, body=json_posts,request_timeout=30)
