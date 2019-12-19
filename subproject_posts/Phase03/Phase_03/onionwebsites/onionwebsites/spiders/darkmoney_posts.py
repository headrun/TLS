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
            #da = ["https://y3pggjcimtcglaon.onion/novosti-iz-smi-47/smi-kompanii-stolknulis-s-novoi-shemoi-moshennichestva-207253/"]
            yield Request(da[0],callback = self.parse_next)

    def parse_next(self, response):
        nodes = response.xpath('//div[@id="posts"]//table[contains(@id,"post")]') #response.xpath('//div[@id="posts"]//div[@align="center"]')
        domain = 'y3pggjcimtcglaon.onion'
        try:
            category = response.xpath('//tr[@valign="bottom"]//span[@class="navbar"]//a/text()').extract()[1]
            sub_category = response.xpath('//tr[@valign="bottom"]//span[@class="navbar"]//a/text()').extract()[2::]
            post_title = response.xpath('//tr[@valign="bottom"]//span[@class="navbar"]//a/text()').extract()[-1]
        except:
            post_title = ''
            category = ''
            sub_category = ''

        for node in nodes:
            json_posts = {}
            author = ''.join(node.xpath('.//div[contains(@id,"postmenu_")]//a[@class="bigusername"]//text()').extract())
            post_id = node.xpath('./@id').extract_first() or ''
            post_id = post_id.replace('post','')
            post_url = urljoin('https://y3pggjcimtcglaon.onion/',node.xpath('.//a[contains(@id,"postcount")]/@href').extract_first())
            publish_date = ''.join(node.xpath('.//td[@style="font-weight:normal; border: 1px solid #000099; border-right: 0px"]//text()').extract()).strip()
            publish_date = time_to_epoch(publish_date,'%d.%m.%Y, %H:%M')
            text = ''.join(node.xpath('.//div[contains(@id,"post_message_")]//text() | .//div[@class="vbseo_buttons"]//text() | .//div[contains(@id,"post_message_")]//td[@class="alt2"]/@class').extract()).strip().replace('alt2','Quote\n ')
            author_url = urljoin('https://y3pggjcimtcglaon.onion/', ''.join(node.xpath('.//div[contains(@id,"postmenu_")]//a[@class="bigusername"]/@href').extract()))
            thread_url = re.sub('index\d+.html','',response.url)
            links = set(node.xpath('.//div[contains(@id,"post_message_")]//a[@rel="nofollow"]/@href | .//div[@class="vbseo_buttons"]//a[contains(@href,"members")]/@href').extract())
            all_links = []
            for link in links:
		if link and 'http' not in link:
                    link = urljoin('https://y3pggjcimtcglaon.onion/',link)
                    all_links.append(link)
                else:
                    all_links.append(link)
            thread_title = post_title
            json_posts.update({
                    "domain": domain,
                    "category": category,
                    "sub_category": sub_category,
                    "thread_title": thread_title,
                    "thread_url": thread_url,
                    "post_title":post_title,
                    "post_id": post_id,
                    "post_url": post_url,
                    "publish_time": publish_date,
                    "fetch_time": fetch_time(),
                    "author": author,
                    "text": clean_text(text),
                    "links": ', '.join(all_links),
                    "author_url": author_url,
            })
	    sk = md5_val(post_url)
	    query={"query":{"match":{"_id":sk}}}
            res = es.search(body=query)
            if res['hits']['hits'] == []:
		es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts,request_timeout=30)
            #doc_to_es(id=sk,body=json_posts,doc_type='post')
	    else:
		data_doc = res['hits']['hits'][0]
                if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
		    es.index(index="forum_posts", doc_type='post', id=sk, body=json_posts,request_timeout=30)
