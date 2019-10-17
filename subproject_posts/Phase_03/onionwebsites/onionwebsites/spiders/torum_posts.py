import deathbycaptcha
from onionwebsites.utils import *

crawl_query = generate_upsert_query_authors_crawl('torum')

class Torum(Spider):
    name = "torum"
    start_urls = ['http://torum6uvof666pzw.onion']

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
        self.cursor = self.conn.cursor()
	dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self,response):
        key = response.headers.get('Set-Cookie','').replace('; path=/','')
        cook = response.headers.get('Set-Cookie','').replace('; path=/','').replace('PHPSESSID=','')
        cookies = {'PHPSESSID': cook}
        headers = {
    'Host': 'torum6uvof666pzw.onion',
    'User-Agent': response.request.headers['User-Agent'],
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'http://torum6uvof666pzw.onion/captcha/',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
}
        if 'Login' in response.body:
            sid = ''.join(set(response.xpath('//form//input[@name="sid"]/@value').extract()))
            cook_val = 'tor_u=1; tor_k=; tor_sid=%s'%(sid)
            headers_ ={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Encoding':'gzip, deflate',
                'Accept-Language':'en-US,en;q=0.5',
                'Cache-Control':'no-cache',
                'Connection':'keep-alive',
                'Content-Length':'152',
                'Content-Type':'application/x-www-form-urlencoded',
                'Cookie': cook_val,
                'Host':'torum6uvof666pzw.onion',
                'Pragma':'no-cache',
                'Referer':'http://torum6uvof666pzw.onion/',
                'Upgrade-Insecure-Requests':'1',
                'User-Agent':response.request.headers['User-Agent'],
            }
            data = {
                'login':'Login',
                'password':'tls2019',
                'redirect': './index.php?sid='+sid,
                'sid':sid,
                'username':'saikrishnatls'
                }
            yield FormRequest('http://torum6uvof666pzw.onion/ucp.php?mode=login&sid=%s'%(sid),callback = self.home_page, headers = headers_, formdata= data)
        else:
            yield Request('http://torum6uvof666pzw.onion/captcha/image.php',callback = self.captcha,headers= headers,cookies = cookies,meta = {'cook':cookies},dont_filter = True)

    def captcha(self,response):
        with open('torum1.jpeg', 'w') as fp:fp.write(response.body)
        client = deathbycaptcha.SocketClient("Innominds", "Helloworld1234")
        captcha = client.decode('torum1.jpeg', type=2)
        headers = {
    'Host': 'torum6uvof666pzw.onion',
    'User-Agent': response.request.headers['User-Agent'],
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Encoding':'gzip, deflate',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': 'http://torum6uvof666pzw.onion/captcha/',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Content-Length':'26'
}
        try:
            val = captcha.get('text','')
            if type(eval(val)) is int and len(val) == 6:
                pass
            else:
                raise Exception('captcha not solved')
        except:
            client = deathbycaptcha.SocketClient("Innominds", "Helloworld1234")
	    try:
            	client.report(captcha["captcha"])
	    except:
		pass
            yield Request('http://torum6uvof666pzw.onion',callback = self.parse,dont_filter = True)
            return
        data = {
                'input':  str(val),
                'submit':'Verify'
                }
        cookies = response.meta.get('cook')
        pprint(cookies)
        pprint(headers)
        pprint(data)
        yield FormRequest('http://torum6uvof666pzw.onion/captcha',callback = self.meta_data, headers= headers,cookies = cookies,formdata= data)

    def meta_data(self,response):
        redirect = ''.join(response.xpath('//input[@name="redirect"]/@value').extract())
        try:
            sid = response.headers .get('Set-Cookie','').split(';')[0].split('=')[1]
        except:
            sid = ''
        headers ={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Encoding':'gzip, deflate',
                'Accept-Language':'en-US,en;q=0.5',
                'Cache-Control':'no-cache',
                'Connection':'keep-alive',
                'Content-Length':'152',
                'Content-Type':'application/x-www-form-urlencoded',
                'Cookie':response.request.headers.get('Cookie'),
                'Host':'torum6uvof666pzw.onion',
                'Pragma':'no-cache',
                'Referer':'http://torum6uvof666pzw.onion/',
                'Upgrade-Insecure-Requests':'1',
                'User-Agent':response.request.headers['User-Agent'],
            }
        data = {
                'login':'Login',
                'password':'tls2019',
                'redirect': redirect,
                'sid':sid,
                'username':'saikrishnatls'
                }
        if sid:
            yield FormRequest('http://torum6uvof666pzw.onion/ucp.php?mode=login&sid=%s'%(sid),callback = self.home_page,headers = headers,formdata= data)

    def home_page(self,response):
        forums = response.xpath('//div[@class="forabg"]//ul[@class="topiclist forums"]//li[@class="row"]//a[@class="forumtitle"]/@href').extract()
        for forum in forums:
            if 'php?' in forum:
                forum = forum.replace('./','http://torum6uvof666pzw.onion/')
                try:
		    yield Request(forum,callback = self.threads_pages)
		except:pass

    def threads_pages(self,response):
        npage = response.xpath('//h2[@class="forum-title"]/..//div[@class="action-bar bar-top"]/div[@class="pagination"]//li[@class="active"]//following-sibling::li//@href').extract_first()
        if npage:
            npage = npage.replace('./','http://torum6uvof666pzw.onion/')
            yield Request(npage, callback = self.threads_pages)
        threads = response.xpath('//ul[@class="topiclist topics"]//li[contains(@class,"row bg")]//a[@class="topictitle"]/@href').extract()
        for thread in threads:
            if './viewtopic' in thread:
                thread = thread.replace('./viewtopic','http://torum6uvof666pzw.onion/viewtopic')
                yield Request(thread,callback = self.meta_page)

    def meta_page(self,response):
        thread_title = ''.join(response.xpath('//div[@class="page-body"]//h2[@class="topic-title"]//a/text()').extract()) or 'Null'
        thread_url = response.url
        category = response.xpath('//ul[@role="menubar"]//span[@itemprop="title"]/text()').extract()[1] or 'Null'
        sub_category = response.xpath('//ul[@role="menubar"]//span[@itemprop="title"]/text()').extract()[2] or 'Null'
	sub_categoryurl = response.xpath('//ul[@role="menubar"]//span[@class="crumb"]//a/@href').extract()[2].replace('./', '/')
	if sub_categoryurl:
	    sub_category_url = 'http://torum6uvof666pzw.onion' + sub_categoryurl
	if  sub_categoryurl == '':
	    sub_category_url = 'Null'
        nodes = response.xpath('//div[contains(@class,"post has-profile bg1")] | //div[contains(@class,"post has-profile bg2")]')
        npage = response.xpath('//h2[@class="forum-title"]/..//div[@class="action-bar bar-top"]/div[@class="pagination"]//li[@class="active"]//following-sibling::li//@href').extract_first()
        if npage:
            npage = npage.replace('./','http://torum6uvof666pzw.onion/')
            yield Request(npage, callback = self.meta_page)
	ord_in_thread =0
        for node in nodes:
	    ord_in_thread = ord_in_thread+1    
            author=  ''.join(node.xpath('.//div[@class="postbody"]//strong//a//text()').extract()).strip() 
            author_url =  ''.join(node.xpath('.//div[@class="postbody"]//strong//a//@href').extract()).replace('./','http://torum6uvof666pzw.onion/')
	    if author_url == '':
		author_url = 'Null'
            text = clean_text(' '.join(node.xpath('.//div[@class="postbody"]//div[@class="content"]//text() | .//div[@class="postbody"]//div[@class="content"]//blockquote').extract())) or 'Null'
            blockquote = node.xpath('.//div[@class="postbody"]//div[@class="content"]//blockquote').extract()
            for quote in blockquote:
                text = text.replace(quote,'Quote ')

            if not author:
                author = 'Null'
            post_title = ''.join(node.xpath('.//div[@class="postbody"]//h3//a/text()').extract()) or 'Null'
            post_url =  ''.join(node.xpath('.//div[@class="postbody"]//ul//li//a/@href').extract()).replace('./','http://torum6uvof666pzw.onion/')
	    if post_url == '':
		post_url = 'Null'
            post_id= re.sub('(.*)&p=','',post_url) or 'Null'
            pub = ''.join(node.xpath('.//div[@class="postbody"]//p//span//following-sibling::text()').extract()).replace("\n","").replace("\t","")
            publish =''.join( re.findall('\d+ \w+ \d+',pub))
            publish_time = datetime.datetime.strptime(publish,'%d %b %Y')
            publish_epoch = time.mktime(publish_time.timetuple())*1000
            #publish_epoch = time.mktime(publish_time.timetuple())*1000
	    if publish_epoch:
		month_year = get_index(publish_epoch)
	    else:
		import pdb;pdb.set_trace()

            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
            reference_url = response.url
            thread_url = response.url
            if 'start' in reference_url:
                test = re.findall('(.*)&start',reference_url)
                thread_url = ''.join(test)
            else:
                thread_url = reference_url
            all_links = 'Null'
	    author_data = {
		'name':author,
		'url':author_url
		}
	    post = {
		'cache_link':'',
		'section':category,
		'language':'english',
		'require_login':'true',
		'sub_section':sub_category,
		'sub_section_url':sub_category_url,
		'post_id':post_id,
		'post_title':post_title,
		'ord_in_thread':int(ord_in_thread),
		'post_url':post_url,
		'post_text':text,
		'thread_title':thread_title,
		'thread_url':thread_url
		}
            json_posts = {
		    'record_id':re.sub(r"\/$", "", post_url.replace(r"https", "http").replace(r"www.", "")),
		    'hostname':'torum6uvof666pzw.onion',
                    'domain':'torum6uvof666pzw.onion',
		    'sub_type':'darkweb',
		    'type':'forum',
		    'author':json.dumps(author_data),
		    'title':thread_title,
		    'text':text,
		    'url':post_url,
		    'original_url':post_url,
		    'fetch_time':fetch_time,
		    'publish_time':publish_epoch,
		    'link_url':all_links,
		    'post':post
		    }
            sk = md5_val(post_url)
	    #query={"query":{"match":{"_id":sk}}}
            #res = es.search(body=query)
            #if res['hits']['hits'] == []:
	    es.index(index=month_year, doc_type='post', id=sk, body=json_posts)
            
	    auth_meta = {'publish_time': publish_epoch}
            json_posts.update({
                'post_id': post_id,
                'auth_meta': json.dumps(auth_meta),
                'crawl_status':0,
                'links': author_url
            })
            self.cursor.execute(crawl_query, json_posts)
            self.conn.commit()
