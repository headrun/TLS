from scrapy.http import FormRequest
from scrapy.http import Request
from scrapy.selector import Selector
import scrapy
import MySQLdb
import json
import datetime
import json
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import myutils
import re
import xpaths
query_posts = myutils.generate_upsert_query_posts('posts_monopoly')


class Monopoly(scrapy.Spider):
    name = "monopoly_post"

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts_monopoly",host="localhost",user="root",passwd="qwerty123" , use_unicode = True , charset = 'utf8')
        self.cursor = self.conn.cursor()
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        headers = {
            'Connection': 'keep-alive',
            'Pragma': 'no-cache',
            'Cache-Control': 'no-cache',
            'Origin': 'https://monopoly.ms',
            'Upgrade-Insecure-Requests': '1',
            'Content-Type': 'application/x-www-form-urlencoded',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Referer': 'https://monopoly.ms/',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
        }
        data = {
            'vb_login_username': 'inqspdr',#pass:mi110n@irinq
            'vb_login_password': '',
            's': '',
            'securitytoken': 'guest',
            'do': 'login',
            'vb_login_md5password': 'c823b3822a485d448a68ee415f5eea59',
            'vb_login_md5password_utf': 'c823b3822a485d448a68ee415f5eea59'
        }
        url = 'https://monopoly.ms/login.php?do=login'
        yield FormRequest(url, callback=self.parse_next, formdata=data)

    def parse_next(self, response):
        headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/69.0.3497.92 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
}
        cooks = response.headers.getlist('Set-Cookie')
        cookies = {}
        [cookies.update({c.split(";")[0].split("=")[0]:c.split(";")[0].split("=")[1]}) for c in cooks]
        url = 'https://monopoly.ms/'
        yield Request(url, callback=self.parse_next1, headers = headers,cookies = cookies)


    def parse_next1(self,response):
	if "inqspdr" in response.body:
            sel = Selector(response)
            url_que = "select distinct(post_url) from monopoly_status where crawl_status = 0 "
            self.cursor.execute(url_que)
            data = self.cursor.fetchall()
            for url in data:
	        yield Request(url[0], callback = self.parse_thread)
        

    def parse_thread(self, response):
        sel = Selector(response)
        domain = "monopoly.ms"
        crawl_type = ''
        try:
	    category = ''.join(sel.xpath(xpaths.CATEGORY).extract()[1])
	except:
	    pass
        try:
            sub_category = '["' + ''.join(sel.xpath(xpaths.SUB_CATEGORY).extract()[2]) + '"]'
	except:
	    pass
        thread_title = ''.join(sel.xpath(xpaths.THREAD_TITLE).extract()).replace('\n','').replace('\r','').replace('\t','')
        nodes = sel.xpath(xpaths.NODES)
        for node in nodes:
            author = ''.join(node.xpath(xpaths.AUTHOR).extract())
            author_url = ''.join(node.xpath(xpaths.AUTHOR_URL).extract())
            if  'member' in author_url :
                author_url = "https://monopoly.ms/" + author_url
	    else :
		author_url = ''
            post_url = ''.join(node.xpath(xpaths.POST_URL).extract())
            if 'http'and 'https' not in post_url :
                post_url = "https://monopoly.ms/" + post_url
            post_id = ''.join(re.findall('\p=\d+',post_url)).replace('p=','').strip()
            post_title = ''
            publish_times = ''.join(node.xpath(xpaths.PUBLISH_TIME).extract()).replace('\r','').replace('\n','').replace('\t','')
            try:
	        publish_date = datetime.datetime.strptime(publish_times,'%d.%m.%Y, %H:%M')
	    except:
	        import pdb;pdb.set_trace()
            publish_time =time.mktime(publish_date.timetuple())*1000
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            Text = ' '.join(node.xpath(xpaths.TEXT).extract()).replace(u'\u0426\u0438\u0442\u0430\u0442\u0430:',u'\u0426\u0438\u0442\u0430\u0442\u0430: %s'%'Quote')
            text = re.sub('\s\s+', ' ', Text)
            thread_url = response.url
            Links = node.xpath(xpaths.LINKS).extract()
            links = []
            for Link in Links:
                if 'http:' not in Link and 'https:' not in Link:
                    al_links = "https://monopoly.ms/" + Link
                    links.append(al_links)
                else:
                    links.append(Link)

            all_links = str(links)
            reference_url = response.url
            if '&page=' not in response.url:
                crawl_type = 'keepup'
            else:
                crawl_type = 'catchup'
            if '&page=' in reference_url:
                test = re.findall('&page=\d+',reference_url)
                thread_url = reference_url.replace(''.join(test),"")
            else:
                thread_url = reference_url
            json_posts = {'domain': domain,
                          'crawl_type': crawl_type,
                          'category': category,
                          'sub_category': sub_category,
                          'thread_title': thread_title,
                          'post_title' : '',
                          'thread_url': thread_url,
                          'post_id': post_id,
                          'post_url': post_url,
                          'publish_epoch': publish_time,
                          'fetch_epoch': fetch_epoch,
                          'author': author,
                          'author_url': author_url,
                          'post_text': myutils.clean_text(Text),
                          'all_links': all_links,
                          'reference_url': response.url
            }
            self.cursor.execute(query_posts, json_posts)
        page_nav= set(sel.xpath(xpaths.PAGENAV).extract())
	for page in page_nav:
	    if "http" not in page:
		page = "https://monopoly.ms/" + page
		yield Request(page, callback = self.parse_thread,meta = {'crawl_type':'catch_up'})
            if page:
		pno = ''.join(re.findall('&page=\d+',page))
                if crawl_type == 'keep_up':
		    page = response.url + pno
		else:
		    page = re.sub('&page=\d+',pno,response.url)
