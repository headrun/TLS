import hashlib
import deathbycaptcha
from scrapy.http import FormRequest
import requests
from onionwebsites.utils import *


class Torum(Spider):
    name = "torum_authors"
    start_urls = ['http://torum6uvof666pzw.onion']

    def __init__(self):
         self.conn = MySQLdb.connect(host="localhost", user=DATABASE_ID, passwd=DATABASE_PASS, db="posts", charset="utf8", use_unicode=True)
         self.cursor = self.conn.cursor()

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def parse(self, response):
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
            client.report(captcha["captcha"])
            yield Request('http://torum6uvof666pzw.onion',callback = self.parse,dont_filter = True)
            return
        data = {
                'input':  str(val),
                'submit':'Verify'
                }
        cookies = response.meta.get('cook')
        import pdb;pdb.set_trace()
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
    

    def home_page(self, response):
	select_que = 'select DISTINCT(links) from torum_authors_crawl where crawl_status = 0'
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
            meta_query = 'select DISTINCT(auth_meta) from torum_authors_crawl where links = "%s"'%url
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('publish_time',''))
            meta = {'publish_time':set(activetime)}
            if url:
                yield Request(url, callback = self.parse_data,meta = meta)


    def parse_data(self, response):
        json_posts = {}
        domain = "torum6uvof666pzw.onion"
        username = ''.join(response.xpath('//dl[@class="left-box details profile-details"]//dd//span/text()').extract())
        join_date = response.xpath('//div[@class="column2"]//dt[contains(text(),"Joined:")]/following-sibling::dd/text()').extract()[0]
        join = datetime.datetime.strptime(join_date,'%d %b %Y')
        joindate = time.mktime(join.timetuple())*1000
        last_active = response.xpath('//div[@class="column2"]//dt[contains(text(),"Joined:")]/following-sibling::dd/text()').extract()[1]
        last = datetime.datetime.strptime(last_active,'%d %b %Y')
        lastactive = time.mktime(last.timetuple())*1000
        totalposts = ''.join(response.xpath('//div[@class="column2"]//dt[contains(text(),"Total posts:")]/following-sibling::dd/text()').extract()).replace('|','').strip()

        groups= ''.join(response.xpath('//div[@class="inner"]//dt[contains(text(),"Description:")]/preceding-sibling::dd/text()').extract()).strip()
        activetimes_ =  response.meta.get('publish_time')
        activetimes = []
        activetimes = activetime_str(activetimes_,totalposts)
        fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
        contact_info = []
        email = ''.join(response.xpath('//dt[contains(text(),"XMPP:")]/preceding-sibling::dd/text()').extract())
        xmpp = ''.join(response.xpath('//dt[contains(text(),"XMPP:")]/following-sibling::dd/text()').extract())
        if email:
            contact_info.append({'user_id':email,'channel':'EMAIL'})
        if xmpp:
            contact_info.append({'user_id':xmpp,'channel':'XMPP'})
        json_posts.update({
                'username': username,
                'domain': domain,
                'auth_sign':'',
                'join_date': joindate,
                'lastactive': lastactive,
                'totalposts': totalposts,
                'fetch_time': fetch_time,
                'groups': groups,
                'reputation': '',
                'credits': '',
                'awards': '',
                'rank': '',
                'activetimes': activetimes,
                'contact_info': str(contact_info),
        })
        sk = md5_val(username)
        doc_to_es(id=sk,body=json_posts,doc_type='author')    
       
