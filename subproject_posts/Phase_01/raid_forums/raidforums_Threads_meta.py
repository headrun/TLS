import logging
from scrapy.http import Request
import csv
import datetime
import scrapy
import time
import re
import MySQLdb
import datetime
import json
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from pyvirtualdisplay import Display
# from selenium import webdriver
import utils
import unicodedata



def clean_spchar_in_text(self, thread_text):
    ''' 
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    thread_text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', thread_text)
    thread_text = re.sub(r'(\n\s*)', '\n', thread_text)
    return thread_text




class Raidforums(scrapy.Spider):
    name = 'raidforums_Threads_meta'
    log_file_name = 'raidforums_Threads_meta_%s.log'%str(datetime.datetime.now()).replace(' ','')
    logging.basicConfig(filename = log_file_name,level=logging.DEBUG)

    def __init__(self, limit= '50', *args, **kwargs):

        super(Raidforums, self).__init__(*args, **kwargs)
        self.query = utils.generate_upsert_query_posts('posts_raidforums')
        self.conn = MySQLdb.connect(db="posts_raidforums",
                                   host="localhost",
                                   user="root",
                                   passwd = "",
                                   use_unicode=True,
                                   charset="utf8")
        self.cursor = self.conn.cursor()
        self.limit = limit
        dispatcher.connect(self.close_conn, signals.spider_closed)
    
    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def start_requests(self):
        que = "select distinct(url) from raidforums_status where status_code = 0 limit %s"%self.limit
        self.cursor.execute(que)
        data = self.cursor.fetchall()
        for url in data:
            up_que = 'update raidforums_status set status_code = 9 where url = "%s"'%MySQLdb.escape_string(''.join(url))
            self.cursor.execute(up_que)
            
        for url in data:
            yield Request(''.join(url),callback = self.parse_comm)

    def parse_comm(self,response):
        logging.info("url:%s",response.url)
        domain = "www.raidforums.com"
        if '?page=' not in response.url:
            crawl_type = 'keepup'
        else: crawl_type = 'catchup'
 
        logging.info("status_code:%s",response.status)
        if response.status != 200 or 302:
            status_code_update = 'update raidforums_status set status_code = 1 where url = "%s"'%MySQLdb.escape_string(response.url)
            self.cursor.execute(status_code_update)
        else:
            # response.status == 200:
            status_code_update = 'update raidforums_status set status_code = 9 where url = "%s"'%MySQLdb.escape_string(response.url)
            self.cursor.execute(status_code_update)
        Category = ''.join(response.xpath('//span[@class="crumbs"]//span/a/text()').extract()[1]).replace('\n','').replace('\r','')

        Subcategory = '['+''.join(response.xpath('//span[@class="crumbs"]//span/a/text()').extract()[2]).replace('\n','"') +']'

        
        thread_type = ''.join(response.xpath('//span[@class="crumbs"]//span/a/text()').extract()[-1]).replace('\n','').replace('\r','')
        thread_Topics = ''.join(response.xpath('//span[@class="crust"]//a/text()').extract())
        thread_Topics = thread_Topics.strip().replace('\n\n', '/').strip('.')
        thread_nodes = response.xpath('//td[@id="posts_container"]//div[@class="post classic "]')
        json_posts = {}
        Thread_url = ''
        Thread_url = response.url
        thread_text, email_id,commantdate = "","",""
        if '?page=' in Thread_url:
             test = re.findall('\?page=\d+',Thread_url)
             Thread_url = Thread_url.replace(''.join(test),'')
        json_posts.update({'domain': domain,
                            'crawl_type': crawl_type,
                            'thread_url': Thread_url,
                            'thread_title' : thread_type
        })

        for i in thread_nodes:
    	    links,links_,link = [],[],[]
            thread_text = ''.join(i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//text() | .//div[@class="post_content"]//div[contains(@class, "post_body")]//img/@alt | .//div[@class="post_content"]//div[contains(@class, "post_body")]//blockquote[@class="mycode_quote"]/@class | .//div[@class="post_content"]//div[contains(@class, "post_body")]//img/@original-title | .//div[@class="post_content"]//div[contains(@class, "post_body")]//a//span[contains(@id, "-tag")]//@id').extract()).strip()
            quote = i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//blockquote[@class="mycode_quote"]/@class').extract()
            if quote:
                thread_text = thread_text.replace(quote[0], " Quote ")
            thread_date = ''.join(i.xpath('.//div[@class="post_content"]//span[@class="post_date"]/span//@title').extract()\
                +i.xpath('.//div[@class="post_content"]//span[@class="post_date"]/text()').extract())     

            if """protected]""" in thread_text:
                mails = i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//@data-cfemail').extract()
                email_id=[utils.decode_cloudflareEmail(id_) for id_ in mails]
                if len(mails) > 1:
                    thread_text =  thread_text.split('[email\xc2\xa0protected]')[0] + email_id[0] + ' ' + thread_text.split('[email\xc2\xa0protected]')[1] + ' ' + email_id[1] + ' ' + thread_text.split('[email\xc2\xa0protected]')[2]
                    
                else:
                    threadtext = thread_text.split('[email\xc2\xa0protected]')
                    if len(threadtext) > 1:
                        thread_text =  thread_text.split('[email\xc2\xa0protected]')[0] + email_id[0]  + " " + thread_text.split('[email\xc2\xa0protected]')[1]
                    else:
                        thread_text =  thread_text.split('[email\xc2\xa0protected]')[0] + email_id[0] + ' '
            commant_date1 = ''.join(i.xpath('.//div[@class="post_head"]//span[@class="post_date"]/span/@title').extract()).split() or ''.join(i.xpath('.//div[@class="post_head"]//span[@class="post_date"]//text()').extract()).split()
            tag = ''.join(i.xpath('.//div[@class="post_content"]//div[contains(@class, "post_body")]//a//span[contains(@id, "-tag")]//@id').extract())
            if "m-tag" in thread_text:
                thread_text = thread_text.replace("m-tag", "[Mod]")
            if "mp-tag" in thread_text:
                thread_text = thread_text.replace("mp-tag", "[Mod+]")
            if "a-tag" in thread_text:
                thread_text = thread_text.replace("a-tag", "[Admin] ")
            if 'd-tag'in thread_text or 'top-d' in thread_text:
                thread_text = thread_text.replace("d-tag", 'Top donator').replace("top-d", 'Top donator')
            if 'o-tag' in thread_text:
                thread_text = thread_text.replace("o-tag", 'Owner')
            commant_date1 = ''.join(re.findall('\d\d-\d\d-\d+,\d\d:\d\d\wM',''.join(commant_date1).replace(' ','')))
            thread_date = ''.join(re.findall('\d\d-\d\d-\d+,\d\d:\d\d\wM',''.join(thread_date).replace(' ',''))) or commant_date1
            try:
                commant_date2 = datetime.datetime.strptime(thread_date, '%m-%d-%Y,%I:%M%p')
            except:
                try:
                    commant_date2 = datetime.datetime.strptime(thread_date, '%d-%m-%Y,%I:%M%p')
                except:pass
            try:
                commantdate = time.mktime(commant_date2.timetuple())*1000
                commant_date = int(time.mktime(commant_date2.timetuple())*1000)
            except:pass
            Fetch_Time = int(round(time.time()*1000))
            commant_author = ''.join(i.xpath('.//div[@class="post_author scaleimages"]//span[@class="owner-hex"]/text()').extract()) or \
                            ''.join(i.xpath('./div//span[@class="largetext"]/a/span/text()').extract()) or \
                            ''.join(i.xpath('.//span[@class="largetext"]//img/@original-title').extract()) or \
                            ''.join(i.xpath('.//span[@class="sparkles-ani"]//text()').extract())or \
                             ''.join(i.xpath('.//span[@class="largetext"]//span[@class="god-hex"]//text()').extract())

            Post_Url_ = ''.join(i.xpath('.//div[@class="post_content"]//div[@class="float_right"]//a/@href').extract())
            if  Post_Url_:
                Post_Url = 'https://raidforums.com/'+Post_Url_
            else:
                Post_Url = 'none'

            if Post_Url != '-':Post_id = ''.join(re.findall('pid\d+',Post_Url)).replace('pid','')
            else: Post_id = 'none'
            links_= i.xpath('.//div[@class="post_content"]//div[@class="post_body scaleimages"]//a//@href | .//div[@class="post_content"]//div[@class="post_body scaleimages"]//img[contains(@src, ".jpg")]/@src | .//div[@class="post_content"]//div[contains(@class, "post_body")]//img[@class="mycode_img"]/@src').extract()
            for link in links_:
                if "http" not in link: link = "https://raidforums.com" + link
                links.append(link)
            if links: links = str(links)
            if not links: links= " "
            a_name = ''.join(i.xpath('.//div[@class="post_author scaleimages"]//span[@class="largetext"]//span/text()').extract())or \
            ''.join(i.xpath('.//span[@class="largetext"]//a//span//text()').extract())

            # passing commant_date,thread_type,thread_Topics using dict format
            # Write data into raidforums_crawl table 
            auth_url = ''.join(i.xpath('.//div[@class="author_avatar"]/a/@href').extract())
            if 'https:/' not in auth_url:auth_url = 'https://raidforums.com/' + auth_url
            json_posts.update({
                            'author_url':auth_url,
                            'all_links':links
                    })
            meta = {'commant_date' : commantdate, 'thread_type' : thread_type.encode('utf8'), 'thread_Topics' : thread_Topics.encode('utf8')}
            

            # write data into raidforums_Threads_details table
             
            thread_text = clean_spchar_in_text(self,thread_text)
            json_posts.update({
                               'category': Category,
                               'sub_category': Subcategory,
                               'post_title': '',
                               'post_id': Post_id,
                               'post_url': Post_Url,
                               'publish_epoch':  commant_date,
                               'fetch_epoch': Fetch_Time,
                               'author': commant_author,
                               'post_text': thread_text,
                               'reference_url': response.url
                })
            self.cursor.execute(self.query, json_posts)
            self.conn.commit()
            meta = {'publish_time': commant_date, 'thread_title': thread_type}
            json_crawl = {
                           'post_id': Post_id,
                           'auth_meta': json.dumps(meta),
                           'links':auth_url,
                           }

            crawl_query = utils.generate_upsert_query_crawl('posts_raidforums')
            self.cursor.execute(crawl_query, json_crawl)
            self.conn.commit() 
        
        # navigating to next page
        looppage_url = set(response.xpath('//a[@class="pagination_next"]/@href').extract())
        for url in looppage_url:
            if 'hhtps:/' or "http:" not in url:url  = 'https://raidforums.com/' +url
            yield Request(url,callback = self.parse_comm)
        # closing DB connection using spider signals


