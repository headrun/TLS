import scrapy
from scrapy.spider import Spider
from scrapy.selector import Selector
from scrapy.http import Request
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import json
import MySQLdb
import time
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from antionline_xpaths import *
import utils
query_authors = utils.generate_upsert_query_authors('antionline')


class formus(scrapy.Spider):
    name="antionline_author"
    allowed_domain = ["http://www.antionline.com/"]
    start_urls = ["http://www.antionline.com/forum.php"]

    handle_httpstatus_list = [403]

    def __init__(self, *args, **kwargs):
        self.conn = MySQLdb.connect(db="antionline",host="localhost",user="root",passwd="",use_unicode=True,charset="utf8")
        self.cursor = self.conn.cursor()
        select_query = 'select DISTINCT(links) from antionline_crawl;'
        self.cursor.execute(select_query)
        self.data = self.cursor.fetchall()

    def parse(self, response):
        urls = []
        for da in self.data:
	    urls.append(da[0])
        for url in urls:
            meta_query = 'select DISTINCT(auth_meta) from antionline_crawl where links = "%s"'%url.encode('utf8')
            self.cursor.execute(meta_query)
            meta_query = self.cursor.fetchall()
            activetime=[]
            for da1 in meta_query:
                meta = json.loads(da1[0])
                activetime.append(meta.get('time',''))

            publish_time = set(activetime)
            meta = {'publish_time' : publish_time}
            if url and meta:
                yield Request(url, callback=self.parse_author,meta = meta)



    def parse_author(self, response):
        sel = Selector(response)
        username = ''.join(sel.xpath(USER_NAME).extract()).strip().encode('ascii','ignore').replace('\t','').replace('\n','')
        if username == "":
            username = ''
        if username:
           query = 'update blackhat_status set crawl_status = 1 where reference_url = %(url)s'
           json_data = {'url':reference_url}
           self.cursor.execute(query,json_data)
        domain = "www.antionline.com"
        post_count = ''.join(sel.xpath(TOTAL_POST_COUNT).extract()).replace('\t', '').replace('\n', '')
        fetchtime = int(datetime.datetime.now().strftime("%s")) * 1000

	    #join_dates = ''.join(sel.xpath(JOIN_DATE).extract()).replace('\t', '').replace('\n', '')
	    #joindate = datetime.datetime.strptime(join_dates, '%m/%d/%Y %I:%M %p')
	    #joindate = datetime.datetime.strptime(join_dates, '%b %Y')
        #join_date = time.mktime(joindate.timetuple())*1000
        join_date = ''
        author_signature = ''.join(sel.xpath(AUTHOR_SIGNATURE).extract())
        lastactive = ''
        post_count = ''
        groups = ''.join(sel.xpath('//span[@class="usertitle"]//text()').extract()).replace('\n','').replace('\t','').replace('\r','')
        reputation = ''
        rank = ''
        activetimes_ = response.meta.get('publish_time')
        activetimes = []
        for activetime in activetimes_:
            try:
                dt = time.gmtime(int(activetime)/1000)
                post_count = ''.join(sel.xpath(TOTAL_POST_COUNT).extract()).replace('\t', '').replace('\n', '')
                activetime = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),post_count)
                activetimes.append(activetime)
            except:
                activetime = ' '
                activetimes.append(activetime)

    	json_authors = {}
        json_authors.update({'user_name' : username,
                          'domain' : domain,
                          'crawl_type' : "keepup",
                          'author_signature': author_signature,
                          'join_date' : join_date,
                          'last_active' : lastactive,
                          'total_posts' : post_count,
                          'fetch_time' : fetchtime,
                          'groups' : groups,
                          'reputation' : reputation,
                          'credits' : '',
                          'awards' : '',
                          'rank' : rank,
                          'active_time' : ''.join(activetimes),
                          'contact_info' : '',
                          'reference_url' : response.url
            })
        self.cursor.execute(query_authors, json_authors)




