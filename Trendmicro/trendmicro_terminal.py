import time
import calendar
from selenium import webdriver
from pprint import pprint
from elasticsearch import Elasticsearch
es = Elasticsearch(['10.2.0.90:9342'])
import hashlib
from pprint import pprint
import datetime
import scrapy
from scrapy.selector import Selector
import json
import MySQLdb
import random
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from tbselenium.tbdriver import TorBrowserDriver
from tbselenium.utils import start_xvfb, stop_xvfb


class  Trendmicro():
    def __init__(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn, self.cursor = self.mysql_conn()
        self.count = 0
        self.driver = self.open_driver()
        self.parse()
        self.conn.commit()
        self.conn.close()
        self.close_driver()
	self.close_mysql_conn()

    def mysql_conn(self):
        conn=MySQLdb.connect(db="crawling_sources", host="localhost", user="root", passwd="qwe123", use_unicode=True, charset="utf8mb4")
        cursor=conn.cursor()
        return conn,cursor

    def parse(self):
        global connection
        global cursor
	select_que = "select distinct(data_links) from trendmicro_crawl where crawl_status = 1 limit 10"
        self.cursor.execute(select_que)
        data = self.cursor.fetchall()
        for url in data:
            url = url[0]
	    #url = 'https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/vbs_kjworm.sma'
	    try:
		self.driver = self.open_driver()
	    	self.driver.get(url)
	    except:
	    	import pdb;pdb.set_trace()
	    time.sleep(2)
	    reference_url =  url.encode('utf8')
	    sel=Selector(text = self.driver.page_source)
	    title = ''.join(sel.xpath('//section[@class="articleHeader"]//h1[contains(@class,"lessen_h1")]/text()').extract())
	    try:
	    	name_extracted = title.split('_')[1]
	    except:
	    	name_extracted = '.'.join(title.split('.')[1::])
	    name_extraction = str(name_extracted.split('.')[0]).strip()
	    if name_extraction.startswith('CVE'):
	        upd_qry = 'update crawling_sources.trendmicro_crawl set crawl_status = 9 where data_links = "%s"'
	    	values = url
	    	self.cursor.execute(upd_qry%values)
	    	self.conn.commit()
	    	self.close_driver()
		self.close_mysql_conn()
	    	continue
	              
            try:
	        type_ = ''.join(sel.xpath('//div[@class="iconDetails"]//ul//li[contains(text(), "Threat")]/text()').extract())
	        threat_type = type_.split(':')[1].strip()
            except:
                import pdb;pdb.set_trace()
	    platform = ''.join(sel.xpath('//div[@class="entityHeader"]//strong[contains(text(), "PLATFORM")]/following-sibling::p/text()').extract()).strip()
	    try:
	        aliases = ''.join(sel.xpath('//div[@class="entityHeader"]//strong[contains(text(), "ALIASES")]/following-sibling::p/text()').extract()).strip()
	    except:
		aliases = ''
	    first_seen = ''.join(sel.xpath('//span[contains(text(), "FIRST VSAPI PATTERN DATE: ")]/following-sibling::text()').extract()).strip()
	    if first_seen:
	    	first_seen_ep = calendar.timegm(time.strptime(first_seen, '%d %b %Y'))* 1000
	    else:
	    	first_seen_ep = None
	    last_seen = ''.join(sel.xpath('//div[@class="HolderDateShare"]//div[@id="datePub"]/text()').extract()).strip()
	    month_ = last_seen.split(' ')[0]
	    month = month_[0:3]
	    rest = last_seen.split(' ')[1:]
	    last_seen_at = ' '.join([month] + rest)
	    if last_seen:
	    	last_seen_ep = calendar.timegm(time.strptime(last_seen_at, '%b %d, %Y'))* 1000
	    else:
	        last_seen_ep = None
	    if first_seen_ep == None:
	    	first_seen_ep = last_seen_ep
	    try:
	        solutions = ' '.join(sel.xpath('//section[@class="accordion"]/div[@class="pane showpane"]//text()').extract())
	    except:import pdb;pdb.set_trace()
	    try:
	    	techno = ' '.join(sel.xpath('//section[@class="accordion"]//div[@id="listDesc"][2]//text()').extract()).strip()
	    except:import pdb;pdb.set_trace()
	    try:
	    	overview = ' '.join(sel.xpath('//section[@class="accordion"]//div[@id="listDesc"][1]//text()').extract()).strip()
	    except:import pdb;pdb.set_trace()
	    tist = calendar.timegm(time.gmtime())* 1000 
	    doc = {
		'name':name_extraction,
		'original_name': title,
		'source': 'trendmicro',
		'domain': 'trendmicro.com',
		'url': url,
		'description': overview,
		'aliases': aliases,
		'platform': platform,
		'type': threat_type,
		'removal': solutions,
		'technical_details': techno,
		'first_seen': first_seen_ep,
		'last_seen': last_seen_ep,
  		'created_at': tist, 
		'updated_at': tist 
		}
	    pprint(doc)
	    sk = hashlib.md5(doc['url']).hexdigest()
	    self.es.index(index="malware_docs_trend_micro", doc_type='malwaredoc', id=sk, body=doc)
	    upd_qry = 'update crawling_sources.trendmicro_crawl set crawl_status = 9 where data_links = "%s"'
            values = url
            self.cursor.execute(upd_qry%values)
	    self.conn.commit()
	    self.close_driver()
	self.close_mysql_conn()
	


    def open_driver(self):
	options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-extensions")
        options.add_argument('--headless')
        options.add_argument('--proxy-server={}'.format('socks5://localhost:9050'))
        options.add_argument("--incognito")
        driver = webdriver.Chrome(chrome_options=options)
        return driver

    def close_driver(self):
        try:
            self.driver.quit()
        except Exception as exe:
            raise exe()

    def close_mysql_conn(self):
        try:
            self.conn.close()
        except Exception as exe:
            raise exe()


if __name__ == '__main__':
    obj = Trendmicro()

