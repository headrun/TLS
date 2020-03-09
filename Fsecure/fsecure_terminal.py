import os, sys
import time
import calendar
import subprocess
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


class  Fsecure():
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
	select_que = "select distinct(data_links) from f_secure where crawl_status = 0 limit 250"
        self.cursor.execute(select_que)
        db_data = self.cursor.fetchall()
        for dbs in db_data:
            db = dbs[0]
	    if 'detail' in db:
		upd_qry = 'update crawling_sources.f_secure set crawl_status = 1 where data_links = "%s"'
                values = db
                self.cursor.execute(upd_qry%values)
                self.conn.commit()
                self.close_driver()
		self.close_mysql_conn()
                continue
	    
	    self.driver = self.open_driver()
	    self.driver.get(db)
	    time.sleep(2)
	    sel=Selector(text = self.driver.page_source)
	    category = ''.join(sel.xpath('//div[@class="col-xs-7 col-sm-2 p-t-1"]//p//span[@class="desc-category"]/text()').extract()).strip()
	    platform = ''.join(sel.xpath('//div[@class="col-xs-7 col-sm-2 p-t-1"]//p//span[@class="desc-platforms"]/text()').extract()).strip()
	    type_ = ''.join(sel.xpath('//div[@class="col-xs-7 col-sm-2 p-t-1"]//p//span[@class="desc-type"]/text()').extract()).strip()
	    aliases = ''.join(sel.xpath('//div[@class="col-xs-7 col-sm-10 p-t-1"]//p//span[@class="desc-aliases"]/text()').extract()).strip()
	    title_text = ''.join(sel.xpath('//div[@class="col-xs-12 p-t-3 p-b-2"]//span[@class="desc-name"]/text()').extract()).strip()
	    try:
	    	first_seen_xpath = ''.join(sel.xpath('//p[@class="m-b-0 font-gray-7"]//span[contains(text(), "Date Created:")]/following-sibling::span[@class="desc-datecreated"]/text()').extract())
		created = first_seen_xpath.split('.')[0]
		first_seen = calendar.timegm(time.strptime(created, '%Y-%m-%d %H:%M:%S'))* 1000
	    except:
		first_seen = None
	    try:
		last_seen_xpath = ''.join(sel.xpath('//p[@class="m-b-0 font-gray-7"]//span[contains(text(), "Date Last Modified:")]/following-sibling::span[@class="desc-datemodified"]/text()').extract())
		modified = last_seen_xpath.split('.')[0]
		last_seen = calendar.timegm(time.strptime(modified, '%Y-%m-%d %H:%M:%S'))* 1000
	    except:
		last_seen = None
	    summary_text = ''.join(sel.xpath('//div[@class="desc-summary"]//text()').extract()).strip().replace('\n', ' ')
	    removal_text = ''.join(sel.xpath('//div[@class="desc-removal"]//text()').extract()).strip().replace('\n', ' ')
	    technical_text = ''.join(sel.xpath('//div[@class="desc-technicaldetails"]//text()').extract()).strip().replace('\n', ' ')
	    tist = calendar.timegm(time.gmtime())* 1000 
	    doc = {
		'original_name': title_text,
		'source': 'fsecure',
		'domain': 'f-secure.com',
		'url': db,
		'description': summary_text,
		'aliases': aliases,
		'Type': type_,
		'category':category,
		'platform': platform,
		'removal': removal_text,
		'technical_details': technical_text,
		'created_at': tist,
		'updated_at': tist,
		'first_seen': first_seen,
		'last_seen': last_seen
		}
	   
	    pprint(doc)
	    sk = hashlib.md5(doc['url']).hexdigest()
	    self.es.index(index="malware_docs", doc_type='malwaredoc', id=sk, body=doc)
	    upd_qry = 'update crawling_sources.f_secure set crawl_status = 1 where data_links = "%s"'
            values = db
            self.cursor.execute(upd_qry%values)
	    self.conn.commit()
	    self.close_driver()
	self.close_mysql_conn()
    def open_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-extensions")
        options.add_argument('--headless')
        driver = webdriver.Chrome(chrome_options=options)
        return driver


    def close_driver(self):
        try:
            self.driver.quit()
        except Exception as exe:
            raise exe()
	


if __name__ == '__main__':
    obj = Fsecure()

