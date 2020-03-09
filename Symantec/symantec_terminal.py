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

def check_name(Naming, Type, year):
        if Type != '':
            if ('Exp.CVE' in Naming) or ('ER.Heur!gen1' in Naming) or ('SONAR.PUA!gen10' in Naming) or ('Adware.Gen.3' in Naming) or ('PUA' in Naming) or ('SecurityRisk.Capsferv' in Naming) or ('Remacc.WinPCGuard' in Naming) or ('SecurityRisk.Gen2' in Naming) or ('!' in Naming):
                return False
            else:
                return True
        else:
            if ('Exp.CVE' in Naming) or ('ER.Heur!gen1' in Naming) or ('SONAR.PUA!gen10' in Naming) or ('Adware.Gen.3' in Naming) or ('PUA' in Naming) or ('SecurityRisk.Capsferv' in Naming) or ('Remacc.WinPCGuard' in Naming) or ('SecurityRisk.Gen2' in Naming) or ('!' in Naming):
                return False
            else:
                return True


class  Symantec():
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
	select_que = "select distinct(url) from symantec_browse where crawl_status = 0 limit 10"
        self.cursor.execute(select_que)
        db_data = self.cursor.fetchall()
        for currents in db_data:
            current = currents[0]
	    ig_list = ['https://www.symantec.com/security-center/writeup/2018-052114-1525-99', 'https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-052114-1525-99', 'https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-051413-5628-99',  'https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-051514-0447-99']
	    if current in ig_list:
                self.conn.commit()
                self.close_driver()
		self.close_mysql_conn()
                continue

	    self.driver = self.open_driver()
	    self.driver.get(current)
	    time.sleep(3)
	    sel = Selector(text=self.driver.page_source)
	    if 'vulnerabilities' in self.driver.current_url:
	        try:
		    Naming = ''.join(sel.xpath('//div[@class="content"]/div[@class="text-content"]/h1/text()').extract()).strip()
	    	    Firstseen = ''.join(sel.xpath('//div[@class="content"]/h3[contains(text(),"Date Discovered")]/following-sibling::p[1]/text()').extract()).strip()
		    month_ = Firstseen.split(' ')[0]
		    month = month_[0:3]
                    rest = Firstseen.split(' ')[1:]
                    first_seen_at = ' '.join([month] + rest)
                    first_seen_ep = calendar.timegm(time.strptime(first_seen_at, '%b %d, %Y'))* 1000
                    year = Firstseen.split(',')[-1]
                    Type = ''
		except:
		    continue
	
		if check_name(Naming,Type,int(year)):
		    tist = calendar.timegm(time.gmtime())* 1000
		    Description = ''.join(sel.xpath('//div[@class="content"]/h3[contains(text(),"Description")]/following-sibling::p[1]//text()').extract())
		    doc = {
			'original_name': Naming,
			'name': Naming,
			'source': 'symantec',
			'domain': 'symantec.com',
			'url': current,
			'description': Description,
			'created_at': tist,
			'updated_at': tist,
			'first_seen': first_seen_ep,
			'last_seen': first_seen_ep
			}
		    sk = hashlib.md5(doc['url']).hexdigest()
		    es.index(index="malware_docs", doc_type='malwaredoc', id=sk, body=doc)
		    upd_qry = 'update crawling_sources.symantec_browse set crawl_status = 1 where url = "%s"'
                    values = current
                    self.cursor.execute(upd_qry%values) 
		    self.conn.commit()
                    self.close_driver()
	    
	    else:
                Naming = ''.join(sel.xpath('//div[@class="text-content"]//h1/text()').extract())
		try:
		    type_datas = ''.join(sel.xpath('//div[@name="panel-summary"]//p/strong[contains(text(), "Type")]//..//text()').extract()).replace('\t', '')
		    type_ = type_datas.split('Type:')[1].split('\n')[0].strip()
		except:
		    type_ = ''
		try:
	       	    aka_datas = ''.join(sel.xpath('//div[@name="panel-summary"]//p/strong[contains(text(), "Also Known As")]//..//text()').extract()).replace('\t', '')
		    aka = aka_datas.split('Also Known As:')[1].split('\n')[0].strip()
		except:
		    aka = ''
		try:
		    first_datas = ''.join(sel.xpath('//div[@name="panel-summary"]//p/strong[contains(text(), "Discovered")]//..//text()').extract()).replace('\t', '')
		    first_seen = first_datas.split('Discovered:')[1].split('\n')[0].strip()
	     	    month_ = first_seen.split(' ')[0]
		    month = month_[0:3]
		    rest = first_seen.split(' ')[1::]
		    first_seen_at = ' '.join([month] + rest)
		    first_seen_ep = calendar.timegm(time.strptime(first_seen_at, '%b %d, %Y'))* 1000
		except:
		    first_seen_ep = None
		try:
		    Upd_datas = ''.join(sel.xpath('//div[@name="panel-summary"]//p/strong[contains(text(), "Updated")]//..//text()').extract()).replace('\t', '')
		    last_seen = Upd_datas.split('Updated:')[1].split('\n')[0].strip()
		    month_ = last_seen.split(' ')[0]
		    month = month_[0:3]
		    rest = last_seen.split(' ')[1:]
		    if 'PM' in rest:
			time_ = rest[2].split(':')
			hour = int(time_[0]) + 12
			ptime = [':'.join([str(hour)] + time_[1:])]
			rest_ = rest[:2]
			last_seen_at = ' '.join([month] + rest_ + ptime)
		    else:
			rest_ = rest[:-1]
			last_seen_at = ' '.join([month] + rest_)
		    last_seen_ep = calendar.timegm(time.strptime(last_seen_at, '%b %d, %Y %H:%M:%S'))* 1000
		except:
		    last_seen_ep = None
		try:
		    pl_datas = ''.join(sel.xpath('//div[@name="panel-summary"]//p/strong[contains(text(), "Systems Affected")]//..//text()').extract()).replace('\t', '')
		    platform = pl_datas.split('Systems Affected:')[1].split('\n')[0].strip()
		except:
		    platform = ''
		try:
		    year = first_seen_at.split(',')[1].strip()
		except:
		    year = 0
		if check_name(Naming, type_, int(year)):
		    extracted = Naming.split('.')
		    if len(extracted) > 1:
                        extracted_name = extracted[1]
                    else:
                        extracted_name = extracted[0] 
		    try:
		        summary_text = ''.join(sel.xpath('//div[@class="content"]//div[@id="summary"]//p[2]//text()').extract()).replace('\n', '').replace('\t', '').strip()
		    except:
			summary_text = ''

		    technical_details = ''.join(sel.xpath('//div[@class="content"]//div[@id="technicaldescription"]//text()').extract()).replace('\n', '').replace('\t', '').strip()

		    removal_text = ''.join(sel.xpath('//div[@class="content"]//div[@id="removal"]//text()').extract()).replace('\n', '').replace('\t', '').strip()

		    tist = calendar.timegm(time.gmtime())* 1000
		    if first_seen_ep == None:
			first_seen_ep = last_seen_ep
		    if last_seen_ep == None:
			last_seen_ep = first_seen_ep
		    doc = {
			'original_name': Naming,
			'name': extracted_name,
			'source': 'symantec',
			'domain': 'symantec.com',
			'url': current,
			'description': summary_text,
			'type': type_,
			'platform': platform,
			'removal': removal_text,
			'technical_details': technical_details,
			'created_at': tist,
			'updated_at': tist,
			'first_seen': first_seen_ep,
			'last_seen': last_seen_ep
			}
		    sk = hashlib.md5(doc['url']).hexdigest()
		    pprint(doc)
		    es.index(index="malware_docs", doc_type='malwaredoc', id=sk, body=doc, request_timeout=30)
		    upd_qry = 'update crawling_sources.symantec_browse set crawl_status= 1 where url = "%s"'
                    values = current
                    self.cursor.execute(upd_qry%values) 		
		    self.conn.commit()
		    self.close_driver()
		else:
		    upd_qry = 'update crawling_sources.symantec_browse set crawl_status= 1 where url = "%s"'
                    values = current
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
    obj = Symantec()

