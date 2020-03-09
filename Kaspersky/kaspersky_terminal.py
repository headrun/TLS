import os, sys, csv, hashlib
import calendar
import traceback
from optparse import OptionParser
import datetime
import time
import MySQLdb
import json
import re
import selenium
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from scrapy.selector import Selector
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from w3lib.http import basic_auth_header
import random
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from elasticsearch import Elasticsearch
from pprint import pprint
import hashlib

class Kaspersky():
    def __init__(self):
        self.conn,self.cursor = self.mysql_conn()
        self.count = 0
        self.driver = self.open_driver()
        self.parse()
        self.conn.commit()
        self.conn.close()
        self.close_driver()

    def mysql_conn(self):
        conn=MySQLdb.connect(db="crawling_sources",host="localhost",user="root",passwd="qwe123",use_unicode=True,charset="utf8mb4")
        cursor=conn.cursor()
        return conn,cursor

    def parse(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        query = 'select distinct(data_links) from crawling_sources.threats_kaspersky where crawl_status=0'
        self.cursor.execute(query)
        db_data = self.cursor.fetchall()
        self.conn.commit()
        for db_values in db_data:
            db_value = db_values[0]
            time.sleep(1)
            self.driver.get(db_value)
            time.sleep(5)
            response=Selector(text = self.driver.page_source)
            try:
                date_name = ''.join(response.xpath('//tr[@class="line_info"]/td[@class="cell_two "]//strong//text()').extract()).strip()

                if date_name:
                    date1 = str(date_name)
                    date_ep = calendar.timegm(time.strptime(date1, '%m/%d/%Y'))* 1000
                else:
                    date_ep = None
            except:
                date_ep = None
            try:
                class_name = ''.join(response.xpath('//tr[@class="line_info"]//td[contains(text() ,"Class")]/following-sibling::td/a//text()').extract())
                if class_name:
                    class_ = class_name.strip()
                else:
                    class_ = ''
            except:
                class_ = ''

            try:
                platform_name=''.join(response.xpath('//tr[@class="line_info"]//td[contains(text() ,"Platform")]/following-sibling::td/a//text()').extract())
                if platform_name:
                    platform = platform_name.strip()
                else:
                    platform = ''
            except:
                platform = ''

            ### title
            title_xpath = '//h1[@class="main_title average_title"]//text()'
            title = ''.join(response.xpath((title_xpath)).extract())
            original_name = title
            na_con = title.split('.')[1:]
            nam_conv = '.'.join(na_con)
            naming_convention = title.split('.')[0]+ '.' + '{' + nam_conv + '}'
            name_extraction = title.split('.')[-1]
            ###description
            description_xpath = '//tr[@class="line_info"]//td[contains(text(), "Desc")]//following-sibling::td//text()'
            description = ''.join(response.xpath((description_xpath)).extract()).replace('\n',' ').strip()
            ###technical description
            try:
                tech_description_xpath = '//td[@class="cell_two "]//h2[contains(text(), "Technical Details")]/..//text()'
                tech_description = ''.join(response.xpath((tech_description_xpath)).extract()).replace('\n','').strip()
            except:
                tech_description = ''
            if 'Removal instructions' in tech_description:
                removal = tech_description.split('Removal instructions')[1]
                tech_description = tech_description.split('Removal instructions')[0]
            else:
                removal = ''
            ###Target Countries
            all_count = []
            try:
                count_xpath = '//table[@class="most_attacked_countries"]//tr//text()'
                count_extracted = response.xpath((count_xpath)).extract()
                for count_extrr in count_extracted:
                    countr = count_extrr.find_elements_by_xpath('td')[1].text
                    all_count.append(countr)
            except:
                Countries = ''
            tist = calendar.timegm(time.gmtime())* 1000
            all_count = all_count[1:]
            doc = {'name': name_extraction, 
		   'original_name': original_name,
		   'source': 'kaspersky', 
		   'domain': 'kaspersky.com',
		   'url': db_value ,
		   'description': description, 
	           'type': class_,
		   'platform': platform,
		   'removal': removal,
		   'technical_details': tech_description,
		   'first_seen': date_ep,
		   'last_seen': date_ep, 
		   'target_countries': all_count,
		   'created_at': tist,
		   'updated_at': tist
            }
	    sk = hashlib.md5(doc['url']).hexdigest()
            self.es.index(index="malware_docs", doc_type='malwaredoc', id=sk, body=doc)
            res = self.es.index(index="malware_docs", doc_type='malwaredoc', id=hashlib.md5(db_value).hexdigest(), body=doc)

            upd_qry = 'update crawling_sources.threats_kaspersky set crawl_status= 1 where data_links = "%s"'
            values = db_value
            self.cursor.execute(upd_qry%values)
            self.conn.commit()
            self.cursor.execute(query,doc)
            self.conn.commit()
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


if __name__ =="__main__":
    obj = Kaspersky()
