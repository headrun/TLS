import time
from selenium import webdriver
from pprint import pprint
from elasticsearch import Elasticsearch
#es = Elasticsearch(['10.2.0.90:9342'])
es = Elasticsearch(['http://maldoc.tlssec.net/'])
#es = Elasticsearch(['http://maldoc.tlssec.net/'],http_auth=('headrun','@He@drun$'))
import hashlib
import datetime
import scrapy
from scrapy.selector import Selector
import json
import MySQLdb
from w3lib.http import basic_auth_header
import random
import requests
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from tbselenium.tbdriver import TorBrowserDriver
from tbselenium.utils import start_xvfb, stop_xvfb
from base64 import b64encode

class SandBox(): 
    name = 'vulnerbility'

    def __init__(self):
        self.conn = MySQLdb.connect(host="localhost", user="root", passwd="", db="sandbox")
        self.cursor = self.conn.cursor()


    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()


    def get_driver(self):
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-extensions")
        options.add_argument('--headless')
        options.add_argument('--proxy-server={}'.format('socks5://localhost:9050'))
        options.add_argument("--incognito")
        driver = webdriver.Chrome(chrome_options=options)
        return driver

    def start_requests(self):
        self.driver = self.get_driver()
        time.sleep(1)
        key_que = 'select Distinct(cve_id) from skybox_new_cve where crawl_status = 1 limit 500'
        self.cursor.execute(key_que)
        keys = self.cursor.fetchall()
        skybox_urls = []
	for key in keys:
            key = key[0]
            upd_qry = 'update sandbox.skybox_new_cve set crawl_status = 9 where cve_id = "%s"'
            values = key
            self.cursor.execute(upd_qry%values)
            self.driver.get('https://www.vulnerabilitycenter.com/svc/SVC.html#search=%s'%(key))
            time.sleep(5)
            sel = Selector(text = self.driver.page_source)
            nodes = sel.xpath('//div[@class="GMMNKTXCNDC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL svc-TableRow"] | //div[@class="GMMNKTXCNDC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL GMMNKTXCBEC svc-TableRow"]')
            for node in nodes:
                skybox_url = ''.join(node.xpath('.//td[@cellindex="0"]//a[@target="_top"]/@href').extract())
                skybox_urls.append((skybox_url,key))

        for data in skybox_urls:
            skybox_url,serach_key = data
            self.skybox(skybox_url,serach_key)

        time.sleep(5)
        self.driver.close()


    def skybox(self,skybox_url,serach_key):
        self.driver.get(skybox_url)
        time.sleep(5)
        sel = Selector(text = self.driver.page_source)
        vendors = ''.join(sel.xpath('//table[@class="svc-VulDetailsLeft"]//div[contains(text(),"Vendor")]/../../td[2]//text()').extract())
        skybox_id = ''.join(sel.xpath('//table[@class="svc-VulDetailsLeft"]//div[contains(text(),"Skybox Id")]/../../td[2]//text()').extract())
        cve_id = ''.join(sel.xpath('//table[@class="svc-VulDetailsLeft"]//div[contains(text(),"CVE Id")]/../../td[2]//text()').extract())
        Scanners_and_Other_Sources = ''.join(sel.xpath('//table[@class="svc-VulDetailsLeft"]//div[contains(text(),"Scanners and Other Sources")]/../../td[2]//text()').extract())
        security =  ''.join(sel.xpath('//table[@class="svc-VulDetailsRight"]//div[contains(text(),"Severity")]/../../td[2]//text()').extract())
        cvss_base = ''.join(sel.xpath('//table[@class="svc-VulDetailsRight"]//div[contains(text(),"CVSS Base")]/../../td[2]//text()').extract())
	cvss_temporal = ''.join(sel.xpath('//table[@class="svc-VulDetailsRight"]//div[contains(text(),"CVSS Temporal")]/../../td[2]//text()').extract())
        repoarting_date_ = ''.join(sel.xpath('//table[@class="svc-VulDetailsRight"]//div[contains(text(),"Reporting Date")]/../../td[2]//text()').extract())
        try:
            repoarting_date = (int(time.mktime(time.strptime(repoarting_date_.strip(),'%m/%d/%Y'))) - time.timezone) * 1000
        except:
            repoarting_date = 0
        try:
            last_modification_date_ = ''.join(sel.xpath('//table[@class="svc-VulDetailsRight"]//div[contains(text(),"Last Modification Date")]/../../td[2]//text()').extract())
            last_modification_date = (int(time.mktime(time.strptime(last_modification_date_.strip(),'%m/%d/%Y'))) - time.timezone) * 1000
        except:
            last_modification_date = 0
        effected_products = []
        products = sel.xpath('//div[@class="GMMNKTXCJ3 GMMNKTXCPK"]//span[@class="gwt-InlineHTML" and contains(text(),"Product")]/../../../../../../../..//div[@class="GMMNKTXCHEC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL GMMNKTXCBEC svc-TableRow GMMNKTXCEEC GMMNKTXCKYC"] | //div[@class="GMMNKTXCJ3 GMMNKTXCPK"]//span[@class="gwt-InlineHTML" and contains(text(),"Product")]/../../../../../../../..//div[@class="GMMNKTXCHEC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL svc-TableRow"]  | //div[@class="GMMNKTXCJ3 GMMNKTXCPK"]//span[@class="gwt-InlineHTML" and contains(text(),"Product")]/../../../../../../../..//div[@class="GMMNKTXCHEC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL GMMNKTXCBEC svc-TableRow"] ')
        for prod in products:
            prod_doc = {
                        'Product' : ''.join(prod.xpath('.//td[@cellindex="0"]//text()').extract()),
                        'Vendor' : ''.join(prod.xpath('.//td[@cellindex="1"]//text()').extract()),
                        'Category' : ''.join(prod.xpath('.//td[@cellindex="2"]//text()').extract()),
                        'Affected Versions' : ''.join(prod.xpath('.//td[@cellindex="3"]//text()').extract())
                }
            effected_products.append(prod_doc)
        solutions = []
        external_reff = []
        reff  = sel.xpath('//tr[@class="GMMNKTXCI3"]/td[@class="GMMNKTXCJ3 GMMNKTXCD3"]//span[contains(text(),"Source")]/../../../../../../../..//div[@class="GMMNKTXCHEC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL svc-TableRow"] | //tr[@class="GMMNKTXCI3"]/td[@class="GMMNKTXCJ3 GMMNKTXCD3"]//span[contains(text(),"Source")]/../../../../../../../..//div[@class="GMMNKTXCHEC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL GMMNKTXCBEC svc-TableRow"]')
	for r in reff:
            nef_vals = {
                        'Source':''.join(r.xpath('.//td[@cellindex="0"]//text()').extract()),
                        'Title':''.join(r.xpath('.//td[@cellindex="1"]//text()').extract()),
                        'URL': ''.join(r.xpath('.//td[@cellindex="2"]//text()').extract())
                        }
            external_reff.append(nef_vals)
        solutions = []
        data = sel.xpath('//span[@class="gwt-InlineHTML" and contains(text(),"Name")]/../../../../../../../..//div[@class="GMMNKTXCJ3 GMMNKTXCPK"]/../div[@class="GMMNKTXCHEC"]//table[@class="GMMNKTXCKDC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL GMMNKTXCBEC svc-TableRow"] | //span[@class="gwt-InlineHTML" and contains(text(),"Name")]/../../../../../../../..//div[@class="GMMNKTXCJ3 GMMNKTXCPK"]/../div[@class="GMMNKTXCHEC"]//table[@class="GMMNKTXCKDC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL svc-TableRow"] | //span[@class="gwt-InlineHTML" and contains(text(),"Name")]/../../../../../../../..//div[@class="GMMNKTXCJ3 GMMNKTXCPK"]/../div[@class="GMMNKTXCHEC"]//table[@class="GMMNKTXCKDC"]//tr[@class="GMMNKTXCAEC GMMNKTXCHYC GMMNKTXCAL svc-TableRow GMMNKTXCEEC GMMNKTXCKYC"]')
        for la in data:
            sol = {
                        'Name': ''.join(la.xpath('.//td[@cellindex="0"]//text()').extract()),
                        'Type': ''.join(la.xpath('.//td[@cellindex="1"]//text()').extract()),
                        'Description':''.join(la.xpath('.//td[@cellindex="2"]//text()').extract()),
                        'Links':', '.join(la.xpath('.//td[@cellindex="2"]//a[@target="top"]/@href').extract())
                        }
            solutions.append(sol)
        fetch_time =  int(datetime.datetime.now().strftime("%s")) * 1000
        sky_doc = {
                'fetch_time': fetch_time,
                'vendors':vendors,
                'skybox_id' :skybox_id,
                'cve_id' :cve_id,
                'Scanners and Other Sources':  Scanners_and_Other_Sources,
                'severity':security,
                'cvss_base':cvss_base ,
                'cvss_temporal':cvss_temporal,
                'publish_time' : repoarting_date,
                'modified_date':last_modification_date,
                'affected_products': effected_products,
		'solutions': solutions,
                'external_preferences': external_reff,
                'skybox_url': skybox_url,
                'search key_word': serach_key,
                'domain':'vulnerabilitycenter.com'
                }
        sk = hashlib.md5(skybox_url).hexdigest()
        index = 'skybox_posts'
        doc_type = '/post'
        link = 'http://maldoc.tlssec.net/'
        url = link + index + doc_type + '/' + sk
        doc_data = sky_doc
        self.es_connection(url, doc_data)

    def es_connection(self, url, doc_data):
        userandpass = b64encode(b"headrun:@He@drun$").decode("ascii")
        headers = {
                'Authorization': 'Basic %s' %  userandpass,
                'Content-Type': 'application/json'
                }
        query = json.dumps(doc_data)
        response = requests.post(url, data=query, headers=headers)
        print(response)

    def divide_chunks(self,l, n):
        for i in range(0, len(l), n):
            yield l[i:i + n]

if __name__ == '__main__':
    obj = SandBox()
    obj.start_requests()
