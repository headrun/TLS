import MySQLdb
import hashlib
import datetime
import calendar
import os, sys, csv, re
import logging
import logging.handlers
sys.path.append('/root/madhav/madhav/spiders/scrapely-master')
#from scrapely import Scraper
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from scrapy.selector import Selector
import time
import traceback
from elasticsearch import Elasticsearch

from optparse import OptionParser
from pprint import pprint
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from tbselenium.tbdriver import TorBrowserDriver
from tbselenium.utils import start_xvfb, stop_xvfb
from base64 import b64encode 

loggers = {}

def open_driver():
    display = Display(visible=0, size=(800,600))
    display.start()
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument('--headless')
    options.add_argument('--proxy-server={}'.format('socks5://localhost:9050'))#'http://64.17.30.238:63141'))
    options.add_argument("--incognito")
    driver = webdriver.Chrome(chrome_options=options)
    return display, driver

def close_driver(display, driver):
    try:
        display.stop()
        driver.quit()
    except Exception as exe:
        process_logger.debug(str(traceback.format_exc()))
        process_logger.debug("Exception while closing driver.")
        pass
def start_process():
    es = Elasticsearch(['10.2.0.90:9342'])
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    cursor.execute('select distinct(data_links) from virusradar_updated_browse where crawl_status = 0  limit 10')
    data = cursor.fetchall()
    connection.commit()
    for dbv in data:
        db = dbv[0]
        if 'detail' in db:
            continue
        display, driver = open_driver()
        driver.get(db)
        time.sleep(2)
	for click in driver.find_elements_by_xpath('//div[@class="virus-about-down"]//a[contains(text(),"more info")]'):
           try:
               click.click()
               time.sleep(1)
           except:pass	
        page_source = driver.page_source.strip().encode('utf-8')
	date_xpath = '//div[@id="vtab-40"] | //div[@class="vtab-30"]'
        date_extracted = driver.find_element_by_xpath(date_xpath)
	try:
	    category = date_extracted.text.split('Category')[1].split('\n')[0].strip()
	except:
	    category = ''
	naming_convention_xpath = '//h2'
	naming_extracted = driver.find_element_by_xpath(naming_convention_xpath)
	naming = naming_extracted.text
	naming_ = naming.split('[')[0]
	naming_convention = naming_.split('.')[0]   ##platform/name

	original_name = naming_.strip()

	name_extr = naming_.split('/')[1].split('.')
	if len(name_extr) == 2:
	    name_extraction = name_extr[0]

	elif len(name_extr) == 3:
	    name_extraction = name_extr[1]
	else:
	    name_extraction = name_extr[0]

	aliases = []
	try:
	    aliases_xpath = '//div[@id="vtab-40"]'
	    aliases_extracted = driver.find_element_by_xpath(aliases_xpath)
	    alias = aliases_extracted.text.split('Aliases')[1].split('\n')
	    for ali in alias:
		ali = ali.strip()
		aliases.append(str(ali))

	except:
	    aliases = []
	desc_path = '//div[@id="virus-about"]' 
	desc_ext = driver.find_element_by_xpath(desc_path)
	desc = desc_ext.text
	description = desc.split('Installation')[0].strip('Short description\n')
	try:
	    Other_Info = desc.split('Other information')[1]
	except:
	    Other_Info = ''
        try:
	    Technical_ = desc.split('Installation')[1]
            if Technical_:
	        Technical = Technical_.split('Other information')[0]
            else:
 	        Technical = Technical_
        except:
            Technical = 'None'
	Target_dict = []
	country_url = '//div[@id="add-menu-10-c"]//li[3]//a'
        country_url_extracted = driver.find_element_by_xpath(country_url)
        name_url = country_url_extracted.get_attribute('href')
	country_url = name_url + '/month'
	driver.get(country_url)
	page = driver.page_source
	countries = re.findall("\country\=\'?.*';", page)
	for country in countries:
 	    if not 'No Data' in country:
		country_info = country.split(';')[0].split('=')[1].strip("'")
		country_value = country.split(';')[1].split('=')[1].strip("'")
                if 'All Countries' in country_info:
		    continue
		Target_dict.append(country_info)
	    else:
		continue
        sel = Selector(text=driver.page_source)
        try:
            data = ''.join(sel.xpath('//div[@id="add-menu-10-c"]//ul//li//a//@href').extract()[0])
          
            crea = driver.get("https://www.virusradar.com" + data)
            created = driver.find_element_by_xpath('//div[@class="vtab-30"]')
            created_ = created.text
            created__ = str(created_.split('Detection created')[1].split('\n')[0].strip())
            last = str(created_.split('World activity peak')[1].split('\n')[0].strip())
        except:
            created = ''
            last = ''
	tist = str(datetime.datetime.now()).split('.')[0]
	tist = calendar.timegm(time.gmtime())* 1000
	if created__:
	    first_seen = calendar.timegm(time.strptime(created__, '%Y-%m-%d'))* 1000
	else:
            first_seen = ''
	doc ={'name': name_extraction, 'original_name': original_name, 'source': 'virusradar', 'domain': 'virusradar.com','url': db, 'description': description, 'aliases': aliases, 'type': category, 'removal': Other_Info, 'technical_details': Technical, 'target_countries': Target_dict, 'first_seen': first_seen, 'created_at': tist, 'updated_at': tist,}
	sk = hashlib.md5(doc['url']).hexdigest()
        es.index(index="malware_docs", doc_type='malwaredoc', id=sk, body=doc)
        res = es.index(index="malware_docs", doc_type='malwaredoc', id=hashlib.md5(db).hexdigest(), body=doc)
        upd_qry = 'update crawling_sources.virusradar_updated_browse set crawl_status = 9 where data_links = "%s"'
        values = db
        cursor.execute(upd_qry%values)
        connection.commit()
        close_driver(display, driver)
    close_mysql_connection(connection, cursor)

def myLogger(name):
    log_path = os.path.abspath('logs/')
    try:
        os.mkdir(log_path)
    except:
        pass

    dom_files_path =  os.path.abspath('dom_files/')
    try:
        os.mkdir(dom_files_path)
    except:
        pass

    global loggers
    path = 'logs/vradar_%s_%s.log'

    if loggers.get(name):
        return loggers.get(name)
    else:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)
        now = datetime.datetime.now()
        handler = logging.FileHandler(path %(name, now.strftime("%Y-%m-%d")))
        formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(funcName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        loggers.update(dict(name=logger))
        return logger

process_logger = myLogger('process')

def open_mysql_connection():
    connection = MySQLdb.connect(host='localhost', user='root', passwd='qwe123', db = 'crawling_sources')
    connection.set_character_set('utf8')
    cursor = connection.cursor()
    process_logger.debug("MySQL connection established.")
    return connection, cursor

def close_mysql_connection(connection, cursor):
    try:
        cursor.close()
        connection.close()
    except Exception as exe:
        process_logger.debug(str(traceback.format_exc()))
        process_logger.debug("Exception while closing driver.")
        pass

if __name__ =="__main__":
    start_process()
