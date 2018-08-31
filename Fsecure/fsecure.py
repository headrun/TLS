import MySQLdb
import datetime
import os, sys, csv
import logging
import logging.handlers
import hashlib
import calendar
sys.path.append('/home/epictions/madhav/test/scrapely-master')
#sys.path.append('/root/madhav/madhav/spiders/scrapely-master')
#from scrapely import Scraper
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import traceback
#from elasticsearch import Elasticsearch

from optparse import OptionParser

loggers = {}

def open_driver():
    display = Display(visible=0, size=(800,600))
    display.start()
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument('--headless')
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
    '''
    es = Elasticsearch(['10.2.0.90:9342'])
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    scraper = Scraper()
    sel_query = 'select distinct(data_links) from crawling_sources.f_secure where crawl_status = 0 limit 100' 
    cursor.execute(sel_query)
    db_data = cursor.fetchall()
    close_mysql_connection(connection, cursor)
    ### Training for fsecure site
    train_url = 'https://www.f-secure.com/v-descs/trojan_w32_ursnif.shtml'
    train_data = {'category': 'Malware', 'platform': 'W32', 'Type': 'Trojan', 'aliases': 'Ursnif, Trojan:W32/Ursnif, Trojan.Spy.Ursnif, Trojan.GenericKD.30550163, Gozi, ISFB'}
    scraper.train(train_url, train_data)
    '''
    db_data = ['https://www.f-secure.com/sw-desc/adbar.shtml','https://www.f-secure.com/sw-desc/123search.shtml']
    #db_data = ['https://www.f-secure.com/sw-desc/adbar.shtml','https://www.f-secure.com/sw-desc/123search.shtml','https://www.f-secure.com/sw-desc/180solutions.shtml','https://www.f-secure.com/sw-desc/2020search.shtml','https://www.f-secure.com/sw-desc/404search.shtml','https://www.f-secure.com/sw-desc/7adpower.shtml','https://www.f-secure.com/sw-desc/7fasst.shtml','https://www.f-secure.com/sw-desc/7search-browseraccelerator.shtml','https://www.f-secure.com/sw-desc/abetterinternet_aurora.shtml','https://www.f-secure.com/sw-desc/abox.shtml','https://www.f-secure.com/sw-desc/ab_system_spy.shtml','https://www.f-secure.com/sw-desc/acallno_a.shtml','https://www.f-secure.com/sw-desc/acsoftware_narod.shtml','https://www.f-secure.com/sw-desc/activshopper.shtml','https://www.f-secure.com/sw-desc/actualnames.shtml','https://www.f-secure.com/sw-desc/ad-popper.shtml','https://www.f-secure.com/sw-desc/adbar.shtml','https://www.f-secure.com/sw-desc/adblaster.shtml','https://www.f-secure.com/sw-desc/adbreak.shtml','https://www.f-secure.com/sw-desc/addestroyer.shtml','https://www.f-secure.com/sw-desc/adgoblin.shtml']
    for db in db_data:
        display, driver = open_driver()
        driver.get(db)
        import pdb;pdb.set_trace()
	#time.sleep(5)
        try:
            category_given = driver.find_element_by_xpath('//p[@class="font-gray-7 m-y-0"]//span[@class="desc-category"]')
            category_name = category_given.text
            if category_name:
                category = category_name.strip()
            else:
                category = None
        except:
            category_given = None
        try:
            platform_given = driver.find_element_by_xpath('//p[@class="font-gray-7 m-y-0"]//span[@class="desc-platforms"]')
            platform_name = platform_given.text
            if platform_name:
                platform = platform_name.strip()
            else:
                platform = None
        except:
            platform = None
        try:
            type_given = driver.find_element_by_xpath('//p[@class="font-gray-7 m-y-0"]//span[@class="desc-type"]')
            type_name = type_given.text
            if type_name:
                type_ = type_name.strip()
            else:
                type_ = None
        except:
            type_ = None
        try:
            aliases_given = driver.find_element_by_xpath('//p[@class="font-gray-7 p-b-1"]//span[@class="desc-aliases"]')
            aliases_name = aliases_given.text
            if aliases_name:
                aliases = aliases_name.strip()
            else:
                aliases = None
        except:
            aliases = None  

        title = driver.find_elements_by_xpath('//div[@class="col-xs-12 p-t-4 p-b-2"]/h1/span[@class="desc-name"]')
        title_text = title.text

        try:
	        first_seen_path = driver.find_element_by_xpath('//p[@class="m-b-0 font-gray-7"]//span[contains(text(), "Description Created")]/following-sibling::span')
            first_seen_text = first_seen_path.text
	        created = str(first_seen_text.split('.')[0])
	        first_seen = calendar.timegm(time.strptime(created, '%Y-%m-%d %H:%M:%S'))* 1000 
        except:
	        first_seen = None

	    try:
            summary = driver.find_element_by_xpath('//div[@class="desc-summary"]')
            summary_text = summary.text
	    except:
            summary_text = ''

	    try:
            removal = driver.find_element_by_xpath('//div[@class="desc-disinfection"]')
            removal_text = removal.text
  	    except:
	        removal_text = ''

        try:
            technical = driver.find_element_by_xpath('//div[@class="desc-technicaldetails"]')
            technical_text = technical.text
	    except:
            technical_text = ''
            
            tist = calendar.timegm(time.gmtime())* 1000
            #doc ={'original_name': title_text, 'source': 'fsecure', 'domain': 'f-secure.com','url': db, 'description': summary_text, 'aliases': aliases, 'type': type_, 'platform': platform, 'removal': removal_text, 'technical_details': technical_text, 'created_at': tist, 'updated_at': tist, 'first_seen': first_seen, 'last_seen': first_seen}
            ###Elastic Search
	    #print db
            '''
            res = es.index(index="malware_docs", doc_type='malwaredoc', id=hashlib.md5(db).hexdigest(), body=doc)
            connection, cursor = open_mysql_connection()
            upd_qry = 'update crawling_sources.f_secure set crawl_status = 1 where data_links = "%s"'
            values = db
            cursor.execute(upd_qry%values)
            connection.commit()
            close_driver(display, driver)    
            close_mysql_connection(connection, cursor)
            '''

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
    path = 'logs/kaspersky_%s_%s.log'

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
    connection = MySQLdb.connect(host='localhost', user='root', passwd='')
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
