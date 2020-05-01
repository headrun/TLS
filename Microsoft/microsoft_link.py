import MySQLdb
import hashlib
import datetime
import calendar
import os, sys, csv, re
import logging
import logging.handlers
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
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
ref_links = []

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
    '''service_args = ['--verbose']
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument('--window-size=1420,1080')
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    driver = webdriver.Chrome('/usr/local/bin/chromedriver', chrome_options=options, service_args=service_args)'''
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
    description_link = 'https://www.microsoft.com/en-us/wdsi/threats/malware-encyclopedia-description?Name='
    connection, cursor = open_mysql_connection()
    try:
        cursor.execute('select distinct(url) from crawling_sources.Microsoft_terminal_table where crawl_status = 0 limit 100')
        db_data = cursor.fetchall()
        connection.commit()
    except Exception as exe:
        process_logger.debug('Exceptions while MySQl updation')
        process_logger.debug(str(traceback.format_exc()))
        pass
    for links in db_data:
        link = links[0]
        #if description_link in link :
        ig_list = ['https://www.microsoft.com/en-us/wdsi/threats/malware-encyclopedia-description?Name=TrojanDownloader:Win32/Renos.JW','https://www.microsoft.com/en-us/wdsi/threats/malware-encyclopedia-description?Name=Worm:Win32/Mywife.E@mm!CME24.dam#2', 'https://www.microsoft.com/en-us/wdsi/threats/malware-encyclopedia-description?Name=Worm:Win32/Slenfbot.YJ',]
        if (link in ig_list) or ('Virus:' in link):
            upd_qry = 'update crawling_sources.Microsoft_terminal_table set crawl_status = 1 where url = "%s"'
            values = link
            cursor.execute(upd_qry%values)
            connection.commit()
            continue
        display, driver = open_driver()
        driver.get(link)
        time.sleep(4)
        print link
        try:
            aliases = []	
	    aliases_paths = driver.find_elements_by_xpath('//p[@id="alias"]//span')
            for aliases_path in aliases_paths[2:]:
	        aliases_text = aliases_path.text
	        aliases.append(aliases_text)
        except:
	    aliases = ""
        try:
            title = driver.find_element_by_xpath('//div[@class="margin-both"]/div/h1')
            title_text = title.text
        except:
            title_text = ""
        malware_name = ''.join(title_text).split('/')[-1]
        name_extraction = malware_name.split('.')[0]
        try: 
            firstseen=driver.find_element_by_xpath('//div/span[@id="publishDate"]')
            firstseen_text=firstseen.text
            firstseen_exact = str(''.join(firstseen_text).split('Published')[-1].strip())
        except:
            firstseen_exact = ""
        try:
            lastseen = driver.find_element_by_xpath('//div/span[@id="updatedDate"]/span/span')
            Lastseen = str(lastseen.text.strip())
        except:
            Lastseen = ""
        try:
            summary_text = ""
            summary = driver.find_elements_by_xpath('//div[@class="summaryText"]')
            for sample in summary :
                summary_text = sample.text
        except:
            summary_text = ""
        try:
            to_do_text = ''
            driver.find_element_by_xpath('//div[@id="whattodoDiv"]//button').click()
	    to_do = driver.find_elements_by_xpath('//div[@id="whattodoDiv"]')
	    for simple in to_do :
	        to_do_text = simple.text
        except:
            to_do_text = ''
        try:
            technical_text = ''
	    driver.find_element_by_xpath('//div[@id="technicalDiv"]//button').click()
	    technical=driver.find_elements_by_xpath('//div[@id="technicalDiv"]')
            for tech_sample in technical :
                technical_text = tech_sample.text
        except:
	    technical_text = ''

        try:
            symptoms_text = ''
	    driver.find_element_by_xpath('//div[@id="symptomsDiv"]//button').click()
	    symptoms=driver.find_elements_by_xpath('//div[@id="symptomsDiv"]')
            for symp_sample in symptoms :
                symptoms_text = symp_sample.text
        except:
	    symptoms_text = ''
        techno = technical_text + symptoms_text
        tist = calendar.timegm(time.gmtime())* 1000
        if firstseen_exact:
            first_seen = calendar.timegm(time.strptime(firstseen_exact, '%b %d, %Y'))* 1000
	            
        else:
            first_seen = None
        if Lastseen:
            last_seen = calendar.timegm(time.strptime(Lastseen, '%b %d, %Y'))* 1000
        else:
	    last_seen = None
        if first_seen == None:
            first_seen = last_seen
        if last_seen == None:
	    last_seen = first_seen
        #print link
        doc ={'name': name_extraction, 'original_name': title_text, 'source': 'microsoft', 'domain': 'microsoft.com','url': link, 'description': summary_text, 'removal': to_do_text, 'technical_details': techno, 'first_seen': first_seen, 'last_seen': last_seen, 'created_at': tist, 'updated_at': tist, 'aliases': aliases}
        pprint(doc)
	sk = hashlib.md5(link).hexdigest()
        es.index(index="malware_docs", doc_type='malwaredoc', id=sk, body=doc)
        upd_qry = 'update crawling_sources.Microsoft_terminal_table set crawl_status = 1 where url = "%s"'
        values = link
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
    path = 'logs/microsoft_%s_%s.log'

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
    connection = MySQLdb.connect(host='localhost', user='root', passwd='qwe123')
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
