import MySQLdb
import re
import datetime
import os
import sys
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
from datetime import datetime

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
    driver.set_page_load_timeout(121)
    return display, driver

def close_driver(display, driver):
    try:
        display.stop()
        driver.quit()
    except Exception as exe:
        process_logger.debug(str(traceback.format_exc()))
        process_logger.debug("Exception while closing driver.")
        pass

def start_process(crawl_type):
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    display, driver = open_driver()
    a_to_z_listing = 'https://www.symantec.com/security-center/a-z'
    driver.get(a_to_z_listing)
    time.sleep(5)
    a_z_links =  [sa.get_attribute('href') for sa in driver.find_elements_by_xpath('//div[@id="hrefUrl"]//a')]
    display, driver = open_driver()
    start_url = ['https://www.symantec.com/security-center/threats','https://www.symantec.com/security-center/risks',  a_to_z_listing]+a_z_links
    for i in start_url:
        driver.get(i)
	#driver_data(driver, connection, cursor, i, crawl_type)
        #if (crawl_type != 'keepup') and (crawl_type == 'catchup'):
        try:
            next_page = driver.find_element_by_xpath('//a[@class="paginate_button next"]')
            while next_page:
                next_check(next_page, driver)
                next_page = driver.find_element_by_xpath('//a[@class="paginate_button next"]')
                driver_data(driver, connection, cursor, i, crawl_type)
        except:
            driver_data(driver, connection, cursor, i, crawl_type)
    close_driver(display, driver)
    close_mysql_connection(connection, cursor)

def driver_data(driver, connection, cursor, surl, crawl_type):
        all_datalinks = driver.find_elements_by_xpath('//tbody//tr//td/a')
        for data_links in all_datalinks:
                url = data_links.get_attribute('href')
                sk = ''.join(re.findall('writeup.html/(.*)',url))
		insert_query = "insert into symantec_browse(sk,url,reference_url,crawl_type,created_at,modified_at,crawl_status) values('%s','%s','%s','%s',now(),now(),0) on duplicate key update modified_at = now()"
        	listing_data = (sk ,url, surl, crawl_type)
		print listing_data
        	cursor.execute(insert_query%listing_data)
        	connection.commit()
def next_check(next_page, driver):
	next_page.click()
	return driver


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
        now = datetime.now()
        handler = logging.FileHandler(path %(name, now.strftime("%Y-%m-%d")))
        formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(funcName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        loggers.update(dict(name=logger))
        return logger



process_logger = myLogger('process')
def open_mysql_connection():
    connection = MySQLdb.connect(host='localhost', user='root', passwd='qwe123',db = 'crawling_sources')
    connection.set_character_set('utf8')
    cursor = connection.cursor()

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
    
    parser = OptionParser()
    parser.add_option('-a', '--crawl_type', default='', help='Enter keepup/Enter catchup')

    (options, args) = parser.parse_args()
    crawl_type = options.crawl_type.strip()
    result = start_process(crawl_type)
    

