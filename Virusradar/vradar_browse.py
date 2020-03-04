import MySQLdb
import datetime
import os, sys
import csv
import re
import scrapy
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
start_url = 'http://www.virusradar.com/en/threat_encyclopaedia/filter'

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

def start_process(crawl_type):
    global connection
    global cursor
    #display, driver = open_driver()
    connection, cursor = open_mysql_connection()

    start_url = 'http://www.virusradar.com/en/threat_encyclopaedia/filter'
    alpha_extract(start_url, crawl_type) #crawl_type

def alpha_extract(url, crawl_type): 
    display, driver = open_driver()
    driver.get(url)
    alphabets_links = driver.find_elements_by_xpath('//div[@class="add-menu-20-c-a1"]//a')
    for all_alphabets_links in  alphabets_links:
        alpha_links = all_alphabets_links.get_attribute('href')
        page_extract(alpha_links, crawl_type)

def page_extract(url, crawl_type): #crawl_type
    display, driver = open_driver()
    driver.get(url)
    time.sleep(5)
    page_url = url.split('?page')[0]

    max_pages_numbers = driver.find_elements_by_xpath('//ul[@id="pager"]//li//a')
    if max_pages_numbers:
        max_pages_number =  max_pages_numbers[-1].get_attribute('href')
        if max_pages_number:
            max_pages =  int(max_pages_number.split('=')[-1]) + 1
            lastpage_number = max_pages - 1
            if crawl_type == 'keepup':
                next_page_url = page_url + '?page=' + str(lastpage_number)
                page_next_extract(next_page_url, crawl_type)
            else:
                crawl_type = 'catchup'
                page_next_extract(url, crawl_type)
                for i in range(int(max_pages)):
                    if i >= 1:
                        next_page_url = page_url + '?page=' + str(i)
                        page_next_extract(next_page_url, crawl_type)

    close_driver(display, driver)
    close_mysql_connection(connection, cursor)

def page_next_extract(url,crawl_type):
    connection, cursor = open_mysql_connection()
    display, driver = open_driver()
    driver.get(url)
    time.sleep(15)
    virus_links = driver.find_elements_by_xpath('//td//a')
    for data_links in virus_links:
        url = data_links.get_attribute('href')
        sk = ''.join(re.findall('en/(.*)',url)).replace('/description','')
        if '/detail' in url: continue
        insert_query = "insert into virusradar_updated_browse(sk,data_links,reference_url,crawl_type,created_at,modified_at,crawl_status) values('%s','%s','%s','%s',now(),now(),0) on duplicate key update modified_at = now()"
        listing_data = (sk,url, start_url,crawl_type)
        print insert_query%listing_data
        cursor.execute(insert_query%listing_data)
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
    connection = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'qwe123',db = 'crawling_sources')
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
    parser = OptionParser()
    parser.add_option('-a', '--crawl_type', default='', help='Enter keepup/Enter catchup')
    (options, args) = parser.parse_args()
    crawl_type = options.crawl_type.strip()
    result = start_process(crawl_type)
    











