import MySQLdb
import datetime
import os, sys
import csv
import logging
import logging.handlers
#from scrapely import Scraper
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import traceback
from datetime import datetime
from elasticsearch import Elasticsearch

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
    es = Elasticsearch(['10.2.0.90:9342'])
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    '''
    try:
        cursor.execute('select distinct(data_links) from clientdemo111.threats_kaspersky')
        db_data = cursor.fetchall()
        connection.commit()
    except Exception as exe:
        process_logger.debug('Exceptions while MySQl updation')
        process_logger.debug(str(traceback.format_exc()))
        pass
    '''

    display, driver = open_driver()
    start_url = 'https://www.f-secure.com/en/web/labs_global/descriptions-index'
    driver.get(start_url)
    time.sleep(5)
    f_secure_az_pagetabs = driver.find_elements_by_xpath('//div[contains(@class,"col-xs-12")]//p//a')
    
    for i in f_secure_az_pagetabs:
        all_datalinks = i.get_attribute('href')
        Title = i.text
        if not Title:
            Title = i.get_attribute('text')
        listing_data = Title
        insert_query = "insert into Microsoft_Title_table(Title ,created_at ,modified_at,crawl_status) values(%s,now(),now(),0) on duplicate key update modified_at = now()"
        cursor.execute(insert_query,(listing_data, ))
    connection.commit()
    close_driver(display, driver)    

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
    connection = MySQLdb.connect(host = 'localhost', user = 'root', passwd = 'qwe123' ,db = 'crawling_sources')#(host='176.9.181.61', user='root', passwd='')
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

