import MySQLdb
import datetime
import os
import re
import sys
import csv
import logging
import logging.handlers
from pyvirtualdisplay import Display
from selenium import webdriver
from elasticsearch import Elasticsearch
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import time
import traceback
from datetime import datetime
from optparse import OptionParser

loggers = {}

def __init__(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])
        self.conn,self.cursor = self.mysql_conn()
        self.count = 0
        self.driver = self.open_driver()
        self.parse()
        self.conn.commit()
        self.conn.close()
        self.close_driver()

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
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    display, driver = open_driver()
    start_url = 'https://www.f-secure.com/en/web/labs_global/descriptions-index'
    driver.get(start_url)

    f_secure_az_pagetabs = driver.find_elements_by_xpath('//div[contains(@class,"col-xs-12")]//p//a')
    for i in f_secure_az_pagetabs:
        all_datalinks = i.get_attribute('href')
        if not '#index-' in  all_datalinks :
            if "descs" in all_datalinks:
                sk = ''.join(re.findall('descs/(.*)',all_datalinks)).replace('.shtml','')
                insert_query = "insert into f_secure(sk,data_links,reference_url,created_at,modified_at,crawl_status) values('%s','%s','%s',now(),now(),0) on duplicate key update modified_at = now()"
                listing_data = (sk,all_datalinks,start_url)
                cursor.execute(insert_query%listing_data)
                connection.commit()
            elif "desc" in all_datalinks:
                sk = ''.join(re.findall('desc/(.*)',all_datalinks)).replace('.shtml','')
                insert_query = "insert into f_secure(sk,data_links,reference_url,created_at,modified_at,crawl_status) values('%s','%s','%s',now(),now(),0) on duplicate key update modified_at = now()"
                listing_data = (sk,all_datalinks,start_url)
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
    start_process()













