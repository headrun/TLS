import MySQLdb
import datetime
import urllib
import os, sys
import csv
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
from elasticsearch import Elasticsearch

from optparse import OptionParser

loggers = {}
ref_links = []

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
    es = Elasticsearch(['10.2.0.90:9342'])
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    try:
        cursor.execute('select distinct(Title) from crawling_sources.Microsoft_Title_table')
        db_data = cursor.fetchall()
        connection.commit()
    except Exception as exe:
        process_logger.debug('Exceptions while MySQl updation')
        process_logger.debug(str(traceback.format_exc()))
        pass
    close_mysql_connection(connection, cursor)
    connection, cursor = open_mysql_connection()
    for title in db_data:
        #print title
        update_query  = 'update crawling_sources.Microsoft_Title_table set crawl_status=1 where title = "%s"'
        values = title[0]
        cursor.execute(update_query%values)
        connection.commit()

    close_mysql_connection(connection, cursor)
    
    
    #db_data = [ ('win32/gator',)]
    query = 'https://www.microsoft.com/en-us/wdsi/threats/threat-search?query='
    for title in db_data:
        if ':' in title:
            malware = title.split(':')[1].strip()
            mname = malware.split('/')[1]
            try:
                mn = mname.split('.')[0].strip()
            except:
                mn = mname
        elif '.' in title:
             malware = title.split('.')[1:]
             malware = '.'.join(malware).strip()
             mn = malware
        else:
            m_name = title
            for malware in m_name:
                mn = malware
        titl = urllib.quote(mn)
	if crawl_type == 'Keep_up':
	    malware_query = query + titl + '&sortby=date'
        else:
            malware_query = query + titl
           
        all_links = get_links(malware_query, crawl_type)
	#print all_links
        
def get_links(link, crawl_type):
    display, driver = open_driver()
    old_link = link
    driver.get(link)
    #sub_links = driver.find_elements_by_xpath('//div[@class="NormalResult pad-40"]//div[@id="threatLink"]//a')
    sub_links = driver.find_elements_by_xpath('//h2[@id="threatLink"]//a')
    for sub_link in sub_links :
        data(sub_link)
    
    if crawl_type == 'Keep_up':
        print "Done keepup for %s"%link
    else:
        try:
            #driver.find_element_by_xpath('//div[@id="paging"]//ul[@class="m-pagination"]//li[@id="pgn"]//a[@class="c-glyph"]').click()
  	    next_page = driver.find_element_by_xpath('//div[@id="paging"]//ul[@class="m-pagination"]//li[@id="pgn"]//a[@class="c-glyph"]')
            new_url = next_page.get_attribute('href')
            if old_link != new_url:
                close_driver(display, driver)
                get_links(new_url,crawl_type)
            else:
                close_driver(display, driver)
                #print 'Page Finished %s' %link
        except:
            close_driver(display, driver)
            #print 'Page Finished %s' %link

def data(ref_links):
    connection, cursor = open_mysql_connection()
    try:
        asd = ref_links.get_attribute('href')
    except:
        asd = ''
    all_datalinks = asd
    #print all_datalinks
    insert_query = "insert into Microsoft_terminal_table(url, crawl_status, created_at, modified_at, crawl_type) values(%s, 0, now(), now(), %s) on duplicate key update modified_at = now()"
    listing_data = (all_datalinks,crawl_type)
    cursor.execute(insert_query, listing_data)
    connection.commit()
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
    parser = OptionParser()
    parser.add_option('-a', '--crawl_type', default='', help='Enter Keep_up/Enter Catch_up')
    ###python microsoft_terminal.py -a 'Catch_up'

    (options, args) = parser.parse_args()
    crawl_type = options.crawl_type.strip()
    start_process(crawl_type)
    
