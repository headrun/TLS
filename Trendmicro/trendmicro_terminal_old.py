# -*- coding: utf-8 -*-
#import MySQLdb
import datetime
import os, sys
import logging
import calendar
import hashlib
import logging
import logging.handlers
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
    #es = Elasticsearch(['10.2.0.90:9342'])
    #global connection
    #global cursor
    #connection, cursor = open_mysql_connection()
    '''try:
        cursor.execute('select distinct(data_links) from clientdemo111.malware_demo')
        db_data = cursor.fetchall()
        connection.commit()
    except Exception as exe:
        process_logger.debug('Exceptions while MySQl updation')
        process_logger.debug(str(traceback.format_exc()))
        pass

    close_mysql_connection(connection, cursor)'''

    display, driver = open_driver()
    db_data = ['https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/troj_cve20175753.poi','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/troj_dloadr.ausumq','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_accleaker.hbt','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_admdash.hrx','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_chminer.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_contacts.e','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_dendroid.hbt','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_energy.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_ghostctrl.ops','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_htbenews.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_kagecoin.hbt','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_kagecoin.hbtb','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_leakerlocker.hrx','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_locker.hbt','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_msgcrack.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_msgdos.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_obad.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_oneclickfraud.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_opfake.ctd','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_rusms.a','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_slocker.axbp','https://www.trendmicro.com/vinfo/us/threat-encyclopedia/malware/androidos_stealerc32']
    for db_value in db_data:
        driver.get(db_value)
        time.sleep(50)
        page_source = driver.page_source.strip().encode('utf-8')
        ###title
        title_xpath = '//section[@class="articleHeader"]//h1'
        title_extracted = driver.find_element_by_xpath(title_xpath)
        title = title_extracted.text.strip()
        ###naming_convention = title
        name_extracted = title.split('_')[1]
        name_extraction = str(name_extracted.split('.')[0]).strip()
	if name_extraction.startswith('CVE'):
	    continue

        ###threat_type
        threat_type_xpath = '//div[@class="iconDetails"]//ul//li[contains(text(), "Threat")]'
        threat_type_extracted = driver.find_element_by_xpath(threat_type_xpath)
        type_ = threat_type_extracted.text
        if type_:
            type_ = type_.split(':')[1].strip()
	print db_value 
        platform_xpath = '//div[@class="entityHeader"]//strong[contains(text(), "PLATFORM")]/following-sibling::p'
        platform_extracted = driver.find_element_by_xpath(platform_xpath)
        platform = platform_extracted.text.strip()
       
	try: 
	    aliases_xpath = '//div[@class="entityHeader"]//strong[contains(text(), "ALIASES")]/following-sibling::p'
	    aliases_extracted = driver.find_element_by_xpath(aliases_xpath)
	    aliases_ = aliases_extracted.text.strip()
	    aliases = str(aliases_).split(' ; ')
	except:
	    aliases = ''
			
	try:
	    first_seen_xpath = '//span[contains(text(), "FIRST VSAPI PATTERN DATE")]'
	    first_seen_extracted = driver.find_element_by_xpath(first_seen_xpath)
	    date_values = first_seen_extracted.find_elements_by_xpath("..")
	    first_seen_value = date_values[0].text
	    first_seen = str(first_seen_value.split(':')[1].strip())
	except:
	    first_seen = 0

	date_xpath= '//div[@id="datePub"]'
	date_extracted = driver.find_element_by_xpath(date_xpath)
	last_seen = str(date_extracted.text.strip())
	month_ = last_seen.split(' ')[0]
	month = month_[0:3]
	rest = last_seen.split(' ')[1:]
	last_seen_at = ' '.join([month] + rest)

	solution = []
	sol_xp = '//section[@class="accordion"]/div[@class="pane showpane"]//div'
	sol_ex = driver.find_elements_by_xpath(sol_xp)
	for ext in sol_ex:
	    sol = ext.text.strip()
	    solution.append(sol)
	soultion = ''.join(solution)
 	try:
	    tyu = '//section[@class="accordion"]//div[@class="articleHeader"]'
	    driver.find_element_by_xpath(tyu).click()
	except:
	    tyu = '//section[@class="accordion"]//div[@class="articleHeader"]'
            driver.find_element_by_xpath(tyu).click()

	    tech = '//section[@class="accordion"]//div[@id="listDesc"][2]//div'	
	    tech_ex = driver.find_element_by_xpath(tech)
	    techi = tech_ex.find_elements_by_xpath("..")
	    techno = techi[0].text.strip()

	    driver.find_element_by_xpath('//section[@class="accordion"]//div[@class="articleHeader top-articleHeader"]').click()
	    tech_over = '//section[@class="accordion"]//div[@id="listDesc"][1]'
	    overview = driver.find_element_by_xpath(tech_over).text.strip()

	#tist = str(datetime.datetime.now()).split('.')[0]
	tist = calendar.timegm(time.gmtime())
	if first_seen:
	    first_seen_ep = calendar.timegm(time.strptime(first_seen, '%d %b %Y'))
	else:
	    first_seen_ep = 0

	if last_seen:
	    last_seen_ep = calendar.timegm(time.strptime(last_seen_at, '%b %d, %Y'))
        else:
            last_seen_ep = 0
	doc = {'name': name_extraction, 'original_name': title, 'source': 'trendmicro', 'domain': 'trendmicro.com', 'url': db_value, 'description': overview, 'aliases': aliases, 'platform': platform, 'type': type_, 'removal': solution, 'technical_details': techno, 'first_seen': first_seen_ep, 'last_seen': last_seen_ep, 'created_at': tist, 'updated_at': tist}
	import pdb;pdb.set_trace()
	#res = es.index(index="malware_docs", doc_type='malwaredoc', id=hashlib.md5(db_value).hexdigest(), body=doc)
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
        now = datetime.datetime.now()
        handler = logging.FileHandler(path %(name, now.strftime("%Y-%m-%d")))
        formatter = logging.Formatter('%(asctime)s - %(filename)s - %(lineno)d - %(funcName)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        loggers.update(dict(name=logger))
        return logger

process_logger = myLogger('process')

def open_mysql_connection():
    connection = MySQLdb.connect(host='176.9.181.61', user='root', passwd='')
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

