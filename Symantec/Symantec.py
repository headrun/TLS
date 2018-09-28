import MySQLdb
import datetime
import os, sys
import csv
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
import hashlib
import calendar
from datetime import datetime
from elasticsearch import Elasticsearch
from selenium.common.exceptions import NoSuchElementException
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
  
def check_name(Naming, Type, year):
    if Type != '': 
        if ('Exp.CVE' in Naming) or ('ER.Heur!gen1' in Naming) or ('SONAR.PUA!gen10' in Naming) or ('Adware.Gen.3' in Naming) or ('PUA' in Naming) or ('SecurityRisk.Capsferv' in Naming) or ('Remacc.WinPCGuard' in Naming) or ('SecurityRisk.Gen2' in Naming) or ('!' in Naming):
            return False
        else:
            return True
    else:
        if ('Exp.CVE' in Naming) or ('ER.Heur!gen1' in Naming) or ('SONAR.PUA!gen10' in Naming) or ('Adware.Gen.3' in Naming) or ('PUA' in Naming) or ('SecurityRisk.Capsferv' in Naming) or ('Remacc.WinPCGuard' in Naming) or ('SecurityRisk.Gen2' in Naming) or ('!' in Naming):
            return False
        else:
            return True


def start_process():
    es = Elasticsearch(['10.2.0.90:9342'])
    global connection
    global cursor
    connection, cursor = open_mysql_connection()
    try:
        cursor.execute('select distinct(data_links) from crawling_sources.symantec_browse where crawl_status= 0 limit 50')
        db_data = cursor.fetchall()
        connection.commit()
    except Exception as exe:
        process_logger.debug('Exceptions while MySQl updation')
        process_logger.debug(str(traceback.format_exc()))
        pass

    close_mysql_connection(connection, cursor)
    ###20links for threats,risks,A-Z
    #total_links = ['https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072706-5557-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072707-0303-99', 'https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072612-0411-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072612-0909-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072613-2108-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072613-4236-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072613-4828-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072611-5724-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072505-5631-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072507-1551-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072505-3443-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072505-3904-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072402-3858-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072406-4226-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072013-2255-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-072005-5736-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-071907-5826-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-071908-3523-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-071914-3513-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-071914-4132-99']


    
    for currents in db_data :
        connection, cursor = open_mysql_connection()
        current = currents[0]
        ig_list = ['https://www.symantec.com/security-center/writeup/2018-052114-1525-99', 'https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-052114-1525-99','https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-051413-5628-99', 'https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-051514-0447-99', 'https://www.symantec.com/content/symantec/english/en/security-center/writeup.html/2018-090315-3529-99',]
        if current in ig_list:
            upd_qry = 'update crawling_sources.symantec_browse set crawl_status= 1 where data_links = "%s"'
            values = current
            cursor.execute(upd_qry%values)
            connection.commit()
            continue
        display, driver = open_driver()
        driver.get(current)
	time.sleep(3)
	print current
        if "vulnerabilities" in driver.current_url:
            try :
                naming = driver.find_element_by_xpath('//div[@class="content"]/div[@class="text-content"]/h1')
                Naming = str(naming.text)
                firstseen = driver.find_element_by_xpath('//div[@class="content"]/h3[contains(text(),"Date Discovered")]/following-sibling::p[1]')
                Firstseen = firstseen.text
                month_ = Firstseen.split(' ')[0]
                month = month_[0:3]
                rest = Firstseen.split(' ')[1:]
                first_seen_at = ' '.join([month] + rest)
                first_seen_ep = calendar.timegm(time.strptime(first_seen_at, '%b %d, %Y'))* 1000
                year = Firstseen.split(',')[-1]
                Type = ''
            except :
                continue

            if check_name(Naming,Type,int(year)):
                print current
                tist = calendar.timegm(time.gmtime())* 1000
                description = driver.find_elements_by_xpath('//div[@class="content"]/h3[contains(text(),"Description")]/following-sibling::p[1]')
                for sample in description :
	            Description = sample.text
		doc ={'original_name': Naming, 'name': Naming,'source': 'symantec', 'domain': 'symantec.com','url': current, 'description': Description, 'created_at': tist, 'updated_at': tist, 'first_seen': first_seen_ep, 'last_seen': first_seen_ep,}	
                res = es.index(index="malware_docs", doc_type='malwaredoc', id=hashlib.md5(current).hexdigest(), body=doc)
	    close_driver(display, driver)           
        else:
	    naming = driver.find_element_by_xpath('//div[@class="text-content"]//h1')
	    Naming = str(naming.text)
	    try:
		type_xps = driver.find_elements_by_xpath('//div[@name="panel-summary"]//p//strong[contains(text(), "Type")]')
                if type_xps == []:
                    type_ = ''
		for type_xp in type_xps:
		    type_datas = type_xp.find_element_by_xpath('..').text
		    type_ = str(type_datas.split('Type:')[1].split('\n')[0].strip())
	    except:
		type_ = ''

	    try:
                aka_xps = driver.find_elements_by_xpath('//div[@name="panel-summary"]//p//strong[contains(text(), "Also Known As")]')
                for aka_xp in aka_xps:
                    aka_datas = aka_xp.find_element_by_xpath('..').text
                    aka = str(aka_datas.split('Also Known As:')[1].split('\n')[0].strip())
            except:
                aka = ''

	    try:
		first_xps = driver.find_elements_by_xpath('//div[@name="panel-summary"]//p//strong[contains(text(), "Discovered")]')
		if first_xps == []:
                    first_seen_at = ''
                    first_seen_ep = None
                for first_xp in first_xps:
                    first_datas = first_xp.find_element_by_xpath('..').text
                    first_seen = str(first_datas.split('Discovered:')[1].split('\n')[0].strip())
		    month_ = first_seen.split(' ')[0]
                    month = month_[0:3]
                    rest = first_seen.split(' ')[1:]
                    first_seen_at = ' '.join([month] + rest)
		    first_seen_ep = calendar.timegm(time.strptime(first_seen_at, '%b %d, %Y'))* 1000
            except:
                first_seen_at = ''
                first_seen_ep = None 

	    try:
                Upd_xps = driver.find_elements_by_xpath('//div[@name="panel-summary"]//p//strong[contains(text(), "Updated")]')
                if Upd_xps == []:
                    last_seen_ep = None
                
                for Upd_xp in Upd_xps:
                    Upd_datas = Upd_xp.find_element_by_xpath('..').text
                    last_seen = str(Upd_datas.split('Updated:')[1].split('\n')[0].strip())
		    month_ = last_seen.split(' ')[0]
                    month = month_[0:3]
                    rest = last_seen.split(' ')[1:]
                    if 'PM' in rest:
			time_ = rest[2].split(':')
			hour = int(time_[0]) + 12
			ptime = [':'.join([str(hour)] + time_[1:])]
			
			rest_ = rest[:2]
			last_seen_at = ' '.join([month] + rest_ + ptime)
		    else:
			rest_ = rest[:-1]
			last_seen_at = ' '.join([month] + rest_)
		    last_seen_ep = calendar.timegm(time.strptime(last_seen_at, '%b %d, %Y %H:%M:%S'))* 1000 
            except:
                last_seen_ep = None
	    try:
                pl_xps = driver.find_elements_by_xpath('//div[@name="panel-summary"]//p//strong[contains(text(), "Systems Affected")]')
                if pl_xps == []:
                    platform = ''
                for pl_xp in pl_xps:
                    pl_datas = pl_xp.find_element_by_xpath('..').text
                    platform = str(pl_datas.split('Systems Affected:')[1].split('\n')[0].strip())
            except:
                platform = ''
	    try:
	        year = first_seen_at.split(',')[1].strip()
	    except:
		year = 0
	    if check_name(Naming, type_, int(year)):
		extracted = Naming.split('.')
		if len(extracted) > 1:
		    extracted_name = extracted[1]
		else:
		    extracted_name = extracted[0] 
		summary_text = ''
		summary = driver.find_element_by_xpath('//div[@class="content"]//div[@id="summary"]//p[2]')
		summary_text = summary.text
		close_driver(display,driver)

		display, driver = open_driver()
		tech_link = current + "#technicaldescription"
		driver.get(tech_link)
		time.sleep(3)
                technical_details = ""
		tech_details = driver.find_elements_by_xpath('//div[@class="content"]//div[@id="technicaldescription"]')
		for sample in tech_details :
		    technical_details =sample.text
		close_driver(display, driver)
    
		display, driver = open_driver()
		removal_link = current + "#removal"
		driver.get(removal_link)
		time.sleep(3)
                removal_text = ''
		removal = driver.find_elements_by_xpath('//div[@class="content"]//div[@id="removal"]')
		for sample in removal :
		    removal_text = sample.text
		close_driver(display, driver)

		tist = calendar.timegm(time.gmtime())* 1000
		if first_seen_ep == None:
		    first_seen_ep = last_seen_ep
		if last_seen_ep == None:
		    last_seen_ep = first_seen_ep


                doc ={'original_name': Naming, 'name': extracted_name,'source': 'symantec', 'domain': 'symantec.com','url': current, 'description': summary_text, 'type': type_, 'platform': platform, 'removal': removal_text, 'technical_details': technical_details, 'created_at': tist, 'updated_at': tist, 'first_seen': first_seen_ep, 'last_seen': last_seen_ep,}
                ###Elastic Search
                res = es.index(index="malware_docs", doc_type='malwaredoc', id=hashlib.md5(current).hexdigest(), body=doc)
                upd_qry = 'update crawling_sources.symantec_browse set crawl_status= 1 where data_links = "%s"'
                values = current
                cursor.execute(upd_qry%values)
                connection.commit()
		close_driver(display, driver)
                connection, cursor = open_mysql_connection()
	    else:
                upd_qry = 'update crawling_sources.symantec_browse set crawl_status= 1 where data_links = "%s"'
                values = current
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

