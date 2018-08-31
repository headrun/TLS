import MySQLdb
import datetime
import os, sys, csv, re
import logging
import logging.handlers
sys.path.append('/root/madhav/madhav/spiders/scrapely-master')
from scrapely import Scraper
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
    global connection
    global cursor
    filename = "vradar_%s.csv" %(str(datetime.datetime.now().date()))
    oupf = open(filename, 'ab+')
    csv_file = csv.writer(oupf)
    fields = ["URL", "Naming Convention", "Original Name", "Naming Extraction", "Type", "Aliases", "First Seen", "Last Seen", "Description", "Removal", "Technical Details", 'Target_countries']
    csv_file.writerow(fields)
    '''connection, cursor = open_mysql_connection()
    try:
        cursor.execute('select distinct(data_links) from clientdemo111.virus_table')
        db_data = cursor.fetchall()
        connection.commit()
    except Exception as exe:
        process_logger.debug('Exceptions while MySQl updation')
        process_logger.debug(str(traceback.format_exc()))
        pass

    close_mysql_connection(connection, cursor)'''

    display, driver = open_driver()

    ### Training for Vradar site
    db_data = ['http://www.virusradar.com/en/Win32_Delf.NBJ/description','http://www.virusradar.com/en/MSIL_Antinny.A/description','http://www.virusradar.com/en/Win32_Yurist/description','http://www.virusradar.com/en/Win32_IRCBot.OV/description','http://www.virusradar.com/en/Win32_Agent.NAH/description','http://www.virusradar.com/en/Win32_Himan.A/description','http://www.virusradar.com/en/Win32_Brontok.A/description','http://www.virusradar.com/en/Win32_Juny.A/description','http://www.virusradar.com/en/Win32_Brontok.T/description','http://www.virusradar.com/en/Win32_Pagun.F/description','http://www.virusradar.com/en/Win32_Bagle.GM/description','http://www.virusradar.com/en/Win32_XRat.P/description','http://www.virusradar.com/en/Win32_NoonLight.B/description','http://www.virusradar.com/en/Win32_Agent.ABF/description','http://www.virusradar.com/en/Win32_Exploit.Agent.N/description','http://www.virusradar.com/en/Win32_TrojanDownloader.Delf.AGR/description','http://www.virusradar.com/en/Win32_TrojanClicker.Small.KJ/description','http://www.virusradar.com/en/Win32_PSW.QQPass.JF/description','http://www.virusradar.com/en/Win32_Delf.NDF/description','http://www.virusradar.com/en/Win32_Delf.NDG/description']
    for db in db_data:
        driver.get(db)
        time.sleep(2)
        page_source = driver.page_source.strip().encode('utf-8')

	date_xpath = '//div[@id="vtab-40"]'
        date_extracted = driver.find_element_by_xpath(date_xpath)
	try:
	    created = date_extracted.text.split('Detection created')[1].split('\n')[0]
	except:
	    created = ''
	try:
	    category = date_extracted.text.split('Category')[1].split('\n')[0]
	except:
	    category = ''
	naming_convention_xpath = '//h2'
	naming_extracted = driver.find_element_by_xpath(naming_convention_xpath)
	naming = naming_extracted.text
	naming_ = naming.split('[')[0]
	naming_convention = naming_.split('.')[0]   ##platform/name

	original_name = naming_

	name_extr = naming_.split('/')[1].split('.')
	if len(name_extr) == 2:
	    name_extraction = name_extr[0]

	elif len(name_extr) == 3:
	    name_extraction = name_extr[1]
	else:
	    name_extraction = name_extr[0]


	try:
	    aliases_xpath = '//div[@id="vtab-40"]'
	    aliases_extracted = driver.find_element_by_xpath(aliases_xpath)
	    aliases = aliases_extracted.text.split('Aliases')[1]
	except:
	    aliases = ''
		 
	desc_path = '//div[@id="virus-about"]' 
	desc_ext = driver.find_element_by_xpath(desc_path)
	desc = desc_ext.text

	Description = desc.split('Installation')[0]
	try:
	    Other_Info = desc.split('Other information')[1]
	except:
	    Other_Info = ''

	Technical_ = desc.split('Installation')[1]
	try:
	    Technical = Technical_.split('Other information')[0]
	except:
	    Technical = Technical_

	Target_dict = {}
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
		Target = country_info + ':' + country_value
		Target_dict.update({country_info:country_value})
	    else:
		continue

        values = [db, naming_convention, original_name, name_extraction, category, aliases, created, created, Description.encode('utf-8'), Other_Info.encode('utf-8'), Technical.encode('utf-8'), Target_dict]
        csv_file.writerow(values)

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
