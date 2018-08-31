import MySQLdb
import datetime
import os, sys, csv, hashlib
import logging
import logging.handlers
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
import calendar
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
    '''
    connection, cursor = open_mysql_connection()
    try:
        cursor.execute('select distinct(data_links) from crawling_sources.threats_kaspersky where crawl_status = 0 limit 10')
        db_data = cursor.fetchall()
        connection.commit()
    except Exception as exe:
        process_logger.debug('Exceptions while MySQl updation')
        process_logger.debug(str(traceback.format_exc()))
        pass
    '''
    #close_mysql_connection(connection, cursor)

    display, driver = open_driver()
    #smcraper = Scraper()
    #train_url = 'https://threats.kaspersky.com/en/threat/Adware.AndroidOS.Ewind/'
    #train_data = {'date': '09/28/2016', 'class': 'Adware', 'Platform': 'AndroidOS',}
    #scraper.train(train_url, train_data)
    db_data = ['https://threats.kaspersky.com/en/threat/DoS.LockAkk.Chass/', 'https://threats.kaspersky.com/en/threat/Adware.AndroidOS.Dilidi/']
    #db_data = ['https://threats.kaspersky.com/en/threat/Adware.AndroidOS.Dilidi/','https://threats.kaspersky.com/en/threat/Adware.AndroidOS.Ewind/','https://threats.kaspersky.com/en/threat/Adware.AndroidOS.Zapch/','https://threats.kaspersky.com/en/threat/Adware.Win32.Cydoor/','https://threats.kaspersky.com/en/threat/Adware.Win32.DealPly/','https://threats.kaspersky.com/en/threat/Adware.Win32.ELEX/','https://threats.kaspersky.com/en/threat/Adware.Win32.OpenCandy/','https://threats.kaspersky.com/en/threat/Adware.Win32.PurityScan/','https://threats.kaspersky.com/en/threat/Backdoor.AndroidOS.Coudw/','https://threats.kaspersky.com/en/threat/Backdoor.AndroidOS.Kresoc/','https://threats.kaspersky.com/en/threat/Backdoor.AndroidOS.Triada/','https://threats.kaspersky.com/en/threat/Backdoor.AndroidOS.Ztorg/','https://threats.kaspersky.com/en/threat/Backdoor.Java.Adwind/','https://threats.kaspersky.com/en/threat/Backdoor.Java.QRat/','https://threats.kaspersky.com/en/threat/Backdoor.Linux.Tsunami/','https://threats.kaspersky.com/en/threat/Backdoor.MSIL.Agent/','https://threats.kaspersky.com/en/threat/Backdoor.Win32.AckCmd/','https://threats.kaspersky.com/en/threat/Backdoor.Win32.Agobot/','https://threats.kaspersky.com/en/threat/Backdoor.Win32.Androm/','https://threats.kaspersky.com/en/threat/Backdoor.Win32.Bredolab/']
    for db_value in db_data:
        #db_value = db_values[0]
        #scraper_data = scraper.scrape(db_value)
	time.sleep(1)
        display, driver = open_driver()
        driver.get(db_value)
        time.sleep(5)
        print db_value
        page_source = driver.page_source.strip().encode('utf-8')
        #for data in scraper_data:
        import pdb;pdb.set_trace()
        try:
            date_given = driver.find_element_by_xpath('//tr[@class="line_info"]/td[@class="cell_two "]/strong')
            date_name = date.text()
            if date_given:
                date1 = str(date_given)
                date_ep = calendar.timegm(time.strptime(date1, '%m/%d/%Y'))* 1000
            else:
                date_ep = None 
        except:
            date_ep = None 
        try:
            class_given = driver.find_element_by_xpath('//tr[@class="line_info"]//td[contains(text() ,"Class")]/following-sibling::td/a')
            class_name = class_given.text
            if class_name:
                class_ = class_name.strip()
            else:
                class_ = None
        except:
            class_ = None
        try:
            platform_given=driver.find_element_by_xpath('//tr[@class="line_info"]//td[contains(text() ,"Platform")]/following-sibling::td/a')
            platform_name = platform_given.text
            if platform_name:
                platform = platform_name.strip()
            else:
                platform = None
        except:
            platform = None

        ### title
        title_xpath = '//h1[@class="main_title average_title"]'
        title_extracted = driver.find_element_by_xpath(title_xpath)
        title = title_extracted.text

        original_name = title

        na_con = title.split('.')[1:]
        nam_conv = '.'.join(na_con)
        naming_convention = title.split('.')[0]+ '.' + '{' + nam_conv + '}'
        name_extraction = title.split('.')[-1]
        ###description
        description_xpath = '//tr[@class="line_info"]//td[contains(text(), "Desc")]//following-sibling::td'
        description_extracted = driver.find_element_by_xpath(description_xpath)
        description = description_extracted.text
        ###technical description
        try:
            tech_description_xpath = '//td[@class="cell_two "]//h2[contains(text(), "Technical Details")]/..'
            tech_description_extracted = driver.find_element_by_xpath(tech_description_xpath)
            tech_description = tech_description_extracted.text
        except:
            tech_description = ''
        if 'Removal instructions' in tech_description:
            removal = tech_description.split('Removal instructions')[1]
            tech_description = tech_description.split('Removal instructions')[0]
        else:
            removal = ''
        ###Target Countries
        all_count = []
        try:
            count_xpath = '//table[@class="most_attacked_countries"]//tr'
            count_extracted = driver.find_elements_by_xpath(count_xpath)
            for count_extr in count_extracted:
                countr = count_extr.find_elements_by_xpath('td')[1].text
                all_count.append(countr)
        except:
           Countries = ''
	tist = calendar.timegm(time.gmtime())* 1000
        all_count = all_count[1:]
        '''
        doc = {'name': name_extraction, 'original_name': original_name, 'source': 'kaspersky', 'domain': 'kaspersky.com','url': db_value ,'description': description, 'type': class_, 'platform': platform, 'removal': removal, 'technical_details': tech_description, 'first_seen': date_ep, 'last_seen': date_ep, 'target_countries': all_count, 'created_at': tist, 'updated_at': tist}     
        ###Elastic Search
        res = es.index(index="malware_docs", doc_type='malwaredoc', id=hashlib.md5(db_value).hexdigest(), body=doc) 
        upd_qry = 'update crawling_sources.threats_kaspersky set crawl_status= 1 where data_links = "%s"'
        values = db_value
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
