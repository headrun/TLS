import  nulled_xpath 
import csv
import datetime
from datetime import timedelta
import time
import re
import MySQLdb
import json
from pyvirtualdisplay import Display
import selenium
from selenium import webdriver
from scrapy.selector import Selector
from selenium.webdriver.support.wait import WebDriverWait


conn = MySQLdb.connect(db="nulled", host="localhost", user="root", passwd="", use_unicode=True, charset="utf8")
cursor = conn.cursor()

def close_conn( spider):
    conn.commit()
    conn.close()

def start_requests(display,driver):
    start_url = 'https://www.nulled.to/#!General+Discussion'
    driver.get(start_url)
    time.sleep(1.5)
    WebDriverWait(driver, 10)
    response = Selector(text = driver.page_source)
    formus = response.xpath(nulled_xpath.formus_xpath).extract()
    meta = {'crawl_type':'keep_up'}
    for url in formus:
        if 'https://www.nulled.to/' in url:
            parde_forms(url, meta, driver,display)

def parde_forms(url_,meta,driver,display):
    driver.get(url_)
    time.sleep(1.5)
    WebDriverWait(driver, 10)
    response = Selector(text = driver.page_source)
    crawl_type = meta.get('crawl_type','')
    threads_id = response.xpath(nulled_xpath.threads_id_xpath).extract()
    threads_links = response.xpath(nulled_xpath.threads_urls_xpath).extract()
    for id_, url in zip(threads_id,threads_links):
        sk =''.join(re.findall('\d+',id_))
        val = (sk, url, 0,crawl_type, url_,0,crawl_type,url,url_)
        cursor.execute(nulled_xpath.Threads_crawl_que,val)
    
    next_page = ''.join(response.xpath(nulled_xpath.threads_next_page_xpath).extract())
    if next_page:
        meta = {'crawl_type':'catch_up'}
        if 'https://www.nulled.to/' in  next_page:
            parde_forms(next_page, meta, driver,display)

def open_driver():
    display = Display(visible=0, size=(800,600))
    display.start()
    options = webdriver.ChromeOptions()
    options.add_argument("--incognito")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument('--headless')
    driver = webdriver.Chrome(chrome_options=options)
    return display,driver

def close_driver(display, driver):
    try:
        display.stop()
        driver.quit()
    except Exception as exe:
        pass

if __name__ == "__main__":
    display, driver = open_driver()
    time.sleep(1.5)
    WebDriverWait(driver, 10)
    start_requests(display,driver)
    driver.close()

