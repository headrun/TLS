import deathbycaptcha
import MySQLdb
import os, sys, csv, re
from pyvirtualdisplay import Display
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import selenium
#res = Selector(text=response)
from scrapy.selector import Selector
import time
import scrapy
from scrapy.http import FormRequest
import hashlib
import utils
import datetime
import time
from datetime import timedelta
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
import urllib
dumps_query = utils.generate_upsert_query_dumps_posts('posts_rescator')
cards_query = utils.generate_upsert_query_cards_posts('posts_rescator')

class Rescator(scrapy.Spider):
    name = 'Rescator'

    def __init__(self):
        self.conn = MySQLdb.connect(
            db="posts_rescator",
            host="127.0.0.1",
            user="root",
            passwd="1216",
            use_unicode=True,
            charset="utf8mb4")
        self.cursor = self.conn.cursor()
        dispatcher.connect(self.mysql_conn_close, signals.spider_closed)

    def mysql_conn_close(self, spider):
        self.conn.commit()
        self.conn.close()

    def start_requests(self):
        driver = self.open_driver()
        driver.get('https://rescator.cm/')
        WebDriverWait(driver, 10)
        time.sleep(2)
        url= driver.find_element_by_xpath('//span[@id="captcha"]//img').get_attribute('src')
        headers = {
    'Connection': 'keep-alive',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'Origin': 'https://rescator.cm',
    'Upgrade-Insecure-Requests': '1',
    'Content-Type': 'application/x-www-form-urlencoded',
    'User-Agent': driver.execute_script("return navigator.userAgent;"),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Referer': 'https://rescator.cm/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}
        import pdb;pdb.set_trace()
        urllib.urlretrieve(url,"captcha_img.png")
        client = deathbycaptcha.SocketClient('Innominds','Helloworld1234')
        captcha = client.decode( "captcha_img.png" , type=2)
        data = {
  'login': 'inqspdr',
  'pass': 'r3$c@t!nqspdr',
  'captcharand': re.sub('(.*)xrand=','',url),
  'code': captcha.get('text'),
  'send_login': '\xA0Login\xA0'
}
        driver.quit()
        login_url = 'https://rescator.cm/?action=login'

        yield FormRequest(login_url,headers=headers,callback= self.login_page,formdata = data)

    def login_page(self,response):
        dumps = 'https://rescator.cm/?action=dumps'
        cards = 'https://rescator.cm/?action=cc'
        yield scrapy.Request(dumps,callback = self.dumps_page,headers = response.request.headers)
        yield scrapy.Request(cards,callback = self.cards_page,headers = response.request.headers)

    def dumps_page(self,response):
        domain = "rescator.cm"
        nodes = response.xpath('//tbody[@style="font-size:11px;"]//tr[contains(@id,"td1")]')
        for node in nodes:
            json_dumps = {}
            sk = ''.join(node.xpath('./@id').extract()).replace('td','')
            bin_ = ''.join(node.xpath('.//td[@class="width-50px"]/text()').extract()).strip()
            card = ''.join(node.xpath('.//td[@style="width:130px"]/text()').extract()).strip()
            code = ''.join(node.xpath('.//td[@class="width-10"]/text()').extract()).strip()
            debit_credit = ''.join(node.xpath('.//td[@style="width:50px"][1]/text()').extract()).strip()
            mark = ''.join(node.xpath('.//td[@style="width:50px"][2]/text()').extract()).strip()
            expires = ''.join(node.xpath('.//td[@style="width:50px"][3]/text()').extract()).strip()
            country = ''.join(node.xpath('.//td[@class="width-15"]/text()').extract()).strip()
            bank = ''.join(node.xpath('.//td[@style="width:250px"]/text() | .//a[@class="tooltip"]/text() | .//span[@style="color:red"]/text()').extract()).strip()
            base = ''.join(node.xpath('.//td[@style="width:150px;"]/text()').extract()).strip()
            price = ''.join(node.xpath('.//td[@class="width-10"]//strong/text()').extract()).strip()
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            json_dumps.update({'domain':domain,'sk':sk ,'bin_':bin_,'card':card,'code':code,'debit_credit':debit_credit,'mark':mark,'expires':expires,'country':country,'bank':bank,'base':base,'price':price,'track':'','cart':'','fetch_time':fetch_epoch,'reference_url':response.url})
            self.cursor.execute(dumps_query, json_dumps)
        navigation = response.xpath('//div[@class="btn-group"]//a[contains(text(),"Next ")]/@href').extract()
        for page in navigation:
            pages = "https://rescator.cm" + page
            yield scrapy.Request(pages,callback = self.dumps_page)

    def cards_page(self,response):
        domain = "rescator.cm"
        nodes = response.xpath('//tbody[@style="font-size:11px;"]//tr[contains(@id,"td11")]')
        for node in nodes:
            json_dumps = {}
            sk = ''.join(node.xpath('./@id').extract()).replace('td','')
            bin_ = ''.join(node.xpath('.//td[@class="width-50px"]/text()').extract()).strip()
            card = ''.join(node.xpath('.//td[@style="width:230px"]/text() ').extract()).strip()
            debit_credit = ''.join(node.xpath('.//td[@style="width:50px"][1]/text()').extract()).strip()
            expires = ''.join(node.xpath('.//td[@style="width:50px"][2]/text()').extract()).strip()
            mark = ''.join(node.xpath('.//td[@style="width:100px"]/text()').extract()).strip()
            country = ''.join(node.xpath('.//td[@class="width-15"][1]/text()').extract()).strip()
            zip_ = ''.join(node.xpath('.//td[@style="width:70px;"]/text()').extract()).strip()
            base = ''.join(node.xpath('.//td[@class="width-15"][2]/text()').extract()).strip()
            city = ','.join(node.xpath('.//td[9]//text()').extract()).strip()
            sate = ','.join(node.xpath('.//td[8]//text()').extract()).strip()
            price = ''.join(node.xpath('.//td[@class="width-10"]//strong/text()').extract()).strip()
            fetch_epoch = int(datetime.datetime.now().strftime("%s")) * 1000
            json_dumps.update({'domain':domain,'sk':sk ,'bin_':bin_,'card':card,'debit_credit':debit_credit,'mark':mark,'expires':expires,'country':country,'zip_':zip_,'base':base,'city':city,'sate':sate,'price':price,'phone':'','vbv':'','birthday':'','cart':'','fetch_time':fetch_epoch,'reference_url':response.url})
            self.cursor.execute(cards_query, json_dumps)
        navigation = response.xpath('//div[@class="btn-group"]//a[contains(text(),"Next")]/@href').extract()
        for page in navigation:
            pages = "https://rescator.cm" + page
            yield scrapy.Request(pages,callback = self.cards_page)

    def open_driver(self):
        #display = Display(visible=0, size=(800,600))
        #display.start()
        options = webdriver.ChromeOptions()
        options.add_argument('--no-sandbox')
        options.add_argument("--disable-extensions")
        options.add_argument('--headless')
        driver = webdriver.Chrome(chrome_options=options)
        return driver #display, driver

