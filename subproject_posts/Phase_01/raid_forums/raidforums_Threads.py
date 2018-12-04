import sys
reload(sys)
#sys.setdefaultencoding('utf-8')
#encoding: utf- 8
from scrapy.http import Request
import csv
import datetime
import scrapy
import time
import re
import MySQLdb
import datetime
import json
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from pyvirtualdisplay import Display
from selenium import webdriver
import logging

class Raidforums(scrapy.Spider):
    name = 'raidforums_Threads'
    start_urls = ['https://raidforums.com/']
    log_file_name = 'raidforums_%s.log'%str(datetime.datetime.now()).replace(' ','')
    logging.basicConfig(filename = log_file_name,level=logging.DEBUG)

    def __init__(self):
        self.conn = MySQLdb.connect(db="posts_raidforums", host="localhost", user="root", passwd="", use_unicode=True, charset="utf8")
        self.cursor=self.conn.cursor()
        dispatcher.connect(self.close_conn, signals.spider_closed)

    def close_conn(self, spider):
        self.conn.commit()
        self.conn.close()

    def parse(self, response):
        nodes = response.xpath('//tr[@class="forumstyles"]') 
        for node in nodes:
            link = ''.join(node.xpath('.//strong/a/@href').extract())
            if 'https:/' not in link:link = 'https://raidforums.com/'+link
            yield Request(link,callback = self.parse_forum)

    def parse_forum(self,response):
        n_page = response.xpath('//div[@class="pagination"]//a[@class="pagination_next"]//@href').extract()
        if n_page:
            if 'https:/' not in n_page[0]:pg = 'https://raidforums.com/' + n_page[0]
            yield Request(pg, callback = self.parse_forum)
        Threads_urls = response.xpath('//table[@class="tborder clear"]//tr//span[@class=" subject_new"]/a/@href').extract()
        for url in Threads_urls:
            if 'https:/' not in url:url = 'https://raidforums.com/'+ url
            #yield Request(url, callback = self.parse_comm)
            test_que_in = 'select * from raidforums_status where url = "%s"'%MySQLdb.escape_string(url)
            self.cursor.execute(test_que_in)
            test_que_in = self.cursor.fetchall()
            if not test_que_in:
                que = "insert into raidforums_status(url,status_code,crawl_type) values('%s','%s','%s')"%(MySQLdb.escape_string(url),0,'keepup')
                self.cursor.execute(que)
            else:
                que = 'update raidforums_status set status_code = 0 where url = "%s"'%MySQLdb.escape_string(url)
                self.cursor.execute(que)
