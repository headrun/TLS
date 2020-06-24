import json
import cfscrape
import re
import unicodedata
import configurations
import time
import MySQLdb
import logging
from time import strftime
import datetime
import dateutil.relativedelta
import os,sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('/home/epictions/mango/mango')
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from urllib import urlencode
from elasticsearch import Elasticsearch
from datetime import date, timedelta
import hashlib
import scrapy
from scrapy import Spider 
from scrapy.selector import Selector
from scrapy.http import Request
from urlparse import urljoin
from user_agent import user_agent_list

es = Elasticsearch(['10.2.0.90:9342'])
FORUM_POST = 'forum_posts'
DOC_TYPE = 'post'
FORUM_AUTHOR = 'forum_author'
DATABASE_USER = 'tls_dev'
DATABASE_PASS = 'hdrn!'
TOR_PASS = 'tls@2019'


def generate_upsert_query_posts_crawl(crawler):
    table_name = configurations.tables[crawler]['post_crawl']
    upsert_query = """INSERT INTO {0} (sk,post_url,crawl_status,reference_url)values(%(sk)s,%(post_url)s, 0, %(reference_url)s)\
             ON DUPLICATE KEY UPDATE post_url = %(post_url)s, crawl_status = %(crawl_status)s, reference_url = %(reference_url)s """.format(table_name)
    return upsert_query

def generate_upsert_query_authors_crawl(crawler):
    table_name = configurations.tables[crawler]['author_crawl']
    upsert_query = """INSERT INTO {0} (post_id, auth_meta, links,crawl_status) VALUES(%(post_id)s, %(auth_meta)s, %(links)s,0) \
                    ON DUPLICATE KEY UPDATE auth_meta=%(auth_meta)s, links=%(links)s, crawl_status = 0""".format(table_name)
    return upsert_query

def generate_upsert_query_posts(crawler):
    table_name = configurations.tables[crawler]['posts']
    upsert_query = """INSERT INTO {0} (domain, crawl_type, category, sub_category, thread_title,post_title, thread_url, post_id,\
                    post_url, publish_epoch, fetch_epoch, author, author_url, post_text, all_links, reference_url,created_at,modified_at)\
                    VALUES( %(domain)s, %(crawl_type)s, %(category)s, %(sub_category)s, %(thread_title)s,%(post_title)s, %(thread_url)s,\
                    %(post_id)s, %(post_url)s, %(publish_epoch)s, UNIX_TIMESTAMP(now())*1000 , %(author)s, %(author_url)s,\
                    %(post_text)s, %(all_links)s, %(reference_url)s,now(),now()) ON DUPLICATE KEY UPDATE crawl_type=%(crawl_type)s,\
                    category=%(category)s, sub_category=%(sub_category)s, thread_title=%(thread_title)s,post_title = %(post_title)s, \
                    thread_url=%(thread_url)s, post_url=%(post_url)s, publish_epoch=%(publish_epoch)s,\
                    fetch_epoch= UNIX_TIMESTAMP(now())*1000, author=%(author)s, author_url=%(author_url)s, post_text=%(post_text)s,\
                    all_links=%(all_links)s, reference_url=%(reference_url)s , modified_at = now()""".format(table_name)
    return upsert_query

def generate_upsert_query_authors(crawler):
    table_name = configurations.tables[crawler]['authors']
    upsert_query = """ INSERT INTO {0} (user_name, domain, crawl_type, author_signature, join_date, last_active, \
                    total_posts, fetch_time, groups, reputation, credits, awards, rank, active_time, contact_info,\
                    reference_url,created_at,modified_at) VALUES ( %(user_name)s, %(domain)s, %(crawl_type)s, %(author_signature)s, \
                    %(join_date)s, %(last_active)s, %(total_posts)s,UNIX_TIMESTAMP(now())*1000, %(groups)s, %(reputation)s,\
                    %(credits)s, %(awards)s, %(rank)s,  %(active_time)s, %(contact_info)s, %(reference_url)s,now(),now())\
                    ON DUPLICATE KEY UPDATE crawl_type=%(crawl_type)s, author_signature=%(author_signature)s, \
                    join_date=%(join_date)s, last_active=%(last_active)s, total_posts=%(total_posts)s,\
                    fetch_time=UNIX_TIMESTAMP(now())*1000, groups=%(groups)s, reputation=%(reputation)s, credits=%(credits)s,\
                    awards=%(awards)s, rank=%(rank)s, active_time=%(active_time)s, contact_info=%(contact_info)s,\
                    reference_url=%(reference_url)s ,modified_at = now()""".format(table_name)
    return upsert_query

def check_rec_in_es(sk):
    query={"query":{"match":{"_id":sk}}}
    res = es.search(body=query)
    if res['hits']['hits'] == []:
	return True
    else:
	return None

def doc_to_es(**kwargs):
    try:
	sk = kwargs['id']
        doc = kwargs['body']
        doc_type = kwargs['doc_type']
    except:
	raise Exception("need all id,doc_type and doc")
    if doc_type =='post':
        if check_rec_in_es(sk):
            es.index(index=FORUM_POST,doc_type=DOC_TYPE,id=sk,body=doc)
    elif doc_type == 'author':
        es.index(index=FORUM_AUTHOR,doc_type=DOC_TYPE,id=sk,body=doc)


def md5_val(text):
    return hashlib.md5(text).hexdigest()

def fetch_time():
    return round((time.time()- time.timezone)*1000)

def time_to_epoch(str_of_time, str_of_patter):
    try:time_in_epoch = (int(time.mktime(time.strptime(str_of_time, str_of_patter))) - time.timezone) * 1000
    except:time_in_epoch = None
    return time_in_epoch

def get_index(time_in_epoch):
    try:indices = time.strftime("%m_%Y", time.localtime(int(time_in_epoch/1000)))
    except:indices = time.strftime("%m_%Y", time.localtime(int(time_in_epoch)))
    return 'forum_posts_'+indices

def activetime_str(activetime_,totalposts):
    activetime = []
    for a in set(activetime_):
        try:
            dt = time.gmtime(int(a)/1000)
            a = """[{"year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s"}]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),totalposts.encode('utf8'))
            activetime.append(a)
        except:pass
    return ','.join(activetime)

def clean_text(input_text):
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', input_text)
    text = re.sub(r'(\n\s*)', '\n', text)
    text = re.sub('\s\s+', ' ', text)
    return text

def clean_url(unclean_url):
   cleaned_url = re.sub(r'(\/\?|\/)$', '', unclean_url)
   return cleaned_url

def decode_cloudflareEmail(cfString):
    rand = int(cfString[:2],16)
    email_id = ''.join([chr(int(cfString[i:i+2], 16) ^ rand) for i in range(2, len(cfString), 2)])
    return email_id

def que_filename(name):
    name = name + str(int(time.time()))
    que_val = open('/home/epictions/query_files/'+name+'.txt','w')    
    return que_val

def quefile_to_mysql(fname_,query,conn,cursor):
    fname = '/home/epictions/query_files/'+fname_
    que_file = open(fname,'a+')
    lines = que_file.readlines()
    log = open('/home/epictions/query_files/mysql_error.txt','a+')
    for val in lines:
        try:
	    val_ = json.loads(val)
	except:
	    log.write('it is not a dict {0}'.format(val))
	    continue
	try:
	    cursor.execute(query, val_)
	    conn.commit()
        except Exception as e:
            log.write('error: {0},filename:{1}'.format(str(e),fname_)+'\n')
    
    os.rename(que_file.name,que_file.name.replace('.txt','_old.txt'))
    que_file.close()

proxy_list = ['fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com','centralusa.torguardvpnaccess.com','west.usa.torguardvpnaccess.com','westusa.torguardvpnaccess.com','east.usa.torguardvpnaccess.com','eastusa.torguardvpnaccess.com']+ ['au.torguardvpnaccess.com', 'melb.au.torguardvpnaccess.com', 'bul.torguardvpnaccess.com', 'cp.torguardvpnaccess.com', 'egy.torguardvpnaccess.com', 'iom.torguardvpnaccess.com', 'isr.torguardvpnaccess.com', 'fin.torguardvpnaccess.com', 'br.torguardvpnaccess.com', 'ca.torguardvpnaccess.com', 'vanc.ca.west.torguardvpnaccess.com', 'frank.gr.torguardvpnaccess.com', 'ice.torguardvpnaccess.com', 'ire.torguardvpnaccess.com', 'in.torguardvpnaccess.com', 'jp.torguardvpnaccess.com', 'nl.torguardvpnaccess.com', 'lon.uk.torguardvpnaccess.com', 'ro.torguardvpnaccess.com', 'ru.torguardvpnaccess.com', 'mos.ru.torguardvpnaccess.com', 'swe.torguardvpnaccess.com', 'swiss.torguardvpnaccess.com', 'bg.torguardvpnaccess.com', 'hk.torguardvpnaccess.com', 'cr.torguardvpnaccess.com', 'hg.torguardvpnaccess.com', 'my.torguardvpnaccess.com', 'thai.torguardvpnaccess.com', 'turk.torguardvpnaccess.com', 'tun.torguardvpnaccess.com', 'mx.torguardvpnaccess.com', 'singp.torguardvpnaccess.com', 'saudi.torguardvpnaccess.com', 'fr.torguardvpnaccess.com', 'pl.torguardvpnaccess.com', 'czech.torguardvpnaccess.com', 'gre.torguardvpnaccess.com', 'it.torguardvpnaccess.com', 'sp.torguardvpnaccess.com', 'no.torguardvpnaccess.com', 'por.torguardvpnaccess.com', 'za.torguardvpnaccess.com', 'den.torguardvpnaccess.com', 'vn.torguardvpnaccess.com', 'sk.torguardvpnaccess.com', 'lv.torguardvpnaccess.com', 'lux.torguardvpnaccess.com', 'nz.torguardvpnaccess.com', 'md.torguardvpnaccess.com', 'uae.torguardvpnaccess.com', 'slk.torguardvpnaccess.com', 'fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com', 'centralusa.torguardvpnaccess.com', 'west.usa.torguardvpnaccess.com', 'westusa.torguardvpnaccess.com', 'east.usa.torguardvpnaccess.com', 'eastusa.torguardvpnaccess.com']
                                                                              
