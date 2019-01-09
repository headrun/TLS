import re
import unicodedata
import configuration
import time
import MySQLdb
import logging
from time import strftime
import datetime


def generate_upsert_query_posts_crawl(crawler):
    table_name = configuration.tables[crawler]['post_crawl']
    upsert_query = """INSERT INTO {0} (sk,post_url,crawl_status,reference_url)values(%(sk)s,%(post_url)s, 0, %(reference_url)s)\
             ON DUPLICATE KEY UPDATE post_url = %(post_url)s, crawl_status = %(crawl_status)s, reference_url = %(reference_url)s """.format(table_name)
    return upsert_query

def generate_upsert_query_authors_crawl(crawler):
    table_name = configuration.tables[crawler]['author_crawl']
    upsert_query = """INSERT INTO {0} (post_id, auth_meta, links,crawl_status) VALUES(%(post_id)s, %(auth_meta)s, %(links)s,0) \
                    ON DUPLICATE KEY UPDATE auth_meta=%(auth_meta)s, links=%(links)s, crawl_status = 0""".format(table_name)
    return upsert_query

def generate_upsert_query_posts(crawler):
    table_name = configuration.tables[crawler]['posts']
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
    table_name = configuration.tables[crawler]['authors']
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

def fetch_time():
    return round((time.time()- time.timezone)*1000)

def time_to_epoch(str_of_time, str_of_patter):
    try:time_in_epoch = (int(time.mktime(time.strptime(str_of_time, str_of_patter))) - time.timezone) * 1000
    except:time_in_epoch = False
    return time_in_epoch

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
    return text

def clean_url(unclean_url):
   cleaned_url = re.sub(r'(\/\?|\/)$', '', unclean_url)
   return cleaned_url
