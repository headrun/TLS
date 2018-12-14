from scrapy.spider import Spider
import re
import unicodedata
import configuration
import time
import MySQLdb


def generate_upsert_query_posts_crawl(crawler):
   table_name = configuration.tables[crawler]['post_crawl']
   upsert_query = """INSERT INTO {0} (sk,post_url,crawl_status,reference_url)values(%(sk)s,%(post_url)s, 0, %(reference_url)s)\
            ON DUPLICATE KEY UPDATE post_url = %(post_url)s, crawl_status = %(crawl_status)s, reference_url = %(reference_url)s """.format(table_name)
   return upsert_query


def generate_upsert_query_posts(crawler):
    table_name = configuration.tables[crawler]['posts']

    upsert_query = """INSERT INTO {0} (domain, crawl_type, category, sub_category, thread_title,post_title, thread_url, post_id,\
                    post_url, publish_epoch, fetch_epoch, author, author_url, post_text, all_links, reference_url)\
                    VALUES( %(domain)s, %(crawl_type)s, %(category)s, %(sub_category)s, %(thread_title)s,%(post_title)s, %(thread_url)s,\
                    %(post_id)s, %(post_url)s, %(publish_epoch)s, %(fetch_epoch)s, %(author)s, %(author_url)s,\
                    %(post_text)s, %(all_links)s, %(reference_url)s) ON DUPLICATE KEY UPDATE crawl_type=%(crawl_type)s,\
                    category=%(category)s, sub_category=%(sub_category)s, thread_title=%(thread_title)s,post_title = %(post_title)s, \
                    thread_url=%(thread_url)s, post_url=%(post_url)s, publish_epoch=%(publish_epoch)s,\
                    fetch_epoch=%(fetch_epoch)s, author=%(author)s, author_url=%(author_url)s, post_text=%(post_text)s,\
                    all_links=%(all_links)s, reference_url=%(reference_url)s """.format(table_name)

    return upsert_query
def generate_upsert_query_authors(crawler):
    table_name = configuration.tables[crawler]['authors']

    upsert_query = """ INSERT INTO {0} (user_name, domain, crawl_type, author_signature, join_date, last_active, \
                    total_posts, fetch_time, groups, reputation, credits, awards, rank, active_time, contact_info,\
                    reference_url) VALUES ( %(user_name)s, %(domain)s, %(crawl_type)s, %(author_signature)s, \
                    %(join_date)s, %(last_active)s, %(total_posts)s, %(fetch_time)s, %(groups)s, %(reputation)s,\
                    %(credits)s, %(awards)s, %(rank)s, %(active_time)s, %(contact_info)s, %(reference_url)s )\
                    ON DUPLICATE KEY UPDATE crawl_type=%(crawl_type)s, author_signature=%(author_signature)s, \
                    join_date=%(join_date)s, last_active=%(last_active)s, total_posts=%(total_posts)s,\
                    fetch_time=%(fetch_time)s, groups=%(groups)s, reputation=%(reputation)s, credits=%(credits)s,\
                    awards=%(awards)s, rank=%(rank)s, active_time=%(active_time)s, contact_info=%(contact_info)s,\
                    reference_url=%(reference_url)s """.format(table_name)

    return upsert_query

def generate_upsert_query_crawl(crawler):
    table_name = configuration.tables[crawler]['crawl']

    upsert_query = """INSERT INTO {0} (post_id, auth_meta, crawl_status, links) VALUES(%(post_id)s, %(auth_meta)s,0, %(links)s) \
                    ON DUPLICATE KEY UPDATE auth_meta=%(auth_meta)s, crawl_status=0, links=%(links)s """.format(table_name)
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
            a = """[{ "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),totalposts.encode('utf8'))
            activetime.append(a)
        except:pass
    return str(activetime)




def clean_text(input_text):
    '''
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', input_text)
    text = re.sub(r'(\n\s*)', '\n', text)
    text = re.sub('\s\s+', ' ', text)
    return text
