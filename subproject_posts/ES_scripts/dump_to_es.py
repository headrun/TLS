#version2
import hashlib
import MySQLdb
import csv
import datetime
import sys
from elasticsearch import Elasticsearch
from pprint import pprint
import MySQLdb.cursors
reload(sys)
sys.setdefaultencoding('utf-8')
from dump_config import crawlers
POSTS_COLUMNS = "domain, crawl_type, category, sub_category, thread_url, thread_title, post_id, post_url, post_title, publish_epoch as publish_time, fetch_epoch as fetch_time, author, author_url, post_text as text, all_links as links "#, reference_url"
AUTHOR_COLUMNS = "user_name as username,domain,crawl_type,author_signature as auth_sign,join_date,last_active as lastactive,total_posts as totalposts,fetch_time,groups,reputation,credits,awards,rank,active_time as activetimes,contact_info as contactinfo "#,reference_url"
CLAUSE = ''

def get_conn(DB_SCHEMA,DB_CHARSET):
    conn=MySQLdb.connect(db=DB_SCHEMA,
			host="localhost",
			user="root",
			passwd="",
			use_unicode=True,
			charset=DB_CHARSET,
			cursorclass = MySQLdb.cursors.DictCursor)
    cursor=conn.cursor()
    return conn,cursor


class Data():

    def __init__(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])	
	for crawler in crawlers:
	    self.dump_fun(crawler)

    def dump_fun(self,crawler):
	es = self.es
	DB_CHARSET = crawler.get('DB_CHARSET')
	DB_SCHEMA = crawler.get('DB_SCHEMA')
	global TABLE_NAME_AUTHORS 
	global TABLE_NAME_POSTS 
	global RECS_BATCH 
	global conn
	global cursor 
	RECS_BATCH = crawler.get('RECS_BATCH')
	TABLE_NAME_POSTS = crawler.get('TABLE_NAME_POSTS')
	TABLE_NAME_AUTHORS = crawler.get('TABLE_NAME_AUTHORS')
	conn, cursor = get_conn(DB_SCHEMA,DB_CHARSET)
        p_que = 'select count(*) from {0}'.format(TABLE_NAME_POSTS)
        cursor.execute(p_que)
        posts = cursor.fetchone()
	global TOTAL_RECS_POSTS
        TOTAL_RECS_POSTS = int(posts.get('count(*)'))
        self.push_posts_data(es, cursor)
        self.push_authors_data(es, cursor)
        conn.close()

    def push_posts_data(self, es, cursor):
        print("Populating posts")
        for i in range(1, TOTAL_RECS_POSTS/RECS_BATCH + 2):
            query = 'SELECT {0} from {1} {2} limit {3},{4} '.format(POSTS_COLUMNS, TABLE_NAME_POSTS,CLAUSE,(i-1)*RECS_BATCH, RECS_BATCH)
            print(query)
            cursor.execute(query)
            rows = cursor.fetchall()
            print("Iteration: ", i)
            for post_record in rows:
                try:
                    post_record['sub_category'] = eval(post_record.get('sub_category'))
		except:pass
		try:
	            post_record['links'] = eval(post_record.get('links'))
                except:pass
                if not post_record.get('publish_time'): post_record['publish_time'] = 0
		if 'hellbound' in 
	        res = es.index(index="forum_posts", doc_type='post', id=hashlib.md5(post_record.get().hexdigest(), body=post_record)
		#pprint(post_record)
        print("posts push is done")

    def push_authors_data(self, es, cursor):
        print("Populating authors")
        # Note: CLAUSE not appllicable for AUTHORS
        que = 'SELECT {0} from {1} '.format(AUTHOR_COLUMNS,TABLE_NAME_AUTHORS)
        cursor.execute(que)
        row1 = cursor.fetchall()
	print que
        for author_record in row1:
            #res = es.index(index="forum_author", doc_type='post', id=hashlib.md5(username).hexdigest(), body=post_record)
	    #pprint(author_record)
	    pass
        print("authors push is done")


if __name__ == "__main__":   
    populate_data = Data() 
