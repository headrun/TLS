#python extract_dump.py --DB_SCHEMA hellbound --TABLE_NAME_AUTHORS hellbound_authors --TABLE_NAME_POSTS hellbound_posts --DB_CHARSET utf8 --RECS_BATCH 25000
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
import optparse
parser = optparse.OptionParser()
parser.add_option("--DB_SCHEMA",dest = "DB_SCHEMA",help = "enter DB_SCHEMA")
parser.add_option("--TABLE_NAME_AUTHORS",dest = "TABLE_NAME_AUTHORS",help = "enter TABLE_NAME_AUTHORS")
parser.add_option("--TABLE_NAME_POSTS",dest = "TABLE_NAME_POSTS",help = "enter TABLE_NAME_POSTS")
parser.add_option("--DB_CHARSET",dest = "DB_CHARSET",help = "enter DB_CHARSET")
parser.add_option("--RECS_BATCH",dest = "RECS_BATCH",help = "enter RECS_BATCH")
(options,arguments) = parser.parse_args()
DB_SCHEMA = options.DB_SCHEMA
TABLE_NAME_AUTHORS = options.TABLE_NAME_AUTHORS
TABLE_NAME_POSTS = options.TABLE_NAME_POSTS
DB_CHARSET = options.DB_CHARSET
RECS_BATCH  = int(options.RECS_BATCH)
POSTS_COLUMNS = "domain, crawl_type, category, sub_category, thread_url, thread_title, post_id, post_url, post_title, publish_epoch as publish_time, fetch_epoch as fetch_time, author, author_url, post_text as text, all_links as links "#, reference_url"
AUTHOR_COLUMNS = "user_name as username,domain,crawl_type,author_signature as auth_sign,join_date,last_active as lastactive,total_posts as totalposts,fetch_time,groups,reputation,credits,awards,rank,active_time as activetimes,contact_info as contactinfo "#,reference_url"
CLAUSE = ''

def get_conn():
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
        es = Elasticsearch(['10.2.0.90:9342'])
        conn,cursor = get_conn()
        p_que = 'select count(*) from {0}'.format(TABLE_NAME_POSTS)
        cursor.execute(p_que)
        posts = cursor.fetchone()
        a_que = 'select count(*) from {0}'.format(TABLE_NAME_AUTHORS)
        cursor.execute(a_que)
        authors = cursor.fetchone()
        self.TOTAL_RECS_POSTS = int(posts.get('count(*)'))#posts[0])
        self.TOTAL_RECS_AUTHORS = int(authors.get('count(*)'))#authors[0])
        self.push_posts_data(es, cursor)
        self.push_authors_data(es, cursor)
        conn.close()

    def push_posts_data(self, es, cursor):
        print("Populating posts")
        for i in range(1, self.TOTAL_RECS_POSTS/RECS_BATCH + 2):
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
		post_url = post_record.get('post_url')
	        res = es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=post_record)
		#pprint(post_record)
        print("posts push is done")

    def push_authors_data(self, es, cursor):
        print("Populating authors")
        # Note: CLAUSE not appllicable for AUTHORS
        que = 'SELECT {0} from {1} '.format(AUTHOR_COLUMNS,TABLE_NAME_AUTHORS)
        cursor.execute(que)
        row1 = cursor.fetchall()
        for author_record in row1:
	    username = author_record.get('username')
            res = es.index(index="forum_author", doc_type='post', id=hashlib.md5(username).hexdigest(), body=author_record)
	    #pprint(author_record)
        print("authors push is done")


if __name__ == "__main__":   
    populate_data = Data() 
