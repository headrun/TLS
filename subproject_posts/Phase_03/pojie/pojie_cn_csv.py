import MySQLdb
import csv
import datetime
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')


def get_conn():
    conn=MySQLdb.connect(db="POJIE_DB",host="localhost",user="root",passwd="root", use_unicode=True,charset="utf8")
    cursor=conn.cursor()
    return conn,cursor

class Data():
    conn,cursor = get_conn()

    csv_path = os.path.join(os.getcwd(), 'csv')
    if not os.path.isdir(csv_path):
        os.mkdir(os.path.abspath('csv/'))
    file_name = "TlS_Posts_pojie_%s.csv"%(str(datetime.datetime.now().date()))
    oupf = open(os.path.join(csv_path, file_name), "wb+")
    todays_excel_file = csv.writer(oupf)
    headers = ['Domain','crawl_type', 'Category','SubCategory', 'Thread Title', \
        'Thread Url', 'Post id', 'Post` Url', 'Publish Time', \
        'Fetch Time', 'author', "author_url",'post_title', 'Text', \
        ' all_links','reference_url','created_at','modified_at']
    todays_excel_file.writerow(headers)
    excel_auth_name1 = "TLS_Author_pojie_%s.csv"%(str(datetime.datetime.now().date()))
    oupf1 = open(os.path.join(csv_path, excel_auth_name1), 'wb+')
    todays_excel_file1  = csv.writer(oupf1)
    headers1 = ['username', 'Domain', 'crawl_type', 'author_signature',\
    'join_date','lastactive','totalposts','FetchTime', 'groups',\
    'reputation','credits','awards','rank','activetime','contactinfo','reference_url']
    todays_excel_file1.writerow(headers1)

    query = 'SELECT * from pojie_posts'
    cursor.execute(query)
    row = cursor.fetchall()
    for i in row:
        todays_excel_file.writerow(i)
    que = 'SELECT * from pojie_authors'
    cursor.execute(que)
    row1 = cursor.fetchall()
    for j in row1:
        todays_excel_file1.writerow(j)
    conn.close()

