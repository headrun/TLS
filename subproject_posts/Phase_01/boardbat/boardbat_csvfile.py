import MySQLdb
import csv
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def get_conn():
    conn=MySQLdb.connect(db="bb_posts",host="localhost",user="root",passwd="",use_unicode=True,charset="utf8")
    cursor=conn.cursor()
    return conn,cursor


class Data():
    conn,cursor = get_conn()
    excel_file_name = 'board_thread_%s.csv'
    oupf = open(excel_file_name, 'wb+')
    todays_excel_file = csv.writer(oupf)
    headers = ['Domain','crawl_type', 'Category','SubCategory', 'threadtitle', 'Thread Url', 'Post id', 'Post` Url', 'Publish Time', 'Fetch Time', 'Author','author_url', 'posttitle', 'Text', ' Links','reference_url']
    todays_excel_file.writerow(headers)

    query = 'SELECT * from boardbat_posts'
    cursor.execute(query)
    row = cursor.fetchall()
    for i in row:
        x=[]
        for e in i:
            try:
                e = e.replace(u'\\xa0',u'').encode('utf-8')
                x.append(e)
            except:
                pass
        x = tuple(x)
        todays_excel_file.writerow(i)
    conn.close()
