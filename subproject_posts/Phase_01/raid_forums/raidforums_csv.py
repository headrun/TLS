import MySQLdb
import csv
import datetime
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
def get_conn():
    #conn=MySQLdb.connect(db="BleepingComputer_DB",host="localhost",user="root",passwd="123",use_unicode=True,charset="utf8")
    conn=MySQLdb.connect(db="posts_raidforums",host="localhost",user="root",passwd = "",use_unicode=True,charset="utf8")
    cursor=conn.cursor()
    return conn,cursor
class A():
    conn,cursor = get_conn()
    excel_file_name = 'RAIDF_thread%s.csv'%str(datetime.datetime.now()).replace(' ','')
    oupf = open(excel_file_name, 'wb+')
    todays_excel_file = csv.writer(oupf)
    headers = ['Domain','crawl_type','Category','SubCategory', 'ThreadTitle', 'ThreadUrl', 'Postid', 'Posturl', 'PublishTime', 'FetchTime','Author', 'author_url','post_title','Text', ' Links','reference_url']
    todays_excel_file.writerow(headers)
    excel_auth_name1 = 'RAIDF_Author%s.csv'%str(datetime.datetime.now()).replace(' ','')
    oupf1 = open(excel_auth_name1, 'wb+')
    todays_excel_file1  = csv.writer(oupf1)
    headers1 = ['username', 'domain','crawl_type','author_signature','join_date','lastactive','totalposts','FetchTime','groups','reputation','credits','awards','rank','activetime','contactinfo','reference_url']
    todays_excel_file1.writerow(headers1)
    que = 'SELECT * from raidforums_posts'
    cursor.execute(que)
    row = cursor.fetchall()
    for i in row:
        x=[]
        for e in i:
            x.append(e)
            #try:
                #e = e.replace(';','0x3B')
                #x.append(e)
            #except:
                #x.append(e)
        todays_excel_file.writerow(x)
    que1 = 'SELECT * from raidforums_authors'
    cursor.execute(que1)
    row1 = cursor.fetchall()
    for j in row1:
        x=[]
        for e in j:
            x.append(e)
            #try:
                #e = e.replace(';','0x3B')
                #x.append(e)
            #except:
                #x.append(e)
        todays_excel_file1.writerow(x)
    cursor.close()
    conn.close()


