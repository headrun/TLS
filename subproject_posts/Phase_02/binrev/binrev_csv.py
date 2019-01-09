import MySQLdb
import csv
import datetime
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')

def get_conn():
    #conn=MySQLdb.connect(db="KERNELDATA",host="localhost",user="root",passwd="1216",use_unicode=True,charset="utf8")
    conn = MySQLdb.connect(user="root", host = "localhost", db="binrev", passwd="", use_unicode=True, charset = 'utf8')
    cursor=conn.cursor()
    return conn,cursor


class Data():
    conn,cursor = get_conn()


    csv_path = os.path.join(os.getcwd(), 'csv')
    if not os.path.isdir(csv_path):
        os.mkdir(os.path.abspath('csv/'))
    file_name = "binrevposts_%s.csv"%(str(datetime.datetime.now().date()))
    #excel_file_name = 'boardthread%s.csv'%str(datetime.datetime.now())
    #oupf = open(excel_file_name, 'wb+')
    oupf = open(os.path.join(csv_path, file_name), "wb+")
    todays_excel_file = csv.writer(oupf)
    headers = ['Domain','Crawl_type','Category','SubCategory', 'Thread_Title', 'Post_Title', 'Thread_Url', 'Post_id', 'Post_Url', 'Publish _Time', 'Fetch_Time', 'Author','Author_Url', 'Post_Text', ' All_Links', 'reference_url', 'Created_at', 'Modified_at'] #'Crawl_Type'
    todays_excel_file.writerow(headers)

    #excel_auth_name1 = 'board_author_Details_%s.csv'%str(datetime.datetime.now())
    excel_auth_name1 = "binrevauthor_%s.csv"%(str(datetime.datetime.now().date()))
    oupf1 = open(os.path.join(csv_path, excel_auth_name1), 'wb+')
    todays_excel_file1  = csv.writer(oupf1)
    headers1 = ['username', 'domain', 'Crawl_Type','author_signature', 'join_date','lastactive','totalposts','FetchTime','Groups','reputation','credits','awards', 'rank','activetime', 'contact_info', 'reference_url', 'Created_at', 'Modified_at']
    todays_excel_file1.writerow(headers1)

    query = 'SELECT * from binrev_posts'
    cursor.execute(query)
    row = cursor.fetchall()
    for i in row:
        x=[]
        for e in i:
            try:
                #e = str(e)
                #e = e.replace(';',':')
                e = e.replace(';','0x3B')
                x.append(e)
            except:
                x.append(e)
        x = tuple(x)
        todays_excel_file.writerow(x)

    que = 'SELECT * from binrev_authors'
    cursor.execute(que)
    row1 = cursor.fetchall()
    for j in row1:
        x=[]
        for e in j:
	    try:
                e = e.replace(';','0x3B')
                x.append(e)
            except:
                x.append(e)
        x = tuple(x)
    	todays_excel_file1.writerow(x)
    conn.close()

