import MySQLdb

def connection():
    conn = MySQLdb.connect(db= "nulled", host = "localhost", use_unicode=True, charset="utf8mb4")
    cursor = conn.cursor()
    return conn,cursor

def change():
    conn,cursor = connection()
    que = 'select author_url from nulled_posts'
    cursor.execute(que)
    data = cursor.fetchall()
    for da in data:
        da = da[0]
        if da[0:len(da)/2]==da[len(da)/2::]:
            up_que = 'update nulled_posts set author_url = %(url)s where author_url = %(post_url)s'
            val = {'url':da[0:len(da)/2],'post_url':da}
            cursor.execute(up_que,val)
    conn.close()
change()

