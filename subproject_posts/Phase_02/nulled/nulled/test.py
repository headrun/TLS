import MySQLdb

def connection():
    conn = MySQLdb.connect(db= "nulled", host ="localhost", use_unicode=True,  charset="utf8mb4")#MySQLdb.connect(db= "nulled", host = "127.0.0.1", user="root", passwd = "123", use_unicode=True, charset="utf8mb4")
    cursor = conn.cursor()
    return conn,cursor

def change():
    conn,cursor = connection()
    que = 'select post_url from nulled_posts'
    cursor.execute(que)
    data = cursor.fetchall()
    print len(data)
    x = 0
    for da in data:
	print x
	x = x+1
        da = da[0]
        if da[0:len(da)/2]==da[len(da)/2::]:
            up_que = 'update nulled_posts set post_url = %(url)s where post_url = %(post_url)s'
            val = {'url':da[0:len(da)/2],'post_url':da}
            cursor.execute(up_que,val)
    conn.close()
change()

