#! /bin/bash

mysql_to_es()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/tls_scripts/scripts ;
python dump_to_es.py ;
}


mysql_to_es_hbh()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/tls_scripts/scripts ;
python extract_dump.py --DB_SCHEMA hellbound --TABLE_NAME_AUTHORS hellbound_authors --TABLE_NAME_POSTS hellbound_posts --DB_CHARSET utf8 --RECS_BATCH 25000 ;
}


mysql_to_es()
{
time_1="`date +%s`";
dump_to_es
sleep 10;
extract_dump_hbh   
time_2="`date +%s`";
let time_diff=$time_2-$time_1;
echo "start_time $time_1 and end_time $time_2 time_diff $time_diff" >> /tmp/mysql_to_es.log;
}


mysql_to_es || echo "somthing wrong in script please check it once at time `date`" >> /tmp/mysql_to_es.log





