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


mysql_to_es
sleep 10
mysql_to_es_hbh






