#! /bin/bash


send_mail()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/tls_scripts/scripts;
python send_mail_log.py --BODY "Data is not populating to Elatic Search" --SUBJECT "Issue in Elastic Search"
}

mysql_to_es_fun()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/tls_scripts/scripts;
python dump_to_es.py
}


mysql_to_es()
{
time_1="`date +%s`";
mysql_to_es_fun
sleep 10;
time_2="`date +%s`";
let time_diff=$time_2-$time_1;
echo "start_time $time_1, and end_time $time_2, time_diff $time_diff" >> /tmp/mysql_to_es.log;

if [ $time_diff -lt 50 ]
then  
      send_mail
    fi
}

mysql_to_es || echo "somthing wrong in script please check it once at time `date`" >> /tmp/mysql_to_es_time.log






