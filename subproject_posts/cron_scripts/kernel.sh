#! /bin/bash

#kernel

posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/kernelmode/kernelmode/spiders; 
scrapy crawl kernel_thread --logfile /home/epictions/Phase_01/log_files/kernel_thread.log
}

authors()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/kernelmode/kernelmode/spiders; 
scrapy crawl kernel_author --logfile /home/epictions/Phase_01/log_files/kernel_author.log
}

mysql_to_ES()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/tls_scripts/scripts;
python extract_dump.py --DB_SCHEMA posts --TABLE_NAME_AUTHORS kernel_authors --TABLE_NAME_POSTS kernel_posts --DB_CHARSET utf8 --RECS_BATCH 25000
}

posts
authors
#mysql_to_ES
