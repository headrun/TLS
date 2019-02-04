#! /bin/bash

posts()
{

export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/boardbat/boardbat/spiders;
scrapy crawl boardbat_thread --logfile /home/epictions/Phase_01/log_files/boardbat_thread.log ;

}

posts

