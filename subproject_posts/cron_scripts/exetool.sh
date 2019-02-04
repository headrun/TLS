#! /bin/bash

posts_and_authors()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/exe_tools/exe_tools/spiders; 
scrapy crawl exetool --logfile /home/epictions/Phase_01/log_files/exetool.log
}


posts_and_authors
sleep 600
