#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/raidforums/raidforums/spiders;
#scrapy crawl raidforums_threads --logfile /home/epictions/Phase_01/log_files/raidforums_threads.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/raidforums/raidforums/spiders;
scrapy crawl raidforums_threads_meta --logfile /home/epictions/Phase_01/log_files/raidforums_threads_meta.log ;
}


authors()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/raidforums/raidforums/spiders;
scrapy crawl raidforums_author --logfile /home/epictions/Phase_01/log_files/raidforums_author.log ;
}




#crawl
#sleep 300

posts
sleep 600
authors
