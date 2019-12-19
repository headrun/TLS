#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/prologic/prologic/spiders;
scrapy crawl  prologic_threads_crawl   --logfile /home/epictions/Phase_03/log_files/prologic_threads_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/prologic/prologic/spiders;
scrapy crawl  prologic_posts   --logfile /home/epictions/Phase_03/log_files/prologic_posts.log ;
}

crawl
sleep 300

posts
sleep 600


