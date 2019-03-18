#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/monopoly/monopoly/spiders;
scrapy crawl monopoly_posts --logfile /home/epictions/Phase_03/log_files/monopoly_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/monopoly/monopoly/spiders;
scrapy crawl monopoly_post  --logfile /home/epictions/Phase_03/log_files/monopoly_posts.log ;
}

crawl
sleep 300

posts
sleep 600

