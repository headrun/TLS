#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/at4re/at4re/spiders;
scrapy crawl  at4re_browse   --logfile /home/epictions/Phase_03/log_files/at4re_browse.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/at4re/at4re/spiders;
scrapy crawl  at4re_posts   --logfile /home/epictions/Phase_03/log_files/at4re_posts.log ;
}

author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/at4re/at4re/spiders;
scrapy crawl at4re_author --logfile /home/epictions/Phase_03/log_files/at4re_author.log ;
}

crawl
sleep 300

posts
sleep 600

author

