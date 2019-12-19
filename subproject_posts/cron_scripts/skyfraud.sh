#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/skyfraudru/skyfraudru/spiders;
scrapy crawl  skyfraud_crawl   --logfile /home/epictions/Phase_03/log_files/skyfraud_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/skyfraudru/skyfraudru/spiders;
scrapy crawl  skyfraud_posts   --logfile /home/epictions/Phase_03/log_files/skyfraud_posts.log ;
}

author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/skyfraudru/skyfraudru/spiders;
scrapy crawl skyfraud_author --logfile /home/epictions/Phase_03/log_files/skyfraud_author.log ;
}

crawl
sleep 300

posts
sleep 600

author

