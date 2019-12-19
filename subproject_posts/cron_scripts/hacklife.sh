#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/hacklife/hacklife/spiders;
scrapy crawl hacklife_crawl --logfile /home/epictions/Phase_03/log_files/hacklife_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/hacklife/hacklife/spiders;
scrapy crawl hacklife_posts --logfile /home/epictions/Phase_03/log_files/hacklife_posts.log ;
}

author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/hacklife/hacklife/spiders;
scrapy crawl hacklife_authors --logfile /home/epictions/Phase_03/log_files/hacklife_authors.log ;
}

crawl
sleep 300

posts
sleep 600

author

