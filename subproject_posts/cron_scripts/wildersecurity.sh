#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/wildersecurity/wildesecurity/spiders;
scrapy crawl wilderssecurity_crawl --logfile /home/epictions/Phase_02/log_files/wilderssecurity_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/wildersecurity/wildesecurity/spiders;
scrapy crawl wilderssecurity --logfile /home/epictions/Phase_02/log_files/wilderssecurity_posts.log ;
}


author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/wildersecurity/wildesecurity/spiders;
scrapy crawl wilderssecurity_author --logfile /home/epictions/Phase_02/log_files/wilderssecurity_author.log ;
}

crawl
sleep 300

posts
sleep 300
author
