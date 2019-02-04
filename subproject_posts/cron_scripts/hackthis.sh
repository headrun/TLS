#! /bin/bash/

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hackthis/hackthis/spiders;
scrapy crawl  hackthis_crawl   --logfile /home/epictions/Phase_02/log_files/hackthis_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hackthis/hackthis/spiders;
scrapy crawl  hackthis_thread   --logfile /home/epictions/Phase_02/log_files/hackthis_thread.log ;
}


author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hackthis/hackthis/spiders;
scrapy crawl  hackthis_author   --logfile /home/epictions/Phase_02/log_files/hackthis_author.log ;
}

crawl 
sleep 300

posts
sleep 600

author
