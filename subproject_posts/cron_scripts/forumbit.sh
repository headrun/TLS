#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/media/media/spiders;
scrapy crawl forumbit_crawl  --logfile /home/epictions/Phase_03/log_files/forumbit_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/media/media/spiders;
scrapy crawl forumbit_posts   --logfile /home/epictions/Phase_03/log_files/forumbit_posts.log ;
}

author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_03/media/media/spiders;
scrapy crawl forumbit_author --logfile /home/epictions/Phase_03/log_files/forumbit_author.log ;
}

crawl
sleep 300

posts
sleep 600

author
