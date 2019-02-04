#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/blackhat/blackhat/spiders;
scrapy crawl blackhat_crawl --logfile /home/epictions/Phase_03/log_files/blackhat_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/blackhat/blackhat/spiders;
scrapy crawl blackhat_thread --logfile /home/epictions/Phase_03/log_files/blackhat_posts.log ;
}


author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/blackhat/blackhat/spiders;
scrapy crawl blackhat_author --logfile /home/epictions/Phase_03/log_files/blackhat_authors.log ;
}

crawl
sleep 300

posts
for i in $( seq 1 40 )
    do
       posts
       sleep 600
    done

author
for i in $( seq 1 10)
    do 
       author
       sleep 300
    done

