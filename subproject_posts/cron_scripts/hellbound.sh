
#hellbound
crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hellbound/hellbound/spiders; 
scrapy crawl hellbound_thread_crawl --logfile /home/epictions/Phase_02/log_files/hellbound_thread_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hellbound/hellbound/spiders; 
scrapy crawl hellbound_posts --logfile /home/epictions/Phase_02/log_files/hellbound_posts.log
}


author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hellbound/hellbound/spiders; 
scrapy crawl hellbound_authors --logfile /home/epictions/Phase_02/log_files/hellbound_authors.log
}


crawl
sleep 300

posts
sleep 300

author

