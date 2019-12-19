
#binrev


crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/binrev/binrev/spiders; 
scrapy crawl binrev_crawl --logfile /home/epictions/Phase_02/log_files/binrev_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/binrev/binrev/spiders; 
scrapy crawl binrev_posts --logfile /home/epictions/Phase_02/log_files/binrev_posts.log
}


author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/binrev/binrev/spiders; 
scrapy crawl binrev_author --logfile /home/epictions/Phase_02/log_files/binrev_author.log
}


crawl
sleep 300

posts
sleep 300

author

