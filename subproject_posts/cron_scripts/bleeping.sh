#! /bin/bash


crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/bleeping_computer/bleeping_computer/spiders; 
scrapy crawl bleeping_threads_crawl --logfile /home/epictions/Phase_01/log_files/bleeping_thread_crawl.log ;
}


posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/bleeping_computer/bleeping_computer/spiders;
scrapy crawl bleeping_threads --logfile /home/epictions/Phase_01/log_files/bleeping_threads.log
}


author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_01/bleeping_computer/bleeping_computer/spiders;
scrapy crawl bleeping_author --logfile /home/epictions/Phase_01/log_files/bleeping_author.log
}


crawl

posts

#author

