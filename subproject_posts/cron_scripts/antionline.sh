
#antionline


export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/antionline/antionline/spiders; 
scrapy crawl antionline_crawl  --logfile /home/epictions/Phase_02/log_files/antionline_crawl.log



export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/antionline/antionline/spiders; 
scrapy crawl antionline_posts  --logfile /home/epictions/Phase_02/log_files/antionline_posts.log


export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/antionline/antionline/spiders;
scrapy crawl antionline_author  --logfile /home/epictions/Phase_02/log_files/antionline_author.log




