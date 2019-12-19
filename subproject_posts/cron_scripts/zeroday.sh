#! /bin/bash

crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/zero_daytoday/zero_daytoday/spiders;
scrapy crawl 0_daytoday_browse --logfile /home/epictions/Phase_02/log_files/0_daytoday_browse.log ;
}

posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/zero_daytoday/zero_daytoday/spiders;
scrapy crawl 0_daytoday_posts --logfile /home/epictions/Phase_02/log_files/0_daytoday_posts.log ;
}

posts_boxdata()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/zero_daytoday/zero_daytoday/spiders;
scrapy crawl 0_daytoday_posts_boxdata --logfile /home/epictions/Phase_02/log_files/0_daytoday_posts_boxdata.log ;
}


author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/zero_daytoday/zero_daytoday/spiders;
scrapy crawl 0daytoday_author --logfile /home/epictions/Phase_02/log_files/0daytoday_author.log ;
}


crawl
sleep 300

posts
sleep 300

posts_boxdata
sleep 600

author
