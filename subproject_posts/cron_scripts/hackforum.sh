#! /bin/bash

my_fun_thread_crawl()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hackforums/hackforums/spiders;
scrapy crawl hackforums_crawl --logfile /home/epictions/Phase_02/log_files/hackforums_crawl.log 
}


send_mail()
{ 
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02;
python send_mail_log.py 
}


time_1="`date +%s`";
my_fun_thread_crawl
time_2="`date +%s`";
let first_test=$time_2-$time_1;
if [ $first_test -lt 300 ]
then
  my_fun_thread_crawl
  let sec_test="`date +%s`"-$time_1;
  if [ $sec_test -lt 600 ]
  then
    my_fun_thread_crawl
    let three_test="`date +%s`"-$time_1;
    if [ $three_test -lt 900 ]
    then
      send_mail
    fi
  fi
fi




my_fun_posts()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hackforums/hackforums/spiders;
scrapy crawl hackforums_posts --logfile /home/epictions/Phase_02/log_files/hackforums_posts.log
}


time_1="`date +%s`";
my_fun_posts
time_2="`date +%s`";
let first_test=$time_2-$time_1;
if [ $first_test -lt 300 ]
then
  my_fun_posts
  let sec_test="`date +%s`"-$time_1;
  if [ $sec_test -lt 600 ]
  then
    my_fun_posts
    let three_test="`date +%s`"-$time_1;
    if [ $three_test -lt 900 ]
    then
      send_mail
    fi
  fi
fi


my_fun_author()
{
export PATH='$PATH:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin';
cd /home/epictions/Phase_02/hackforums/hackforums/spiders;
scrapy crawl hackforums_author --logfile /home/epictions/Phase_02/log_files/hackforums_author.log
}

time_1="`date +%s`";
my_fun_author
time_2="`date +%s`";
let first_test=$time_2-$time_1;
if [ $first_test -lt 300 ]
then
  my_fun_author
  let sec_test="`date +%s`"-$time_1;
  if [ $sec_test -lt 600 ]
  then
    my_fun_author
    let three_test="`date +%s`"-$time_1;
    if [ $three_test -lt 900 ]
    then
      send_mail
    fi
  fi
fi
