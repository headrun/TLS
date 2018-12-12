



#############need to solve emoji issue############################






hellbound_posts_crawl.py  ---> collect all thread_url and store in hellbound_threads_crawl table
hellbound_posts.py        ---> collect all author_url and post_data and store in hellbound_authors_crawl,hellbound_posts tables
hellbound_authors.py      ---> collect all the author data and store in hellbound_authors table
xpaths.py                 ---> contains all the xpthas for hellbound_crawler
my_utils.py               ---> some functions in my_utils file
configuration.py          ---> DB configurations

mysql> show tables;
+-------------------------+
| Tables_in_hellbound     |
+-------------------------+
| hellbound_authors       |
| hellbound_authors_crawl |
| hellbound_posts         |
| hellbound_threads_crawl |
+-------------------------+


CREATE DATABASE `hellbound` /*!40100 DEFAULT CHARACTER SET utf8mb4 */
use hellbound;

CREATE TABLE `hellbound_threads_crawl` (
  `sk` varbinary(50) NOT NULL,
  `thread_url` text COLLATE utf8_unicode_ci NOT NULL,
  `status_code` int(20) NOT NULL,
  `crawl_type` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,
  `reference_url` text COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`sk`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci

CREATE TABLE `hellbound_authors_crawl` (
  `post_id` varbinary(20) NOT NULL,
  `links` text COLLATE utf8_unicode_ci,
  `auth_meta` text COLLATE utf8_unicode_ci,
  `status_code` int(20) NOT NULL,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;



CREATE TABLE `hellbound_posts` (
  `domain` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `crawl_type` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
  `category` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `sub_category` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `thread_title` text COLLATE utf8mb4_bin,
  `thread_url` text COLLATE utf8mb4_bin,
  `post_title` text COLLATE utf8mb4_bin,
  `post_id` varbinary(20) NOT NULL,
  `post_url` text COLLATE utf8mb4_bin,
  `publish_epoch` bigint(20) DEFAULT NULL,
  `fetch_epoch` bigint(20) DEFAULT NULL,
  `author` text COLLATE utf8mb4_bin,
  `author_url` text COLLATE utf8mb4_bin,
  `post_text` text COLLATE utf8mb4_bin,
  `all_links` text COLLATE utf8mb4_bin,
  `reference_url` text COLLATE utf8mb4_bin,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


CREATE TABLE `hellbound_authors_crawl` (
  `post_id` varbinary(20) NOT NULL,
  `links` text COLLATE utf8_unicode_ci,
  `auth_meta` text COLLATE utf8_unicode_ci,
  `status_code` int(20) NOT NULL,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

CREATE TABLE `hellbound_posts` (
  `domain` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `crawl_type` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
  `category` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `sub_category` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `thread_title` text COLLATE utf8mb4_bin,
  `thread_url` text COLLATE utf8mb4_bin,
  `post_title` text COLLATE utf8mb4_bin,
  `post_id` varbinary(20) NOT NULL,
  `post_url` text COLLATE utf8mb4_bin,
  `publish_epoch` bigint(20) DEFAULT NULL,
  `fetch_epoch` bigint(20) DEFAULT NULL,
  `author` text COLLATE utf8mb4_bin,
  `author_url` text COLLATE utf8mb4_bin,
  `post_text` text COLLATE utf8mb4_bin,
  `all_links` text COLLATE utf8mb4_bin,
  `reference_url` text COLLATE utf8mb4_bin,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;

CREATE TABLE `hellbound_authors` (
  `user_name` varchar(250) COLLATE utf8_unicode_ci NOT NULL,
  `domain` varchar(150) COLLATE utf8_unicode_ci DEFAULT 'https://www.hellboundhackers.org',
  `crawl_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT 'keep up',
  `author_signature` text COLLATE utf8_unicode_ci,
  `join_date` bigint(20) DEFAULT NULL,
  `last_active` bigint(20) DEFAULT NULL,
  `total_posts` text COLLATE utf8_unicode_ci,
  `fetch_time` bigint(20) DEFAULT NULL,
  `groups` text COLLATE utf8_unicode_ci,
  `reputation` text COLLATE utf8_unicode_ci,
  `credits` text COLLATE utf8_unicode_ci,
  `awards` text COLLATE utf8_unicode_ci,
  `rank` text COLLATE utf8_unicode_ci,
  `active_time` text COLLATE utf8_unicode_ci,
  `contact_info` text COLLATE utf8_unicode_ci,
  `reference_url` text COLLATE utf8_unicode_ci,
  PRIMARY KEY (`user_name`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
