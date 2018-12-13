scrapy runspider wilderssecurity_crawl.py  
--> to get all threads and dump to DB with 0 status

scrapy runspider wilderssecurity.py

--> to get all posts and dumpt to db

scrapy runspider wilderssecurity_author.py 

--> to get author details

CREATE DATABASE `POSTS_WILDER` /*!40100 DEFAULT CHARACTER SET latin1 */ 



CREATE TABLE `wilder_posts` (
  `domain` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `crawl_type` varchar(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci DEFAULT NULL,
  `category` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `sub_category` varchar(150) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL,
  `thread_title` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `thread_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `post_id` varbinary(20) NOT NULL,
  `post_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `publish_epoch` bigint(20) DEFAULT NULL,
  `fetch_epoch` bigint(20) DEFAULT NULL,
  `author` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `author_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `post_title` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `post_text` text COLLATE utf8mb4_bin,
  `all_links` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  `reference_url` text CHARACTER SET utf8 COLLATE utf8_unicode_ci,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin 

CREATE TABLE `wilder_authors` (
  `user_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
  `domain` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
  `crawl_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
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
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci




CREATE TABLE `wilder_crawl` (
  `links` text COLLATE utf8_unicode_ci,
  `post_id` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  `auth_meta` text COLLATE utf8_unicode_ci,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci 



CREATE TABLE `wilder_status` (
  `post_id` varchar(150) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `links` text COLLATE utf8_unicode_ci,
  `crawl_status` int(11) DEFAULT '0',
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
