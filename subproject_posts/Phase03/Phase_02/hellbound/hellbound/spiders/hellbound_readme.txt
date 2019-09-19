scrapy runspider hellbound_posts_crawl.py 

---> collect all thread_url and store in hellbound_threads_crawl table

scrapy runspider hellbound_posts.py  

---> collect all author_url and post_data and store in hellbound_authors_crawl,hellbound_posts tables

scrapy runspider hellbound_authors.py

---> collect all the author data and store in hellbound_authors table

create databse hellbound;


CREATE TABLE `hellbound_authors` (
  `user_name` varchar(250) COLLATE utf8_unicode_ci NOT NULL,
  `domain` varchar(150) COLLATE utf8_unicode_ci DEFAULT 'https://www.hellboundhackers.org',
  `crawl_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT 'keep up',
  `author_signature` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `join_date` bigint(20) DEFAULT NULL,
  `last_active` bigint(20) DEFAULT NULL,
  `total_posts` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `fetch_time` bigint(20) DEFAULT NULL,
  `groups` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `reputation` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `credits` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `awards` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `rank` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `active_time` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `contact_info` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `reference_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_name`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;


CREATE TABLE `hellbound_posts` (
  `domain` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `crawl_type` varchar(20) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL,
  `category` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `sub_category` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  `thread_title` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `thread_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `post_title` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `post_id` int(32) NOT NULL,
  `post_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `publish_epoch` bigint(20) DEFAULT NULL,
  `fetch_epoch` bigint(20) DEFAULT NULL,
  `author` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `author_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `post_text` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `all_links` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `reference_url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin,
  `created_at` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `modified_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

EATE TABLE `hellbound_authors_crawl` (
  `post_id` varbinary(20) NOT NULL,
  `links` text COLLATE utf8_unicode_ci,
  `auth_meta` text COLLATE utf8_unicode_ci,
  `crawl_status` int(20) NOT NULL,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

CREATE TABLE `hellbound_threads_crawl` (
  `sk` varbinary(150) NOT NULL,
  `post_url` text COLLATE utf8_unicode_ci NOT NULL,
  `crawl_status` int(20) NOT NULL,
  `reference_url` text COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`sk`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;




