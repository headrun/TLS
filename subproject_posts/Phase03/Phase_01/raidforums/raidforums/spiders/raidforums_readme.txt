
"""For Creating Database"""
CREATE DATABASE `posts` /*!40100 DEFAULT CHARACTER SET latin1 */

"""FOr creating Tables""""

CREATE TABLE `bleeping_posts` (
  `domain` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `crawl_type` varchar(20) COLLATE utf8mb4_bin DEFAULT NULL,
  `category` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `sub_category` varchar(150) COLLATE utf8mb4_bin NOT NULL,
  `thread_title` mediumtext COLLATE utf8mb4_bin,
  `thread_url` mediumtext COLLATE utf8mb4_bin,
  `post_id` varbinary(20) NOT NULL,
  `post_url` mediumtext COLLATE utf8mb4_bin,
  `post_title` mediumtext COLLATE utf8mb4_bin,
  `publish_epoch` bigint(20) DEFAULT NULL,
  `fetch_epoch` bigint(20) DEFAULT NULL,
  `author` mediumtext COLLATE utf8mb4_bin,
  `author_url` mediumtext COLLATE utf8mb4_bin,
  `post_text` mediumtext COLLATE utf8mb4_bin,
  `all_links` mediumtext COLLATE utf8mb4_bin,
  `reference_url` mediumtext COLLATE utf8mb4_bin,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin


CREATE TABLE `bleeping_authors` (
  `user_name` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
  `domain` varchar(150) COLLATE utf8_unicode_ci NOT NULL,
  `crawl_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `author_signature` text COLLATE utf8_unicode_ci,
  `join_date` bigint(20) DEFAULT NULL,
  `last_active` bigint(20) DEFAULT NULL,
  `total_posts` text COLLATE utf8_unicode_ci,
  `fetch_epoch` bigint(20) DEFAULT NULL,
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


CREATE TABLE `bleeping_crawl` (
  `links` text COLLATE utf8_unicode_ci,
  `auth_meta` text COLLATE utf8_unicode_ci,
  `post_id` varchar(100) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`post_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci


