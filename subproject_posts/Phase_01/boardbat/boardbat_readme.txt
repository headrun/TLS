1.File Names:

boardbat_thread.py
boardbat_csvfile.py

Run the program as follows:

scrapy crawl boardbat_thread
python boardbat_csvfile.py


3.Table Schema:
create database `posts_bb` DEFAULT CHARACTER SET latin1 ;

CREATE TABLE `boardbat_posts` (
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
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
