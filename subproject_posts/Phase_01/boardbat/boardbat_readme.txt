1.File Names:

boardbat_thread_v2.py
boardbat_csvfile.py

Run the program as follows:

scrapy crawl boardbat_thread
python boardbat_csvfile.py


3.Table Schema:

CREATE TABLE `boardbat_posts` (
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
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;


