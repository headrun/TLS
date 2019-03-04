#BLEEPING_THREAD_CRAWL
URLS = '//td[@class="col_c_forum"]/h4/a/@href'
NAVIGATIONS_LINKS = '//div[@class="topic_controls clearfix"]//li[@class="next"]//@href'
THREAD_URLS = '//td[@class="col_f_content "]//h4//a//@href'

#BLEEPING_POSTS

NEXT_PAGE_LINK =  '//link[@rel="stylesheet"]//following-sibling::link[@rel="next"]/@href'
POST_NODES = '//div[@class="post_wrap"]'
POST_TEXT = './/div[@class="post entry-content "]/text() | .//iframe[@class="EmbeddedVideo"]//@alt | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//img[@class="attach"]//@src |  .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author'
DATA_TIME = './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time'
CATEGORY = '//span[@itemprop="title"]//text()'
SUB_CATEGORY = '//span[@itemprop="title"]//text()'
THREAD_TITLE = '//h1[@class="ipsType_pagetitle"]//text()'
POST_URL = './/span//a[@itemprop="replyToUrl"]//@href'
PUBLISH_TIME = './/div[@class="post_body"]//p[@class="posted_info desc lighter ipsType_small"]//abbr//text()'
AUTHOR = './/span[@itemprop="name"]//text()'
LINKS = './/div[@class="post entry-content "]//a[@class="bbc_url"]//@href | .//iframe[@class="EmbeddedVideo"]//img[@itemprop="image"]//@src | .//img[@class="attach"]//@src | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@src | .//iframe[@class="EmbeddedVideo"]//@src | .//div[@class="post entry-content "]//li[@class="attachment"]//a[img]/@href | .//div[@itemprop="commentText"]//a[@class="bbc_url"]//@href | .//div[@itemprop="commentText"]//a//@href | .//div[@id="gifv"]//video/source/@src'
AUTHOR_LINKS = './/span[@class="author vcard"]//a//@href'
TEXT =  './/div[@class="post entry-content "]//text() | .//div[@class="post entry-content "]//li[@class="attachment"]//a//img//@alt | .//div[@class="post entry-content "]//p//a//@alt | .//div[@class="post entry-content "]//br'
TEXT_HTTP ='.//div[@class="post entry-content "]//text() | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//iframe[@class="EmbeddedVideo"]//@alt | .//img[@class="attach"]//@alt | .//div[@class="post entry-content "]//li[@class="attachment"]//a[img]/@alt | .//div[@class="post entry-content "]//li[@class="attachment"]//a[strong]//text() | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img/@alt '
TEXT_JPG = './/div[@class="post entry-content "]//text() | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//iframe[@class="EmbeddedVideo"]//@alt | .//img[@class="attach"]//@alt | .//div[@class="post entry-content "]//li[@class="attachment"]//a[img]/@href | .//div[@class="post entry-content "]//li[@class="attachment"]//a[strong]//text() | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt '
TEXT_JUNK = './/div[@class="post entry-content "]//p[@style="text-align:center"]//a//img[@class="bbc_img"]/@alt'
LINK =  './/div[@id="gifv"]//video/source/@src'
AUTHOR_QUOTE = './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-author'
DATE_DATE = './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-date'
AUTHOR_DATE_TEXT = './/div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt '
DATE_TIME = './/div[@itemprop="commentText"]//following-sibling::blockquote//@data-time'
TEXT_ = './/div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt '
TEXT_AUTHOR_DATE = './/div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt '
DATE_AUTHOR_TEXT = './/div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt '
FINAL_TEXT = './/div[@class="post entry-content "]//text() | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-date | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-author | .//div[@class="post entry-content "]//span[@rel="lightbox"]//@alt | .//div[@itemprop="commentText"]//following-sibling::blockquote//@data-time | .//div[@class="post entry-content "]//p//img/@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt '

#BLEEPING_AUTHORS

USERNAME = '//h1//span[@class="fn nickname"]//text()'
TOTALPOSTS = '//li[@class="clear clearfix"]//span[contains(text(),"Active Posts")]/../span[@class="row_data"]/text()'
JOINDATE = '//div[@id="user_info_cell"]/text()'
LAST_ACTIVE = '//span[contains(text(),"Last Active")]//text()'
AUTHOR_SIGN  = '//div[@class="signature"]/a//strong//text() | //div[@class="signature"]/a//following-sibling::text() | //div[@class="signature"]//span//text() | //div[@class="signature"]//text() | //div[@class="signature"]//p//img//@src | //div[@class="signature"]//img//@src | //div[@class="signature"]//a/@href'
AUTHOR_SIGN_ = '//div[@class="signature"]/a//strong//text() | //div[@class="signature"]/a//following-sibling::text() | //div[@class="signature"]//span//text() | //div[@class="signature"]//text() | //div[@class="signature"]//p//img//@src | //div[@class="signature"]//a/@href'
GROUPS = '//li[@class="clear clearfix"]//span[contains(text(),"Group")]/..//span[@class="row_data"]//text()'







