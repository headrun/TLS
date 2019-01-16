



URLS = '//td[@class="col_c_forum"]/h4/a/@href'
NAVIGATIONS_LINKS = '//div[@class="topic_controls clearfix"]//li[@class="next"]//@href'
THREAD_URLS = '//td[@class="col_f_content "]//h4//a//@href'

# META DATA
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









