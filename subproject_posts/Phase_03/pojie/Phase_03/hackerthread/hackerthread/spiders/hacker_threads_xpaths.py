CATEGORY = '//span[@class="crumb"]//a//text()'
site_domain = "https://www.hackerthreads.org/"
THREAD_TITLE = '//h2[@class="topic-title"]//text()'
#TEXT = './/div[@class="content"]//text()'
#TEXT = './/div[@class="content"]//text() | .//img[@class="postimage"]/@alt | .//img[@class="smilies"]/@alt'
TEXT = './/div[@class="content"]//text() | .//div[@class="content"]//img[@class="postimage"]/@alt | .//div[@class="content"]//img[@class="smilies"]/@alt | .//div[@class="content"]//following-sibling::div[@class="notice"]//text() | .//div[@class="content"]//following-sibling::dl[@class="attachbox"]//text()'
#AUTHOR = './/span[@class="responsive-hide"]//a[@class="username-coloured"]//text()'
AUTHOR = './/p[@class="author"]//a[contains(@class, "username")]//text() | .//dt[@class="no-profile-rank no-avatar"]//strong//span[@class="username"]/text()'
PUBLISH_TIME = './/span[@class="responsive-hide"]/following-sibling::text()'
POST_URL = './/h3//a//@href'
#POST_TITLE = './/h3[@class="first"]//a//text()'
POST_TITLE = './/div[contains(@id, "p")]//h3/a/text()'
#AUTHOR_LINK = './/span[@class="responsive-hide"]//a[@class="username-coloured"]/@href'
AUTHOR_LINK = './/p[@class="author"]//a[contains(@class, "username")]//@href'
#POST_ID = './/h3[@class="first"]//a/@href'
POST_ID = './/div[contains(@id, "p")]//h3/a/@href'
POST_URL = './/div[contains(@id, "p")]//h3/a/@href'
TOTAL_POSTS = './/dd[@class="profile-posts"]/strong/following-sibling::text()'
JOIN_DATE = './/dd[@class="profile-joined"]/strong/following-sibling::text()'
GROUP = './/dt/following-sibling::dd[@class="profile-rank"]//text()'





