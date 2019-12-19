# Kernel_Threat_xpaths

LINKS = '//a[@class="topictitle"]//@href'
ALL_LINKS = './/dl[@class="postprofile"]//a[contains(@class,"username")]//@href'
NEXT_PAGE = '//li[@class="arrow next"]//a[@class="button button-icon-only"]//@href'
URLS = '//a[@class="forumtitle"]//@href'
AUTHOR_LINKS = '//dl[@class="postprofile"]//a[@class="username-coloured"]//@href'
NODES = '//div[@id="page-body"]//div[contains(@class,"post has-profile")]'
CATEGORY = '//span[@class="crumb"][2]//span[@itemprop="title"]/text()'
SUB_CATEGORY = '//span[@class="crumb"][3]//span[@itemprop="title"]/text()'
POST_TITLE = './/h3//a//text()'
POST_ID = './/div[@class="postbody"]//h3/a/@href'
PUBLISH = './/p[@class="author"]/text()'
AUTHOR = './/span[@class="responsive-hide"]//strong//text()'
ARROW = './/blockquote/div/cite/a[2]/@href'
POST_LINKS = './/div//a[@class="postlink"]/@href'
PAGE_NAVIGATION = '//li[@class="arrow next"]//a[@class="button button-icon-only"]//@href'


# Kernel_author_xpaths

USER_NAME = '//dl[@class="left-box details profile-details"]//dt[contains(text(),"Username:")]/..//dd/span/text()'
AUTHOR_SIGNATURE = '//div[@class="signature standalone"]//text()'
JOINING_DATE = '//dl[@class="details"]//dt[contains(text(),"Joined:")]//following-sibling::dd[1]/text()'
LAST_ACTIVE = '//dl[@class="details"]//dt[contains(text(),"Last active:")]//following-sibling::dd[1]/text()'
TOTAL_POSTS = '//dl[@class="details"]//dt[contains(text(),"Total posts:")]//following-sibling::dd[1]/text()'
GROUPS = '//div[@class="inner"]//dd/select[contains(@name,"g")]//text()'
AUTHOR_RANK = '//dl[@class="left-box details profile-details"]//dt[contains(text(),"Rank:")]/..//dd[2]//text()'
POSTS_COUNT = '//dl[@class="details"]//dt[contains(text(),"Total posts:")]//following-sibling::dd[1]/text()'

