FORUMS_MAIN_URLS = '//span[@class="forumtitle"]//a[contains(@href,"forumdisplay")]//@href'
MAIN_URLS = '//h2[@class="forumtitle"]//a//@href'
THREAD_URLS =  '//h3[@class="threadtitle"]//a//@href'
NAVIGATION =  '//span//a[@rel="next"]//@href' # USE SET
DOMAIN  = 'http://www.antionline.com/forums.php'
#CATEGORY =  response.xpath('//li[@class="navbit"]//a//text()').extract()[1]
#SUBCATEGORY =  response.xpath('//li[@class="navbit"]//a//text()').extract()[2]
THREAD_TITLE =  '//span[@class="threadtitle"]//text()'
PUBLISH_TIME= './/span[@class="date"]//span//..//text()'
POST_URL =  './/div[@class="posthead"]//span//a//@href'
#POST_ID =  POST_URLS.split('=1#post')[-1]
NODES = '//div[@id="postlist"]//li[@class="postbitlegacy postbitim postcontainer old"]'
POST_TITLE = './/h2[@class="title icon"]//text() | .//h2[@class="title icon"]//@alt'

########POST_TEXT = './/blockquote[@class="postcontent restore "]//text() | .//blockquote[@class="postcontent restore "]//img//@title | .//h2[@class="title icon"]//text()' # |. //h2[@class="title icon"]//@alt |  .//div[@class="bbcode_postedby"]//img//@alt'
LINK =  './/blockquote[@class="postcontent restore "]//a//@href | .//div[@class="bbcode_postedby"]//img//@src'

AUTHOR_LINK = './/a[@class="username offline popupctrl"]//@href'
AUTHOR_NAME = './/div[@class="username_container"]//strong//text() | .//span[@class="username guest"]//text()'
#AUTHOR DATA#
USER_NAME = '//div[@class="username_container"]//strong//text() | //span[@class="username guest"]//text()'
#JOIN_DATE = '//dl[@class="userinfo_extra"]//dt[contains(text(), "Join Date")]//following-sibling::dd[1]//text()'
TOTAL_POST_COUNT = '//dl[@class="userinfo_extra"]//dt[contains(text(), "Posts")]//following-sibling::dd[1]//text()'
AUTHOR_SIGNATURE = '//blockquote[@class="signature restore"]//div[@class="signaturecontainer"]//text()'
#LOCATION = '//dl[@class="userinfo_extra"]//dt[contains(text(), "Location")]//following-sibling::dd[1]//text()'

#join_date =  response.xpath('//dl[@class="userinfo_extra"]//dt[contains(text(), "Join Date")]//following-sibling::dd[1]//text()').extract()
#posts =  response.xpath('//dl[@class="userinfo_extra"]//dt[contains(text(), "Posts")]//following-sibling::dd[1]//text()').extract()
#location = response.xpath('//dl[@class="userinfo_extra"]//dt[contains(text(), "Location")]//following-sibling::dd[1]//text()').extract()
