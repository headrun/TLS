#THREAD  XPATHS

DOMAIN = "www.hackthissite.org"
FORUMS = '//a[@class="forumtitle"]/@href'
THREADURLS = '//a[@class="topictitle"]/@href'
THREADTITLE = '//div[@id="page-body"]//h2//a/text()'
CATEGORY = '//li[@class="icon-home"]//a/text()'
SUBCATEGORY = '//li[@class="icon-home"]//a/text()'
NODES = '//div[contains(@class,"post bg")]'
POSTTITLE = './/h3//a/text()'
AUTHOR = './/p[@class="author"]//a/text()'
AUTHOR_URL = './/p[@class="author"]//strong//a/@href | .//p[@class="author"]//strong//a[@class="username-coloured"]/@href'
POSTID = './/h3//a[contains(@href,"#p")]/@href'
POSTURL = './/h3//a[contains(@href,"#p")]/@href'
PUBLISHTIMES = './/p[@class="author"]//strong/following-sibling::text()'
POST_TEXT = './/div[@class="content"]//text() | .//div[@class="notice"]//text() | .//div[@class="content"]//span//text() | .//div[@class="content"]//img/@alt | .//div[@class="content"]//blockquote/@class'
LINKS = './/div[@class="content"]//a[@class="postlink"]/@href | .//div[@class="content"]//a[@class="postlink-local"]/@href | .//div[@class="content"]//a/@href | .//div[@class="content"]//img[not(contains(@src,"/smilies/"))]/@src | .//div[@class="notice"]//a/@href '
PAGE_NAVIGATION = '//div[@class="topic-actions"]//div[@class="pagination"]//a/@href'

#AUTHOR XPATHS

USERNAME = '//dd//span/text()'
TOTALPOSTS = '//dl//dt[contains(text(),"Total posts:")]//following-sibling::dd[1]/text()'
JOINDATES = '//dl//dt[contains(text(),"Joined:")]//following-sibling::dd[1]/text()'
LASTACTIVES = '//dl//dt[contains(text(),"Last visited:")]//following-sibling::dd[1]/text()'
AUTHOR_SIGNATURE = '//div[@class="signature"]//text() | //div[@class="signature"]//img[not(contains(@src,"/images/smilies/"))]/@src | //div[@class="signature"]//a[@class="postlink"]/@href | //div[@class="signature"]//img[contains(@src,"/images/smilies/")]//@alt'
GROUPS = '//dd//select[@name="g"]//text()'
RANK = '//dt[contains(text(),"Rank:")]//following-sibling::dd[1]/text()'
EMAILADDRESS = '//dt[contains(text(),"E-mail address:")]//following-sibling::dd[1]//text()'
WEBSITES = '//dt[contains(text(),"Website:")]//following-sibling::dd[1]//text()'
MSNM = '//dt[contains(text(),"MSNM/WLM:")]//following-sibling::dd[1]//text()'
YIM = '//dt[contains(text(),"YIM:")]//following-sibling::dd[1]//text()'
AIM = '//dt[contains(text(),"AIM:")]//following-sibling::dd[1]//text()'
REPUTATIONS = '//dd//img/@src'

