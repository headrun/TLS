

FORUMS = '//h3[@class="node-title"]//a//@href'
THREADURLS = '//h3[@class="node-title"]//a//@href | //div[@class="structItem-cell structItem-cell--main"]//div[@class="structItem-title"]//a/@href'
INNERPAGENAVIGATION = '//a[@class="pageNav-jump pageNav-jump--next"]//@href'
THREADTITLE = '//h1[@class="p-title-value"]//text()'
CATEGORY = '//span[@itemprop="name"]//text()'
SUBCATEGORY = '//span[@itemprop="name"]//text()'
AUTHOR = './/a[@class="username "]//span//text()'
AUTHORURL = './/a[@class="username "]//@href'
POSTURL = './/ul[@class="message-attribution-opposite message-attribution-opposite--list"]//li//a[@data-xf-init="share-tooltip"]/@href'
PUBLISHTIME = './/div[@class="message-attribution-main"]//a//time[@class="u-dt"]/@data-date-string'
POST_TEXT = './/article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//text() | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//a//img[@class="bbImage "]/@src | .//div[@class="message-lastEdit"]//text() | .//h4[@class="block-textHeader"]//text() | .//article[@class="message-body js-selectToQuote"]//ul[@class="attachmentList"]//li//div//a//img//@src | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//blockquote//div[@class="bbCodeBlock-title"]//a//text()'
AUTHOR_SIGNATURE = './/aside[@class="message-signature"]//div[@class="bbWrapper"]//text() | .//aside[@class="message-signature"]//div[@class="bbWrapper"]//div//a//@href | .//aside[@class="message-signature"]//div[@class="bbWrapper"]//a//img[@class="bbImage "]//@src'
ORD_IN_THREAD = './/ul[@class="message-attribution-opposite message-attribution-opposite--list"]//li//a//text()'
LINKS = './/article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//a//img[@class="bbImage "]/@src | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//a//@href'
PAGENAV = '//a[@class="pageNav-jump pageNav-jump--next"]//@href'

#AUTHOR XPATHS

USERNAME = '//h1[@class="memberHeader-name"]//span//text()'
TOTALPOSTS = '//dl[@class="pairs pairs--rows pairs--rows--centered fauxBlockLink"]//dd//a//text()'
JOINDATE = '//dl[@class="pairs pairs--inline"]//dd//time[@class="u-dt"]//@data-date-string'
LASTACTIVE = '//dl[@class="pairs pairs--inline"]//dd//time[@class="u-dt"]//@data-date-string'
GROUPS = '//div[@class="memberHeader-blurb"]//span[@class="userTitle"]//text()'
REPUTATIONS = '//dl[@class="pairs pairs--rows pairs--rows--centered"]//dd//text()'
