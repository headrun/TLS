#CRAWL XPATHS

FORUMS = '//h3[@class="node-title"]//a/@href'
THREADS='//div[@class="structItem-title"]//a[contains(@href,"threads")]/@href'
PAGENAV='//div[@class="pageNav  pageNav--skipEnd"]//a[@class="pageNav-jump pageNav-jump--next"]/@href'

#POSTS XPATHS

THREADTITLE = '//h1[@class="p-title-value"]//span[@class="label-append"]//following-sibling::text() | //h1[@class="p-title-value"]/text()'
CATEGORY = '//div[@class="p-breadcrumbs--parent"]//span[@itemprop="name"]/text()'
SUBCATEGORY = '//div[@class="p-breadcrumbs--parent"]//span[@itemprop="name"]/text()'
NODES = '//div[@class="block-container lbContainer"]//article[contains(@id,"js-post-")]'
POST_ID = './/div[@class="message-userContent lbContainer js-lbContainer "]//@data-lb-id'
POST_URL = './/div[@class="message-attribution-opposite"]//a//@href'
PUBLISHTIME = './/div[@class="message-main js-quickEditTarget"]//a[@class="message-attribution-main u-concealed"]//@data-time | .//div[@class="message-main js-quickEditTarget"]//div[not(contains(@class,"message-lastEdit"))]/@data-time | .//time[@class="u-dt"]/@data-time'
TEXT = './/article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//text() | .//div[@class="message-lastEdit"]//text() | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//img[@class="smilie"]//@alt | .//article[@class="message-body js-selectToQuote"]//img[@class="bbImage"]/@alt | .//div[contains(@class,"bbCodeBlock-expandContent")]/@class | .//article[@class="message-body js-selectToQuote"]//div[@class="bbMediaWrapper"]//iframe/@src '
AUTHOR = './/h4[@class="message-name"]//a//span//text() | .//h4[@class="message-name"]//span/text()'
AUTHOR_URL = './/h4[@class="message-name"]//a[@class="username "]/@href'
LINKS = './/article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//a/@href | .//article[@class="message-body js-selectToQuote"]//div[@class="bbWrapper"]//img[not(contains(@src,"/styles/images"))]/@src | .//article[@class="message-body js-selectToQuote"]//div[@class="bbMediaWrapper"]//iframe/@src'
INNERPAGENAV='//div[@class="pageNav  "]//a[@class="pageNav-jump pageNav-jump--next"]/@href'

#AUTHOR XPATHS

USERNAME = '//h1[@class="memberHeader-name"]//span/text()'
GROUPS = '//div[@class="memberHeader-blurb"]//span/text()  | //div[@class="memberHeader-blurb"]//text()'
LASTACTIVE = '//dl[@class="pairs pairs--inline memberHeader-blurb"]//@data-time'
JOINDATE = '//dl[@class="pairs pairs--rows pairs--rows--centered"]//@data-time'
CREDITS = '//dt[@title="Points"]//following-sibling::dd//a/text()'
RANK = '//em[@class="userBanner userBanner bannerVIP"]//text() | //em[@class="userBanner userBanner bannerMaitre"]//strong/text()'
TOTALPOSTS = '//dt[contains(text(),"Messages")]//following-sibling::dd/a/text()'

