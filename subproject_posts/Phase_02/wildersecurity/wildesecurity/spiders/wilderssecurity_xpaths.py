
URLS = '//h3[@class="nodeTitle"]//a[contains(@data-description, "#nodeDescription-")]/@href | //h3[@class="nodeTitle"]//a[contains(@data-description-x, "#nodeDescription-")]/@href'
NAVIGATION = '//link[@rel="next"]//@href'
CATEGORY = '//span[2]//a[@class="crumb"]//text()'
SUBCATEGORY = '//span[3]//a[@class="crumb"]//text()'
THREAD_TITLE = '//div[@class="titleBar"]//h1//text()'
NODES = '//li[contains(@id, "post-")]'
TEXT = './/blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//div[@class="attribution type"]//a//text() | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//text() | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img//@alt | .//div[@class="attachedFiles"]//text() | .//aside//div[@class="attribution type"]//text()'


AUTHOR_LINK = './/h3[@class="userText"]/a[@class="username"]//@href'
AUTHOR = './/h3[@class="userText"]/a[@class="username"]//text()'
PUBLISH_TIME = './/a//span[@class="DateTime"]//@title'
POSTURL = './/div[@class="publicControls"]//a//@href'
JOINDATE = './/dl[@class="pairsJustified"]//dt[contains(text(), "Joined:")]//following-sibling::dd//text()'
GROUPS = './/em[@class="userTitle"]//text()'


site_domain = 'https://www.wilderssecurity.com/'


