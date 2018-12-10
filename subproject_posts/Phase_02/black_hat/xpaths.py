

FORUMS = '//div[@class="nodeText"]//h3[@class="nodeTitle"]//a/@href'
THREADURLS = '//div[@class="titleText"]//h3//a[@class="PreviewTooltip"]/@href'
INNERPAGENAVIGATION = '//div[@class="PageNav"]//a[@class="text"]/@href'
THREADTITLE = '//div[@class="titleBar"]//h1/text()'
CATEGORY = '//span[@itemprop="title"]/text()'
SUBCATEGORY = '//span[@itemprop="title"]/text()'
NODES = '//div[@class="pageContent"]//ol[@class="messageList"]//li[@class="message   "]'
AUTHOR = './/a[@class="username"]/span/text() | .//h3[@class="userText"]//a[@class="username"]//text()'
AUTHORURL = './/h3[@class="userText"]//a[@class="username"]/@href | .//h3[@class="userText"]//a[@class="username"]//@href'
POSTURL = './/div[@class="publicControls"]//a[@title="Permalink"]/@href'
POSTID = './/div[@class="publicControls"]//a[@class="item muted postNumber hashPermalink OverlayTrigger"]/@data-href'
PUBLISHTIME = './/div[@class="privateControls"]//span/text() | .//div[@class="privateControls"]//a[@class="datePermalink"]//abbr/@data-datestring'
POST_TEXT = './/div[@class="messageContent"]//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]/text()| .//div[@class="messageContent"]//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//span/text()| .//div[@class="messageContent"]//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//ol//li//text() |.//div[@class="messageContent"]//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a/text()|.//div[@class="messageContent"]//article//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//span//b/text() |.//blockquote[@class="quoteContainer"]/@class | .//div[@class="messageContent"]//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//i/text() | .//div[@class="messageContent"]//div[@class="attribution type"]/text() | .//div[@class="messageContent"]//span/text()|.//div[@class="messageContent"]//h4[@class="attachedFilesHeader"]/text() |.//div[@class="messageContent"]//div[@class="quote"]//text() | .//div[@class="messageContent"]//div[@class="quote"]//span/text() | .//div[@class="messageContent"]//div[@class="quote"]//span//b/text()| .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//b/text() | .//ul[@class="messageNotices"]//li/text() | .//div[@class="messageContent"]//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//img//@alt  |  .//div[@class="messageContent"]//blockquote//img[@class="bbCodeImage LbImage"]//@alt | .//div[@class="editDate"]//text()| .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//div[@style="text-align: center"]/text() |.//div[@class="messageContent"]//div[@class="bbCodeBlock bbCodeCode"]//pre/text() | .//div[@class="messageContent"]//div[@class="bbCodeBlock bbCodeCode"]//div[@class="type"]/text()|.//div[@class="messageContent"]//iframe/@src '

AUTHOR_SIGNATURE = './/div[@class="baseHtml signature messageText ugc"]//text() | .//div[@class="baseHtml signature messageText ugc"]//a/@href | .//div[@class="baseHtml signature ugc"]//img[@class="mceSmilieSprite mceSmilie6"]/@src |.//div[@class="baseHtml signature ugc"]//img[@class="bbCodeImage"]/@src | .//div[@class="baseHtml signature ugc"]//span/text() | .//div[@class="baseHtml signature ugc"]//a//img[@class="bbCodeImage"]/@src | .//div[@class="baseHtml signature ugc"]//a[@class="externalLink"]//img[@class="bbCodeImage"]/@src'

LINKS = './/blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a/@href | .//div[@style="text-align:center"]//img[@class="bbCodeImage LbImage"]/@src |.//div[@class="attachedFiles"]//a/@href | .//div[@class="attribution type"]//a[@class="AttributionLink"]/@href | .//div[@class="quote"]//a/@href| .//div[@class="messageContent"]//blockquote//img[@class="bbCodeImage LbImage"]//@src | .//div[@class="messageContent"]//iframe/@src | .//blockquote[@class="messageText SelectQuoteContainer ugc baseHtml"]//a[@class="externalLink"]/@href'
PAGENAV = '//div[@class="PageNav"]//a[@class="text"]/@href'

#AUTHOR XPATHS

USERNAME = '//h1[@class="username"]//span/text()'
TOTALPOSTS = '//dt[contains(text(),"Messages:")]//..//text()'
JOINDATE = '//dt[contains(text(),"Joined:")]//..//text()'
LASTACTIVE = '//div[@class="secondaryContent pairsJustified"]//span[@class="DateTime"]/text() | //div[@class="secondaryContent pairsJustified"]//abbr[@class="DateTime"]/@data-datestring'
#AUTHOR_SIGNATURE = '//div[@class="baseHtml signature ugc"]//a//text() | //div[@class="baseHtml signature ugc"]//    a//@href | //div[@class="baseHtml signature ugc"]//text() | //div[@class="baseHtml signature ugc"]//img[@class="bbCodeImage"]/@src | //div[    @class="baseHtml signature ugc"]//span/text()'
GROUPS = '//p//span[@class="userTitle"]/text()'
REPUTATIONS = '//dt[contains(text(),"Positive ratings received:")]//..//text()'
FACEBOOK = '//dt[contains(text(),"Facebook:")]//..//text()'
TWITTER = '//dt[contains(text(),"Twitter:")]//..//text()'
YAHOOMESSENGER = '//dt[contains(text(),"Yahoo! Messenger:")]//..//text()'
GOOGLETALK = '//dt[contains(text(),"Google Talk:")]//..//text()'
SKYPEID = '//dt[contains(text(),"Skype:")]//..//text()'
AIM = '//dt[contains(text(),"AIM:")]//..//text()'
ICQ = '//dt[contains(text(),"ICQ:")]//..//text()'

