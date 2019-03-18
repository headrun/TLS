

THREAD_LINKS = '//div[@class="ExploitTableContent"]//h3//a//@href'

## POSTS ##

COMMENT_AUTHOR_NAME =  './/div[@class="support_title"]//text()'
COMMENT_PUBLISH_TIME = './/div[@class="support_date"]//text()'
COMMENT_TEXT = './/div[@class="support_text"]//text()'

AUTHOR_LINK = '//div[contains(text(), "Author")]//following-sibling::div//a//@href'

## POSTS_BOXDATA ##

FULL_TITLE = '//div[contains(text(), "Full title")]//following-sibling::div//text()'
CATEGORY = '//div[contains(text(), "Category")]//following-sibling::div//text()'
DATE_ADD = '//div[contains(text(), "Date add")]//following-sibling::div//a//text()'
PLATFORM = '//div[contains(text(), "Platform")]//following-sibling::div//a//text()'
PRICE = '//div[@class="GoldText"] [ contains(text(),"BTC")]/text() | //div[@class="td"] [contains(text(),"free")]/text() '
DESCRIPTION = '//div[contains(text(), "Description")]//following-sibling::div//text()'
CVE_XPATH = '//div[contains(text(), "CVE")]//following-sibling::div//text()'
ABUSES = '//div[contains(text(), "Abuses")]//following-sibling::div//text()'
COMMENTS = '//div[contains(text(), "Comments")]//following-sibling::div//text()'
VIEWS = '//div[contains(text(), "Views")]//following-sibling::div//text()'
REL_REALEASES = '//div[contains(text(), "Rel. releases")]//following-sibling::div//text()'
RISKS = '//div[contains(text(), "Risk")]//following-sibling::div//img//@src | //div[contains(text(), "Risk")]//following-sibling::div//text()'
VERIFIED = '//div[contains(text(), "Verified")]//following-sibling::div//img//@src'
VENDOR_LINK = '//div[contains(text(), "Vendor")]//following-sibling::div//text()'
AFFECTED_VER = '//div[contains(text(), "Affected ver")]//following-sibling::div//text()'
TESTED_ON = '//div[contains(text(), "Tested on")]//following-sibling::div//text()'
TAGS = '//div[contains(text(), "Tags")]//following-sibling::div//text()'
VEDIO_PROOF = '//div[contains(text(), "Author")]//following-sibling::div//a//@href'




## AUTHORS ##
USER_NAME = '//div[contains(text(), "Author")]//following-sibling::div//text()'
JOIN_DATE = '//div[contains(text(), "Reg date")]//following-sibling::div//text()'
BL_XPATH = '//div[contains(text(), "BL")]//following-sibling::div//text()'
EXPLOITS = '//div[contains(text(), "Exploits")]//following-sibling::div//text()'
READERS =  '//div[contains(text(), "Readers")]//following-sibling::div//text()'

