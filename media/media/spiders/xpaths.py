#XPATHS FOR CRAWL

MAIN_LINKS = '//h2[@class="ipsType_sectionTitle ipsType_reset cForumTitle"]//a[not(contains(@href,"#"))]/@href'
FORUM_LINKS = '//div[@class="ipsDataItem_main"]//h4[@class="ipsDataItem_title ipsType_large ipsType_break"]//a/@href'
THREAD_URLS = '//span[@class="ipsType_break ipsContained"]//a/@href'


#XPATHS FOR THREADS

THREADTITLE = '//h1[@class="ipsType_pageTitle ipsContained_container"]//text()'
CATEGORY = '//ul[@data-role="breadcrumbList"]//span/text()'
SUBCATEGORY = '//ul[@data-role="breadcrumbList"]//span/text()'
NODES = '//div//article[@class="cPost ipsBox  ipsComment  ipsComment_parent ipsClearfix ipsClear ipsColumns ipsColumns_noSpacing ipsColumns_collapsePhone  "]'
AUTHOR = './/h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//a[@class="ipsType_break"]/text() | .//span[@class="__cf_email__"]/@data-cfemail '
AUTHOR_URL = './/h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//a/@href'
POST_URL = './/div[@class="ipsType_reset"]//a[@class="ipsType_blendLinks"]//@href'
PUBLISHTIME = './/div[@class="ipsType_reset"]//@title'
POST_TEXT = './/div[@class="ipsType_normal ipsType_richText ipsContained"]/text() |.//div[@class="ipsType_normal ipsType_richText ipsContained"]//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p/text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//blockquote//text() |  .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//u//strong//span/text()|.//div[@class="cPost_contentWrap ipsPad"]//ul//li/text() | .//blockquote//div/text() | .//strong//time/text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//strong/text() | .//div[@class="cPost_contentWrap ipsPad"]//img/@alt | .//div[@class="ipsSpoiler"]/@class  | .//div[@class="ipsEmbeddedVideo"]//@src | .//iframe/@src | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//a//iframe/@src  | .//blockquote/@class | .//div[@class="cPost_contentWrap ipsPad"]//@data-ipsquote-timestamp | .//div[@class="cPost_contentWrap ipsPad"]//@data-ipsquote-username | .//p//a[@class="__cf_email__"]//@data-cfemail | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//span[@class="__cf_email__"]//@data-cfemail'
TEXT_DATE = './/div[@class="cPost_contentWrap ipsPad"]//@data-ipsquote-timestamp '
TEXT_AUTHOR = './/div[@class="cPost_contentWrap ipsPad"]//@data-ipsquote-username '
LINKS = './/div[@class="cPost_contentWrap ipsPad"]//p//a/@href | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//strong//a/@href | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//a/@href | .//div[@class="cPost_contentWrap ipsPad"]//img[not(contains(@src,"/emoticons/"))]/@src | .//iframe/@src | .//div[@class="ipsEmbeddedVideo"]//@src | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//p//a//iframe/@src'

#XPATHS FOR AUTHORS

USERNAME = '//h1[@class="ipsType_reset ipsPageHead_barText"]//text()'
TOTALPOSTS = '//ul[@class="ipsList_inline ipsPos_left"]//li//h4//following-sibling::text()'
JOINDATE  = '//li//h4[contains(text(),"Joined")]//following-sibling::time//text()'
LASTACTIVE = '//li//h4[contains(text(),"Last visited")]//following-sibling::span//@datetime'
GROUPS = '//span[@class="ipsPageHead_barText"]//text()'
REPUTATIONS = '//span[@class="cProfileRepScore_points"]//text()'
RANK = '//div[@class="ipsDataItem_generic ipsType_break"]//text()'


