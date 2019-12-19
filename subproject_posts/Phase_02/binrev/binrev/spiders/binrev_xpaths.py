#FORUM_MAIN_URLS =  '//h2[@class="ipsType_sectionTitle ipsType_reset cForumTitle"]//a[contains(@href,"/forum")]//@href'

MAIN_URLS = '//h4[@class="ipsDataItem_title ipsType_large ipsType_break"]/a/@href'
THREAD_URLS = '//div[@class="ipsType_break ipsContained"]//a//@href'
NAVIGATION =  '//li[@class="ipsPagination_next"]//a//@href'
THREAD_TITLE = '//div[@class="ipsType_break ipsContained"]//span//text()' 
NODES =  '//div[@class="ipsAreaBackground_light ipsPad"]//article'
PUBLISH_TIME =  './/p[@class="ipsType_reset"]//time/@title'
POST_URL  =  './/p[@class="ipsType_reset"]//a/@href'
AUTHOR_NAME=  './/strong[@itemprop="name"]//a/text()'

#####TEXT =   './/div[@data-role="commentContent"]//p//text() | .//div[@data-role="commentContent"]//text() | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//img//@alt | .//div[@class="ipsEmbeddedVideo"]//@src | .//blockquote[@class="ipsQuote"]/p/text() |.//blockquote[@class="ipsQuote"]/@class' #| .//blockquote[@class="ipsQuote"]//@data-ipsquote-username | .//blockquote[@class="ipsQuote"]//@data-ipsquote-timestamp'

LINK =  './/div[@data-role="commentContent"]//a//@href | .//div[@class="ipsEmbeddedVideo"]//@src | .//div[@class="ipsType_normal ipsType_richText ipsContained"]//img//@src | .//div[@class="cPost_contentWrap ipsPad"]//img[not(contains (@src ,"/forums/uploads/emoticons/"))]//@src'
AUTHOR_LINK =  './/h3[@class="ipsType_sectionHead cAuthorPane_author ipsType_blendLinks ipsType_break"]//a//@href'

#author
USER_NAME = '//h1[@class="ipsType_reset"]//text()'
TOTAL_POST_COUNT =  '//ul[@class="ipsList_inline ipsPos_left"]//h4[contains(text(),"Content count")]//following-sibling::text()'
JOIN_DATE =  '//ul[@class="ipsList_inline ipsPos_left"]//h4[contains(text(),"Joined")]//following-sibling::time/@title'
LAST_ACTIVE =  '//ul[@class="ipsList_inline ipsPos_left"]//h4[contains(text(),"Last visited")]//following-sibling::span/time/@title'
RANK =  '//div[@class="ipsDataItem_generic ipsType_break"]//text()'

