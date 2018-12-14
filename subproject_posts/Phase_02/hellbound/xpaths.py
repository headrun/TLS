CATEGORY = '//h1[@class="smallalt"]//a//text()'
SUB_CATEGORY = '//h1[@class="smallalt"]//a//text()'
THREAD_TITLE =  '//td[contains(text(),"Author")]/..//h2[@class="forum_title"]/text()'
PUBLISH_TIME = '//table[@class="tbl-border"]//td[contains(text(),"Posted on")]/text()'#'//table[@class="tbl-border"]//td[contains(text(),"Author")]/..//h2[@class="forum_title"]/text()'
POST_TITLE = '//table[@class="tbl-border"]//td[contains(text(),"Author")]/..//h2[@class="forum_title"]/text()'
AITHORS_LINKS = '//table[@class="tbl-border"]//a[contains(@href,"../../user/")]/@href'
AUTHOR_NAME_NODES = '//table[@class="tbl-border"]//a[contains(@href,"../../user/")]'
TEXT_NODES = '//table[@class="tbl-border"]//td[@height="170" and @class="tbl1"]'

POST_IDS = '//table[@class="tbl-border"]//td[contains(text(),"Author")]/..//h2[@class="forum_title"]/../a/@id'
NEXT_PAGE = '//td[@class="tbl2"]/a[contains(text(),">")]/@href'
#ALL_LINKS = './/a[@target="_blank"]/@href'

TEXT_HR =  './/hr//preceding-sibling::text() | \
    .//hr//preceding-sibling::img[contains(@src,"planetsmilies")]/@alt |\
    .//hr//preceding-sibling::span//img[contains(@src,"planetsmilies")]/@alt |\
    .//hr//preceding-sibling::img[contains(@src,"images/smiley")]/@alt | \
    .//hr//preceding-sibling::div[@class="quote"]//text() | \
    .//hr//preceding-sibling::div//img[contains(@src,"images/smiley")]/@alt |\
    .//hr//preceding-sibling::div[@class="quote"]//@class | \
    .//span[@class="small" and contains(text(),"dited by")]//text() | \
    .//hr//preceding-sibling::div//div//text() |\
    .//hr//preceding-sibling::span//img[@class="forum-img"]/@alt |\
    .//hr//preceding-sibling::div//a//text() | \
    .//hr//preceding-sibling::a//text() | \
    .//hr//preceding-sibling::span//text() | \
    .//hr//preceding-sibling::span//div[@class="quote"]//@class | \
    .//hr//preceding-sibling::u//text()'


ALL_LINKS_HR = './/hr//preceding-sibling::a[@target="_blank"]/@href | \
    .//hr//preceding-sibling::a[contains(@href,"../user/")]/@href | \
    .//span[@class="small" and contains(text(),"dited by")]//@href |\
    .//hr//preceding-sibling::div//a[@target="_blank"]/@href  '

TEXT = './/text() | .//div[@class="quote"]/@class | .//img[contains(@src,"images/smiley")]/@alt | \
    .//span//img[contains(@src,"planetsmilies")]/@alt | \
    .//img[contains(@src,"planetsmilies")]/@alt |\
    .//img[@class="forum-img"]/@alt'


ALL_LINKS_NO_HR = './/a[@target="_blank"]/@href | .//a[contains(@href,"../user/")]/@href'




AUTHOR_SIGNATURA = '//div[@class="content_warp"]//div[contains(text(),"Signature")]/..//div[@class="content_body"]//text() | \
//div[@class="content_warp"]//div[contains(text(),"Signature")]/..//div[@class="content_body"]//img[not(contains(@src,"s/smiley"))]/@src | \
//div[@class="content_warp"]//div[contains(text(),"Signature")]/..//div//img[class="forum-img"]/@src | \
//div[@class="content_warp"]//div[contains(text(),"Signature")]/..//div//a[@target="_blank"]/@href | \
//div[@class="content_warp"]//div[contains(text(),"Signature")]/..//div[@class="content_body"]//img[contains(@src,"s/smiley")]/@alt'

JOINED_DATE ='//td[contains(text(),"Date Joined:")]/../td[@colspan="3"]//text()'
LAST_ACTIVE = '//td[contains(text(),"Last Visit:")]/../td[@colspan="3"]//text()'
TOTAL_POSTS = '//td[contains(text(),"Forum Posts:")]/following-sibling::td//text()'
GROUP = '//td[contains(text(),"Member Status:")]/following-sibling::td//text()'
REPUTATION = '//td[contains(text(),"Community Points")]/following-sibling::td//text()'
RANK = '//td[contains(text(),"Rank:")]/following-sibling::td//text()'

ICQ = '//td[contains(text(),"ICQ#:")]/following-sibling::td//text()'
MSN = '//td[contains(text(),"MSN ID:")]/following-sibling::td//text()'
YAHOO = '//td[contains(text(),"Yahoo ID:")]/following-sibling::td//text()'
EMAIL = '//td[contains(text(),"Email Address:")]/following-sibling::td//text()'
AIM = '//td[contains(text(),"AIM:")]/following-sibling::td//text()'
WEBSITE = '//td[contains(text(),"Website URL:")]/following-sibling::td//a/@href'
WEBSITE1 = '//td[contains(text(),"Website URL:")]/following-sibling::td//text()'
