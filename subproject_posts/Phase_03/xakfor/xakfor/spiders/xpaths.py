TEXT = './/span[@class="__cf_email__"]//@data-cfemail | \
        .//div[@class="messageContent"]//text() | \
        .//div[@class="messageContent"]//img[@class="bbCodeImage LbImage"]/@alt |  \
        .//div[@class="messageContent"]//div[@class="quote"]/@class |  \
        .//div[@class="messageContent"]//div[@class="bbCodeBlock bbCodeCode"]/@class | \
        .//div[@class="messageInfo primaryContent"]//div[contains(@id,"likes-post-")]//text() | \
        .//div[@class="messageContent"]//img[@class="mceSmilie"]/@alt | \
        .//div[@class="messageContent"]//iframe/@src'

ALL_LINKS = './/div[@class="messageInfo primaryContent"]//a[@target="_blank"]/@href | \
        .//div[@class="messageInfo primaryContent"]//a[@class="OverlayTrigger item control reputation"]/@href | \
        .//div[@class="messageInfo primaryContent"]//img[@class="bbCodeImage LbImage"]/@src | \
        .//div[@class="messageInfo primaryContent"]//a[@class="AttributionLink"]/@href | \
        .//div[@class="messageInfo primaryContent"]//div[contains(@id,"likes-post-")]//@href | \
        .//div[@class="messageContent"]//iframe/@src '

CFEMAIL = './/span[@class="__cf_email__"]//@data-cfemail'

NEXT_PAGE = '//div[@class="PageNav"]//a[contains(text()," >")]/@href'
#POST_URL = './/div[@class="publicControls"]//a[contains(text(),"#")]/@href'
POST_ID =  './/div[@class="category_content"]//li[contains(@id,"post-")]/@id'
POST_URL = './/div[@class="category_content"]//a[@class="item muted postNumber hashPermalink OverlayTrigger"]/@href'
#POST_ID = './/div[@class="category_content"]//li[contains(@id,"post-")]/@id'
AUTHOR_URL = './/h3[@class="userText"]//a[@class="username"]/@href'
AUTHOR_NAME = './/h3[@class="userText"]//a[@class="username"]//text()'
PUBLISH_1 = './/span[@class="item muted"]//span[@class="DateTime"]//text()'
PUBLISH_2 = './/span[@class="item muted"]//abbr[@class="DateTime"]/@data-datestring'


def ressian_date_to_eng(date_):
    date_1 = date_
    date_ = date_.replace(u' \u044f\u043d\u0432 ', ' Jan ').replace(u'\u0434\u0435\u043a', 'Dec')\
        .replace(u'\u0438\u044e\u043d', 'Jun').replace(u'\u043c\u0430\u0440', 'Mar').replace(u'\u043d\u043e\u044f', 'Nov')\
        .replace(u'\u043e\u043a\u0442', 'Oct').replace(u'\u0430\u0432\u0433', 'Aug').replace(u'\u0444\u0435\u0432', 'Feb')\
        .replace(u'\u0430\u043f\u0440', 'Apr').replace(u'\u043c\u0430\u0439', 'May').replace(u'\u0438\u044e\u043b', 'Jul')\
        .replace(u'\u0441\u0435\u043d', 'Sep')
    return date_
