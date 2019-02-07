MAIN_URLS  = '//td[contains(@class,"trow")]//strong//a//@href'

START_URLS = '//td[contains(@class,"trow")]//strong//a//@href'

THREAD_URLS =  '//div//span[contains(@id, "tid")]//a//@href'
NAVIGATION  = '//a[@class="pagination_next"]//@href'
#category = response.xpath('//div[@class="navigation"]//a//text()').extract()[1]
#sub_category = response.xpath('//div[@class="navigation"]//a//text()').extract()[2]
POST_TITLE = '//div[@class="navigation"]//span[@class="active"]//text()'

#node = response.xpath('//div[contains(@class, "post")]')

NODES= '//div[@class="post classic "]'
#AUTHOR_NAME = set(node.xpath('.//span[contains(@class,"largetext")]//text()').extract())
AUTHOR_LINK =  '//div[@class="author_avatar"]//a//@href'
#THREAD_TITLE =  response.xpath('//td[@class="thead"]//div//strong//text()').extract()[1]

POST_URL = './/div[contains(@class, "float")]//strong//a//@href'
#POST_ID = ''.join(re.findall('#pid\d+',a)).replace('#pid','')
#(or)post_url.split('.html#pid')[-1]


PUBLISH_TIME = './/span[@class="post_date"]//text() | .//span[@class="post_date"]/text()'
#TEXT=
LINK = './/div[@class="post_body scaleimages"]//img[not(contains(@src ,"images/smilies"))]//@src | .//div[@class="post_body scaleimages"]//a//@href |.//div[@class="post_body scaleimages"]//following-sibling::fieldset//a//@href | .//div[@class="post_content"]//div[@class="mycode_align"]//a//@href | .//div[@class="post_body scaleimages"]//..//img[not(contains(@src ,"images/smilies"))]//@src'
#'.//div[@class="post_body scaleimages"]//img//@src'

##Authors##

USERNAME =  './/span[contains(@class,"largetext")]//text()'
AUTHOR_SIGNATURE = '//td[strong[contains(., "%s")]]/following::td//text()'%u'\u062a\u0648\u0642\u064a'
TOTAL_POSTS = '//strong[contains(text(), "%s")]/../../td/text()' % u'\u0625\u062c\u0645\u0627\u0644\u064a \u0627\u0644\u0645\u0634\u0627\u0631\u0643\u0627\u062a :'

GROUPS = '//span[@class="smalltext"]//text()'
RANK = '//span[@class="smalltext"]//@alt'
JOIN_DATES = '//strong[contains(text(), "%s")]/../../td/text()' % u'\u0625\u0646\u0636\u0645 \u0625\u0644\u064a\u0646\u0627 :'
LAST_ACTIVE = '//strong[contains(text(), "%s")]/../../td/text()' % u'\u0622\u062e\u0631 \u0632\u064a\u0627\u0631\u0629'


