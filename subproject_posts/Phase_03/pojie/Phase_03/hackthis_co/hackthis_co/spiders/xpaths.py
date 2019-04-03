#XPATHS FOR CRAWL

FORUMS = '//ul[@class="sections"]//li//a[not(contains(@href,"/level"))]/@href'
THREADURL = '//div[@class="section_info col span_16"]//a[@class="strong"]/@href'
PAGENAV = '//li[@class="right"]//a[@rel="next"]/@href'


#XPATHS FOR THREADS

THREADTITLE = '//h1[@itemprop="name"]/text()'
CATEGORY = '//div[@class="col span_18 forum-main"]//a/text()'
SUBCATEGORY = '//div[@class="col span_18 forum-main"]//a/text()'
NODES = '//li[@class="row clr  "]'
POSTID = './/@data-id'
AUTHOR = './/a[@itemprop="author"]/text() | .//div[@class="col span_5 post_header"]//a/text()'
AUTHORURL = './/a[@itemprop="author"]/@href | .//div[@class="col span_5 post_header"]//a/@href'
PUBLISHTIME = './/time[@itemprop="datePublished"]/@datetime | .//li[@class="highlight"]//@datetime '
TEXTS = './/div[@class="post_body"]//text() | .//div[@class="post_body"]//img[@class="bbcode_img"]/@alt'
TEXTSIGN = './/div[@class="post_signature"]//text() | .//div[@class="post_signature"]//img[@class="bbcode_img"]/@alt '
LINKS = './/div[@class="post_body"]//img[@class="bbcode_img"][not(contains(@src,"gif"))]/@src | .//div[@class="post_body"]//a[@class="bbcode_url"]/@href | .//div[@class="post_body"]//a[not(contains(@href,"gif"))]/@href'
LINKS_SIGN = './/div[@class="post_signature"]//a[@class="bbcode_url"]/@href | .//div[@class="post_signature"]//a[not(contains(@href,"gif"))]/@href'
PAGENAV = '//li[@class="right"]//a[@rel="next"]/@href'

