#bbs_thread_crawl
FORUMS = '//div[contains(@class,"row bbs_home_page_contant")]//a[@class="font-weight-bold"]/@href'
THREAD = '//table[@class="table threadlist"]//tr[contains(@class,"thread")]//a[@target="_blank"]/@href'

        
NEXT_PAGE = '//li[@class="page-item"]//a[contains(text(),"%s")]/@href'% u'\u25b6'
#bbs_posts
CATEGORY = '//ol[@class="breadcrumb mb-2 hidden-md-down"]//i[@class="icon-home"]/..//text()'
SUB_CATEGORY = '//ol[@class="breadcrumb mb-2 hidden-md-down"]//a[contains(@href,"forum")]/..//text()'
SUBCATEGORY_URL = '//ol[@class="breadcrumb mb-2 hidden-md-down"]//a[contains(@href,"forum")]/..//@href'
THREAD_TITLE = '//div[@class="card"]//div[@class="card-body"]//h3[@class="break-all subject"]//text()'
POST_TEXT = './/div[@class="message mt-1 break-all"]//text() | \
        .//div[@class="message mt-1 break-all"]//a[@target="_blank"]/@href | \
        .//div[@class="message mt-1 break-all"]//blockquote[@class="blockquote"]/@class | \
        .//div[@class="message mt-1 break-all"]//blockquote[@class="blockquote"]//text()'
NODES = '//div[@class="card p-1"]//div[@class="card-body"]//tr[@class="post"]'
PUBLISH_EPOCH = './/span[@class="date text-grey ml-2"]//text() | .//div[@class="card"]//div[@class="card-body"]//dl[@class="row small"]//span[@class="date text-grey ml-1"]//text() | .//span[@class="date text-grey small ml-1"]//text()'
AUTHOR = './/span[@class="username font-weight-bold"]/a/text()'
POST_URL = './/dd[@class="text-right text-grey py-1"]//a[@class="text-grey mr-3"]//@href'
AUTHOR_URL = './/span[@class="username font-weight-bold"]/a/@href'
ALL_LINKS =  './/div[@class="message mt-1 break-all"]//p//img[contains(@src,"upload/attach")]/@src |\
        .//div[@class="message mt-1 break-all"]//a[@target="_blank"]/@href | \
        .//div[@class="message mt-1 break-all"]//img[contains(@src,"upload/attach")]/@src'


A_URL = '//div[@class="card"]//div[@class="card-body"]//div[@class="avatar_info"]/a/@href'
ALL_LINKS1 = '//div[@class="card"]//div[@class="card-body"]//div[@class="message break-all"]//a[@target="_blank"]/@href | \
        //div[@class="card"]//div[@class="card-body"]//div[@class="message break-all"]//img[contains(@src,"upload/attach")]/@src '
A_NAME = '//div[@class="card"]//div[@class="card-body"]//dl[@class="row small"]//span[@class="username font-weight-bold"]//text()'
TEXT = '//div[@class="card"]//div[@class="card-body"]//div[@class="message break-all"]//text() | \
        //div[@class="card"]//div[@class="card-body"]//div[@class="message break-all"]//a[@target="_blank"]/@href '
P_EPOCH = '//div[@class="card"]//div[@class="card-body"]//dl[@class="row small"]//span[@class="date text-grey ml-1"]//text()'


