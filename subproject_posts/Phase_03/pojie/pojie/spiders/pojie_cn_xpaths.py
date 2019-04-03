site_domain = 'https://www.52pojie.cn/'

URLS = '//td[@width="49.9%"]/div/a/@href | //h2[em[@class="xw0 xi1"]]/a/@href'
NXT_PG = '//span[@id="fd_page_top"]//a[@class="nxt"]/@href'

THREAD_URLS = '//h3[@class="xw0"]/a[@title]/@href | //th[@class="new"]/a[contains(@id, "content")]/following-sibling::a[@class]/@href'
NODES = '//table[contains(@id, "pid")]'
SUB_CATE = '//div[@class="z"]//em[2]//following-sibling::a[2]//text()'
CAT = '//div[@class="z"]//em[2]//following-sibling::a[1]//text()'
THREAD_TITLE = '//div[@class="z"]//em[2]//following-sibling::a[3]//text()'
#TEXT = './/div[@class="pct"]//text()'
#TEXT = './/div[@class="pct"]//text() | .//div[@class="pct"]//img/@alt'
#TEXT = './/div[@class="pct"]//text() | .//div[@class="pct"]//img/@alt |  .//div[@class="pct"]//p[@class="attnm"]/a/@href'
#TEXT = './/div[@class="pct"]//text() | .//div[@class="pct"]//img/@alt |  .//div[@class="pct"]//p[@class="attnm"]/a/@href | .//div[@class="pct"]//a[@target="_blank"]/@href'
#TEXT = './/div[@class="pct"]//text() | .//div[@class="pct"]//img/@alt |  \
         #.//div[@class="pct"]//p[@class="attnm"]/a/@href'
#TEXT = './/div[@class="pct"]//text() | .//div[@class="pct"]//img/@alt'
TEXT = './/div[@class="pct"]//text() | .//div[@class="pct"]//img/@alt | .//div[@class="quote"]/@class'
ADD_TEXT = './/div[@class="pct"]//following-sibling::div[@class="cm"]//text() | \
			.//div[@class="pct"]//following-sibling::div[@class="cm"]//a/text() | \
			.//tr[2]//td//div//i//text() | .//tr[2]//td//div//i//@alt'
#LINKS = './/div[@class="pct"]//img/@src | .//div[@class="pct"]//p[@class="attnm"]/a/@href | .//div[@class="pct"]//a[@target="_blank"]/@href'
LINKS = './/div[@class="pct"]//img/@src | .//div[@class="pct"]//p[@class="attnm"]/a/@href | \
		.//div[@class="pct"]//a[@target="_blank"]/@href | \
		.//div[@class="pct"]//a[contains(@class, "xi2")]/@href'			
PUBLISH_TIME = './/em[contains(@id, "authorposton")]//text()'
AUTHOR = './/div[@class="authi"]/a[@target="_blank"]//text()'
AUTHOR_URL  = './/div[@class="authi"]/a[@target="_blank"]//@href'
POST_ID = './/div[@class="pi"]//strong//@id'
POST_URL = './/div[@class="pi"]//strong//@href'
NAV_PG = '//div[@class="pgs mtm mbm cl"]//div[@class="pg"]//a[@class="nxt"]/@href'



