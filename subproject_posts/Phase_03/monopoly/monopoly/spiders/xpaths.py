
#posts

FORUMS = '//tr[@align="center"]//td[@class="alt1Active"]/div/a/@href'
THREAD_URLS = '//div//a[contains(@id,"thread_title")]//@href'
INNER_THREADS = '//table[@class="tborder"]//td[@class="alt1"]//a[@rel="next"]//@href'
PAGENAV = '//table[@class="tborder"]//td[@class="alt1"]//a[@rel="next"]//@href'
CATEGORY = '//span[@class="navbar"]/a/text()'
SUB_CATEGORY = '//span[@class="navbar"]/a/text()'
THREAD_TITLE = '//tr/td[@class="navbar"]//strong/text()'
NODES = '//div[@id="posts"]//div[@class="spacer_open"]'
AUTHOR = './/div[contains(@id,"postmenu2")]//a[@class="bigusername"]//text() | .//div[@class="reposition0"]/a[@class="bigusername"]/span/text()| .//div[@class="smallfont reposition1"]/text() '
AUTHOR_URL = './/div[contains(@id,"postmenu2")]//a[@class="bigusername"]/@href '
POST_URL = './/td[@class="alt2 postcount"]/a/@href'
PUBLISH_TIME = './/td[@class="alt2 block2"]//text()'
TEXT = './/div[@class="bbcodestyle"]//div[@class="smallfont"]/text() | .//div[@class="postbitcontrol2"]//a//font/text() | .//div[@class="postbitcontrol2"]/b//a/text() | .//div[@class="postbitcontrol2"]//font/text() | .//td[@class="alt2 converttodiv"]//font/text() | .//div[@class="postbitcontrol2"]/b/font/text() | .//div[@class="postbitcontrol2"]/font/text() | .//div[@class="postbitcontrol2"]/font/text() | .//div[@style="font-style:italic"]/text() | .//td[@style="border:1px inset"]/div//strong/text() | .//em/text() | .//em/span[@class="time"]/text() | .//td[@style="border:1px inset"]/div/text() | .//div[@class="postbitcontrol2"]/a/img/@alt | .//div[@align="left"]//b//span/text() | .//b/font[@color="#FF0000"]/span/text() | .//div[@class="postbitcontrol2"]//b/text() | .//div[@class="postbitcontrol2"]/text() | .//div[@align="right"]/i/text() | .//div[@class="postbitcontrol2"]/img/@title | .//div[@class="postbitcontrol2"]/font/img/@title | .//div[@class="postbitcontrol2"]/a/text() | .//div[@class="postbitcontrol2"]/b/span/text() | .//div[@class="postbitcontrol2"]/fieldset/legend/text() | .//table//td/img[@class="inlineimg"]/@alt | .//div[@style="font-style:italic"]/font/text() | .//td/img[@class="inlineimg"]//@title | .//div[@class="postbitcontrol2"]/span/text() | .//table//td/a/text() | .//div[@style="font-style:italic"]/img/@title | .//div[@class="postbitcontrol2"]//ul//li/a/text() | .//div[@class="postbitcontrol2"]/i/text() | .//div[@class="postbitcontrol2"]/span/b/font/a/text() | .//div[@class="postbitcontrol2"]/i/font/text() |.//td[@class="alt1"]//div[@class="postbitcontrol2"]//div[@align="center"]//text() | .//div[@align="right"]/a/img//@alt | .//div[@align="center"]/img/@alt'
LINKS = './/div[@class="postbitcontrol2"]//a/@href | .//div[@class="postbitcontrol2"]/td//a/@href | .//div[@class="postbitcontrol2"]//table/td/img[@class="inlineimg"]/@src | .//div[@align="center"]/img/@src | .//div[@align="left"]/a/@href |.//td/div/a/img[@class="inlineimg"]/@src'


