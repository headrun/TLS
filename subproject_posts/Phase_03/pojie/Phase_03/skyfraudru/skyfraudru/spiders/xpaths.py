
#Posts

LINKS = '//td[@class="alt2Active"]/div/h3/a/@href'
URLS = '//td[@class="alt1"]//div//a[contains(@id,"thread_title")]/@href'
#NUM = '//table[@class="tborder"]//td[@class="alt1"]/a[@rel="next"]/@href'
CATEGORY = '//span[@class="navbar"]/a/text()'
SUBCATEGORY = '//span[@class="navbar"]/a/text() | //span[@style="color: #FF6600;"]//text()'
NODES = '//div[contains(@id,"edit")]'
AUTHORURL = './/a[@class="bigusername"]/@href'
POSTTITLE = './/div[@class="smallfont"]/strong/text() | .//div[@class="smallfont"]/img/@alt'
POSTURL = './/td[@class="thead"]/a/@href'
PUBLISH = './/td/a[contains(@name,"post")]//following-sibling::text()'
AUTHOR = './/td[@class="alt2"]//div/a[@class="bigusername"]//span//text()'
TEXT = './/div[contains(@id,"post_message")]//font/text() | .//div[contains(@id,"post_message")]//img/@alt | .//div[contains(@id,"post_message")]//text() | .//hr[@style="color: #173146; background-color:#0D1114"]/../text() | .//span[@class="time"]/text()| .//td//div[contains(@id,"post_message")]//img/@title | .//td//div[contains(@id,"post_message")]//img/@title | .//div[@style="margin:20px; margin-top:5px; "]/div[@class="smallfont"]/text() | .//td[@class="alt2"]//div//a/img[@class="inlineimg"]/@alt'
LINKS_ = './/td//div[contains(@id,"post_message")]//img/@src | .//td//div[contains(@id,"post_message")]//a/@href'
PAGENAV = '//table[@class="tborder"]//td[@class="alt1"]/a[@rel="next"]/@href'

#Authors

USERNAME = '//h1//span[@style="font: 16px courier; color: #FF3800;"]/b/s/text() |  //h1/span[@style="font: 14px verdana; color: white;"]/text() | //h1/span[@style="font: 14px courier; color: #FF3800;"]/s/text() | //h1//span[@style="font: 16px courier; color: #30D5C8;"]/s/text() | //h1//span[@style="font: 16px verdana; color: green;"]//b/text() |  //span[@style="font: 12px verdana; color: white;"]//b/text() | //span[@style="font: 14px verdana; color: white;"]/b/text()'
AUTHORSIGNATURE = '//dd[@id="signature"]//text() | //dd[@id="signature"]/font/a/@href | //dd[@id="signature"]//b/a//@href | //font[@color="Yellow"]//a/@href | //dd[@id="signature"]/img/@title | //b/font[@color="Lime"]/a/@href | //div[@align="center"]/a/@href | //dd[@id="signature"]/a/@href'
JOINDATE = '//fieldset[contains(.,"%s")]//following-sibling::ul//li//text()'% u"\u0420\u0435\u0433\u0438\u0441\u0442\u0440\u0430\u0446\u0438\u044f:"
LASTACTIVE = '//span[contains(.,"%s")]//..//text()'% u'\u041f\u043e\u0441\u043b\u0435\u0434\u043d\u044f\u044f \u0430\u043a\u0442\u0438\u0432\u043d\u043e\u0441\u0442\u044c:'
TOTALPOSTS = '//fieldset[contains(.,"%s")]//following-sibling::ul//li//text()'% u'\u0412\u0441\u0435\u0433\u043e \u0441\u043e\u043e\u0431\u0449\u0435\u043d\u0438\u0439'
GROUPS = '//h2/text() | //h2//b/text() | //a/font[@color="#709EFF"]/text() | //h2//span[@style="font: 16px courier; color:red;"]/text() | //span[@style="font: 14px verdana; color: green;"]/text()'
