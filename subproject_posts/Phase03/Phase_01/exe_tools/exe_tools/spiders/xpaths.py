

THREAD_TITLE = '//td[@class="navbar"]//text()'
THREAD_TOPIC = '//span[@class="navbar"]//text()'
ALL_POSTS = '//div[@id="posts"]//div[@align="center"]'
POST_URL = '//td[@class="thead"]//div[@class="normal"]//a//@href'
POST_TIME = '//td[@class="thead"]//div[@class="normal"]//following-sibling::text()'
AUTHOR = '//a[@class="bigusername"]//text()'
AUTHOR_URL = '//a[@class="bigusername"]//@href'
COMMENTS = '//tr//td[@id="%s"]//text() | //img//@title | //tr//td[@id="%s"]//div[@class="smallfont"]//@alt'
LINKS = '//tr//td[@id="%s"]//div[@id="post_message_%s"]//a//@href'
#stargif_link = '//div[@class="smallfont"]/img/@src'
#reputationgif_link = '//div/span[@id]/img/@src'
POSTS = '//div[@class="smallfont"]//div[contains(text(), "Posts:")]//text()'
ACTIVE_TIME = '//div[a[contains(@name, "post")]]//text()'
AUTHOR_SIGNATURE = '//tr//td[contains(@id, "td_post_")]//div[contains(@id, "post_message_")]/following-sibling::div//text() | //tr//td[contains(@id, "td_post_")]//div[contains(@id, "post_message_")]/following-sibling::div//img[@title]//@title  | //tr/td[contains(@id, "td_post_")]/div[contains(@id, "post_message_")]/following-sibling::div/text()'
ARROW = '//a[@rel="nofollow"]//img[@class="inlineimg"]/@alt'

REFERENCE_URL = '//a[@class="bigusername"]/@href'
JOIN_DATE = '//div[contains(text(), "Join Date:")]//text()'
REPUTATION = '//div[@class="smallfont"]//div[contains(text(), "Rept. Rcvd")]//text()'
GROUPS = '//div[@id="postmenu_%s"]//following-sibling::div[@class="smallfont"]//text()'
NEXT_PAGE = '//td[@class="alt1"]//a[contains(@rel, "next")]//@href'

URLS = [
         'https://forum.exetools.com/showthread.php?t=15279',
         'https://forum.exetools.com/showthread.php?t=14329',
         'https://forum.exetools.com/showthread.php?t=13074',
         'https://forum.exetools.com/showthread.php?t=18906',
         'https://forum.exetools.com/showthread.php?t=18937',
         'https://forum.exetools.com/showthread.php?t=18914',
         'https://forum.exetools.com/showthread.php?t=18785',
         'https://forum.exetools.com/showthread.php?t=18926'
]

SITE_DOMAIN = 'https://forum.exetools.com/'

