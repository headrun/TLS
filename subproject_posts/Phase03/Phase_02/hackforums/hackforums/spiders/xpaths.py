#HACKFORUMS POSTS XPATHS

#LINKS = '//div[@id="tabmenu_7"]//div[@class="td-foat-left mobile-link"]//strong//a//@href'
LINKS = '//div[@class="td-foat-left mobile-link"]//strong//a//@href'
INNERLINKS = '//div[@class="td-foat-left mobile-link"]//strong//a/@href'
THREADURLS = '//div[@class="mobile-link-truncate"]//span[contains(@id,"tid_")]/a/@href'
#'//div[@class="mobile-link-truncate"]//span//a/@href'
PAGENAVIGATION = '//div[@class="pagination"]//a[@class="pagination_next"]//@href'
THREADTITLE = '//td[@class="thead"]//div//strong//text() | //td[@class="thead"]//div//h1/text()'
CATEGORY = '//div[@class="breadcrumb"][1]//a//text()'
SUBCATEGORY = '//div[@class="breadcrumb"][1]//a//text()'
NODES = '//td[@id="posts_container"]//div[contains(@class,"post classic")]'
POSTURL = './/div[@class="float_right"]//strong//a//@href'
POSTID = './/div[@class="float_right"]//strong//a//@id'
PUBLISHTIME = './/div[@class="post_content"]//div[@class="post_head"]//span[@class="post_date"]/text() | .//span[@class="post_date"]//@title'
POST_TEXT = './/div[@class="post_body scaleimages"]//text() | .//div[@class="post_body scaleimages"]//span//@alt | .//div[@class="post_body scaleimages"]//blockquote[@class="mycode_quote"]//text() | .//div[@class="post_body scaleimages"]//blockquote[@class="mycode_quote"]//cite//span/text() | .//div[@class="post_body scaleimages"]//blockquote/@class | .//div[@class="post_body scaleimages"]//cite//a[@class="quick_jump"]//@alt |.//div[@class="post_body scaleimages"]//iframe[@class="video_response"]//@src | .//div[@class="post_body scaleimages"]//iframe[@id="ytplayer"]//src | .//div[@class="post_body scaleimages"]//img[contains(@class,"smilie smilie_")]//@alt | .//div[@class="post_body scaleimages"]//img[@class="mycode_img"]//@alt '
AUTHOR = './/span[@class="largetext"]//a//text()'
AUTHOR_URLS = './/span[@class="largetext"]//a//@href'
LINKS = './/div[@class="post_body scaleimages"]//a[@class="mycode_url"]//@href | .//div[@class="post_body scaleimages"]//img[@class="mycode_img"]//@src |.//div[@class="post_body scaleimages"]//cite//a[@class="quick_jump"]//@href | .//div[@class="ytp-cued-thumbnail-overlay-image"]//@style | .//div[@class="post_body scaleimages"]//iframe[@class="video_response"]//@src  | .//div[@class="post_body scaleimages"]//iframe[@id="ytplayer"]//src'
GIF_LINKS = './/div[@class="post_body scaleimages"]//a[@class="mycode_url"]//@href | .//div[@class="post_body scaleimages"]//cite//a[@class="quick_jump"]//@href | .//div[@class="post_body scaleimages"]//div[@class="ytp-cued-thumbnail-overlay-image"]//@style | .//div[@class="post_body scaleimages"]//iframe[@class="video_response"]//@src '
INNERPAGENAV = '//div[@class="pagination"]//a[@class="pagination_next"]//@href'

#HACKFORUMS AUTHOR XPATHS

USERNAME = '//div[@class="oc-item oc-main oc-item-profile-left"]//strong//span[contains(@class,"group")]//text() | //div[@class="oc-item oc-main oc-item-profile-left"]//strong//span[contains(@style,"color:")]//text()'
AUTHOR_SIGNATURE = '//td[@class="trow1 scaleimages"]//div[@class="mycode_align"]//a[@class="mycode_url"]//@href | //td[@class="trow1 scaleimages"]//div[@class="mycode_align"]//img[@class="mycode_img"]//@src | //td[@class="trow1 scaleimages"]//img[@class="mycode_img"]//@src | //td[@class="trow1 scaleimages"]//blockquote/@class | //td[@class="trow1 scaleimages"]//text() | //td[@class="trow1 scaleimages"]//a[@class="mycode_url"]//@href | //td[@class="trow1 scaleimages"]//span[@class="mycode_b"]//a//@href |//td[@class="trow1 scaleimages"]//blockquote//cite//a[@class="quick_jump"]/@href'
JOINED_DATE = '//table[@class="tborder"]//td//strong[contains(text(),"Joined:")]/../../td/text() | //table[@class="tborder"]//td//strong[contains(text(),"Joined:")]/../../td/span/@title'
LASTACTIVES= '//table[@class="tborder"]//td//strong[contains(text(),"Last Visit:")]/../../td/span/@title | //table[@class="tborder"]//td//strong[contains(text(),"Last Visit:")]/../../td/text()'
TOTALPOSTS = '//table[@class="tborder"]//td//strong[contains(text(),"Total Posts:")]/../../td/text()'
AWARDS = '//table[@class="tborder"]//td//strong[contains(text(),"Awards: ")]/../../td/strong/text()'
GROUPS = '//div[@class="oc-item oc-main oc-item-profile-left"]//span[@class="smalltext"]/text()'

