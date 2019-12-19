import datetime
import time

#XPATHS
#Nulled_Threads_crawl
# @def parse
formus_xpath = '//td[@class="col_c_forum"]//strong[@class="highlight_unread"]//a/@href'
# @def parde_forms
threads_id_xpath = '//tr[@class="__topic notnew expandable"]//a[@itemprop="url"]/@id'
threads_urls_xpath = '//tr[@class="__topic notnew expandable"]//a[@itemprop="url"]/@href'
threads_next_page_xpath = '//div[@class="topic_controls clearfix"]//li[@class="next"]//a[@title="Next page"]/@href'
Threads_crawl_que = 'insert into nulled_threads_crawl(sk,url,status_code,crawl_type,reference_url)values(%s,"%s","%s","%s","%s")ON DUPLICATE KEY UPDATE status_code = "%s", crawl_type = "%s",url = "%s", reference_url = "%s"'



#Nulled_threas_meta
# @def parse_thread
FetchTime = int(datetime.datetime.now().strftime("%s")) * 1000
node_xpath = '//div[@class="post_block hentry clear clearfix column_view  "]'
category_xpath = '//ol[@class="breadcrumb top ipsList_inline left"]//span[@itemprop="title"]//text()'
subcategory_xpath = '//ol[@class="breadcrumb top ipsList_inline left"]//span[@itemprop="title"]//text()'
threadtitle_xpath = '//div[@class="maintitle clear clearfix"]//span//text()'
postid_xpath = './/a[@itemprop="replyToUrl"]/@data-entry-pid'
posturl_xpath = './/a[@itemprop="replyToUrl"]/@href'
publishtime_xpath = './/abbr[@class="published"]//text()'
author_xpath = './/div[@class="post_username"]//span[@itemprop="name"]//text() | .//div[@class="post_username"]//span[@class="author vcard"]//text()'
text_xpath = './/div[@itemprop="commentText"]//blockquote/@data-author | .//div[@itemprop="commentText"]//blockquote/@data-time | .//div[@itemprop="commentText"]//a[@hovercard-ref="member"]//@alt | .//div[@itemprop="commentText"]//text() | .//div[@itemprop="commentText"]//img[@class="bbc_img"]/@alt | .//div[@itemprop="commentText"]//a[style="text-decoration:none;"]//@alt | .//div[@itemprop="commentText"]//img[@class="bbc_emoticon"]/@alt | .//div[@itemprop="commentText"]//p[@class="citation"]/@class '
links_xpath = './/div[@itemprop="commentText"]//a[@hovercard-ref="member"]//@href | .//div[@itemprop="commentText"]//img[@class="bbc_img"]/@src | .//div[@itemprop="commentText"]//a[style="text-decoration:none;"]//@href | .//div[@itemprop="commentText"]//a[@class="bbc_url"]/@href'
#| .//div[@class="signature"]//a[@class="bbc_url"]/@href | .//div[@class="signature"]//img[class="bbc_img"]/@src | .//div[@class="signature"]//img[@class="bbc_img"]/@src'




#author meta xpath

username_xpath = '//div[@class="profile_username"]//span//text()'
totalposts_xpath = '//div[@class="profile-bit-inner"]//td[contains(.,"Posts:")]/..//td[@style="text-align:right;"]/text()'
author_signature_xpath = '//div[@class="signature"]//@href | //div[@class="signature"]//text() | //div[@class="signature"]//@src | .//div[@class="signature"]//text() | .//div[@class="signature"]//img[@class="bbc_emoticon"]/@alt | .//div[@class="signature"]//a[@class="bbc_url"]/@href | .//div[@class="signature"]//img[class="bbc_img"]/@src | .//div[@class="signature"]//img[@class="bbc_img"]/@src '
join_date_xpath = '//td[contains(text(),"Joined:")]/../td[@style="text-align:right"]//text()'
lastactive_xpath = '//td[contains(text(),"Last Visit:")]/../td[@style="text-align:right"]//text()'
reputation_xpath = '//div[@class="profile-bit-inner"]//td[contains(.,"Reputation:")]/../td//strong//text()'
credits_xpath = '//div[@class="profile-bit-inner"]//td[contains(.,"Credits:")]/..//td[@style="text-align:right;"]/text()'
awards_xpath = '//div[@class="small-awards"]//img/@title'
rank_xpath = '//div[@class="col-md-2"]//div[@class="profile-bit"]/div[@class="profile-bit-inner"]//img/@src'


def activetime_str(activetime_,totalposts):
    activetime = []
    for a in activetime_:
        try:
            dt = time.gmtime(int(a)/1000)
            a = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year),str(dt.tm_mon),str(dt.tm_wday),str(dt.tm_hour),totalposts.encode('utf8'))
            activetime.append(a)
        except:pass
    return str(activetime)
