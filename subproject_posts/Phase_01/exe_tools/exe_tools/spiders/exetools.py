#! /usr/bin/env python

import calendar
import datetime
import re
import MySQLdb
import time
import sys
sys.path.append('/home/epictions/tls_scripts/tls_utils')
import tls_utils as utils
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy.spiders import Spider
import xpaths
import json
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from elasticsearch import Elasticsearch
import hashlib
from pprint import pprint


def clean_text(input_text):
    '''  
    Cleans up special chars in input text.
    input = "Hi!\r\n\t\t\t\r\n\t\t\t\r\n\t\t\t\r\n\t\t\r\n\r\n\t\t\r\n\t\t\r\n\t\t\t\r\n\t\t\tHi, besides my account"
    output = "Hi!\nHi, besides my account"
    '''
    text = re.compile(r'([\n,\t,\r]*\t)').sub('\n', input_text)
    text = re.sub(r'(\n\s*)', '\n', text)
    return text

    
class Exetool(Spider):  
    name = "exetool"
    start_urls = ['https://forum.exetools.com/index.php']
    
    def __init__(self):
        self.es = Elasticsearch(['10.2.0.90:9342'])

    def parse(self, response):
        sel = Selector(response)
        start_page_links = sel.xpath(
            '//tbody//tr//td[@class="alt1Active"]//div//a//@href').extract()  # encode('ascii','ignore')
        for start_page_link in start_page_links:
            url = 'https://forum.exetools.com/%s' % start_page_link
            yield Request(url, callback=self.parse_nextlevel)

    def parse_nextlevel(self, response):
        sel = Selector(response)
        next_page_links = sel.xpath('//td[@class="alt1"]//div//a[contains(@id,"thread_title_")]//@href').extract()
        for next_page_link in next_page_links:
            url = 'https://forum.exetools.com/%s' % next_page_link
            #url = 'https://forum.exetools.com/showthread.php?t=19055'
            yield Request(url, callback=self.parse_thread)

        navigation_click = set(sel.xpath('//a[@rel="next"]//@href').extract())
        for click in navigation_click:
            if click:
                next_url = 'https://forum.exetools.com/%s' % click
                yield Request(next_url, callback=self.parse_nextlevel)

    def parse_thread(self, response):
        thread_url = ''.join(re.sub('s=(.*?)&','',response.url)) or 'Null' #response.url.split('&')[0]
        domain = 'forum.exetools.com'
        if '&page=' in response.url:
            crawl_type = 'catch_up'
        else:
            crawl_type = 'keep_up'
        thread_title = ''.join(response.xpath(xpaths.THREAD_TITLE).extract()).strip() or 'Null'
        thread_topic = ''.join(response.xpath(xpaths.THREAD_TOPIC).extract()).strip() or 'Null'
        try:
            category = ''.join(thread_topic.split('> ')[1]) or 'Null'
        except:
            pass
        sub_category = ''.join(thread_topic.split('> ')[2:]) or 'Null'
        sub_category_url =  ''.join(response.xpath('//span[@class="navbar"]//a//@href').extract()[2:]) or 'Null'
        if sub_category_url:
            sub_category_url = 'https://forum.exetools.com/'+sub_category_url
        all_posts = response.xpath(xpaths.ALL_POSTS).extract()
        for post in all_posts:
            Link = []
            sel = Selector(text=post)
            posted_url = ''.join(sel.xpath(xpaths.POST_URL).extract()).strip()
            post_ur = xpaths.SITE_DOMAIN + posted_url
            post_url = ''.join(re.sub('s=(.*?)&', '', post_ur))
            if posted_url == '':
               posted_url = 'Null'
               post_url = posted_url
            try:
                post_id = post_url.split('&')[-2].split('=')[-1].strip() or 'Null'
            except:
                pass
            ord_in_thread = ''.join(sel.xpath('//div[@class="normal"]//a[@target="new"]//text()').extract())
            posted_time = ''.join(sel.xpath(xpaths.POST_TIME).extract()).replace('\n', '').replace('\t', '').replace('\r', '').strip()
            publish_epoch = int(calendar.timegm(time.strptime(posted_time, '%m-%d-%Y, %H:%M')) * 1000)
            if publish_epoch:
                   month_year = time.strftime("%m_%Y", time.localtime(int(publish_epoch/1000)))
            else:
                import pdb;pdb.set_trace()
            fetch_time = int(calendar.timegm(time.gmtime()) * 1000)
            ref_url = ''.join(sel.xpath(xpaths.REFERENCE_URL).extract())
            refe_url  = re.sub('s=(.*?)&', '', ''.join(sel.xpath(xpaths.REFERENCE_URL).extract()))
            if "http" not in refe_url: reference_url = xpaths.SITE_DOMAIN + refe_url
            if not refe_url: reference_url = ""
            join_date = 0
            joindate = ''.join(sel.xpath(xpaths.JOIN_DATE).extract())
            if joindate:
                join_dt = joindate.replace('Join Date:', '')
                join_date = int(time.mktime(time.strptime(join_dt, ' %b %Y')))
            author = ''.join(
                sel.xpath('//a[@class="bigusername"]//text()').extract()).strip() or 'Null'

            author_url = xpaths.SITE_DOMAIN + ''.join(
                    sel.xpath('//a[@class="bigusername"]//@href').extract()).strip()
            author_url = ''.join(re.sub('s=(.*?)&', '', author_url))
            if author == '':
                auth_xp = '//td[@nowrap="nowrap"]//div[@id="%s"]//text()'
                title_id = 'postmenu_%s' % post_id
                auth_xp = auth_xp % title_id
                author = ''.join(sel.xpath('%s' % auth_xp).extract()).strip()
                author_url = 'Null'
            if not author_url:
                author_url = 'Null'
            post_title = "Null"
            comment_id = 'td_post_%s' % post_id
            post_xpath = xpaths.COMMENTS % (comment_id, comment_id)
            text_1 = '\n'.join(sel.xpath('//tr//td[contains(@id, "td_post")]//text() | //img//@title | //tr//td[contains(@id, "td_post")]//div[@class="smallfont"]//@alt | //a[@rel="nofollow"]//img[@class="inlineimg"]/@alt').extract()).strip()
            author_signature = '\n'.join(sel.xpath(xpaths.AUTHOR_SIGNATURE).extract()).strip()
            junk = '\n'.join(sel.xpath('//tr//td[contains(@id, "td_post_")]//div[contains(@id, "post_message_")]/following-sibling::div//em//text()').extract()).strip()
            if junk:
                author_signature = author_signature.replace(junk, '')
            post_text = clean_text(text_1).replace(clean_text(author_signature), '')

            link_xp = xpaths.LINKS % (comment_id, post_id)
            links = ','.join(sel.xpath('%s' % link_xp).extract()).strip()
            if author_signature:
                author_signature = clean_text(author_signature)  or 'Null'
            if links != '':
                linkss = links.split(',')
                for link in linkss:
                    link = ''.join(re.sub('s=(.*?)&', '', link))
                    if ('.com' not in link) and ('http:' not in link) and ('https:' not in link):
                        new_link = xpaths.SITE_DOMAIN + link
                        Link.append(new_link)
                        all_links = ', \n'.join(Link)
                    else:
                        Link.append(link)
                        all_links = ', \n'.join(Link)
            else:
                all_links = links
                #all_links = list(set(all_links))
                all_links = ', '.join(set(all_links))
                if not all_links:
                        all_links = 'Null'
            reputation = '\n'.join(sel.xpath(xpaths.REPUTATION).extract())
            reput = str(reputation.split('Rept. Rcvd ')[-1])
            repo = ''.join(re.findall('(\d+) Times', reput))
            groups_xp = xpaths.GROUPS % post_id
            group_obt = str(
                ''.join(sel.xpath(groups_xp).extract()).encode('utf8').strip())
            activetime = []
            totalposts = ''.join(sel.xpath(xpaths.ALL_POSTS).extract()).strip().replace('Posts:', '')
            if not totalposts: totalposts = ""
            active_timea = ''.join(sel.xpath(xpaths.ACTIVE_TIME).extract()).strip()
            lastactive = time.mktime(time.strptime(active_timea,'%m-%d-%Y, %H:%M'))
            try:
                dt = time.gmtime(int(time.mktime(time.strptime(active_timea,'%m-%d-%Y, %H:%M')))/1)
                activetime_ = """[ { "year": "%s","month": "%s", "dayofweek": "%s", "hour": "%s", "count": "%s" }]"""%(str(dt.tm_year), str(dt.tm_mon), str(dt.tm_mday), str(dt.tm_hour), totalposts)
                activetime.append(activetime_)
            except:
                activetime_ = ' '
                activetime.append(activetime_)
            activetime = ',  '.join(activetime)
            fetch_time = int(datetime.datetime.now().strftime("%s")) * 1000
            post = {
               'cache_link': '',
               'section':category,
               'language': "english",
               'require_login':"false",
               'sub_section':sub_category,
               'sub_section_url':sub_category_url,
               'post_id':post_id,
               'post_title':post_title,
               'ord_in_thread':ord_in_thread,
               'post_url':post_url,
               'post_text':post_text,
               'thread_title':thread_title,
               'thread_url':thread_url
               }
            author = {
               'name':author,
               'url':author_url
               }
            json_posts = {
                       'id':post_url,
                       'hostname': 'forum.exetools.com',
                       'domain' : domain,
                       'sub_type':'openweb',
                       'type':'forum',
                       'author':json.dumps(author),
                       'title':thread_title,
                       'url':post_url,
                       'original_url':post_url,
                       'fetch_time':fetch_time,
                       'publish_time' : publish_epoch,
                       'link_url' : all_links,
                       'post':post
               }
            #query={"query":{"match":{"_id":hashlib.md5(str(post_url)).hexdigest()}}}
            #res = self.es.search(body=query)
            #if res['hits']['hits'] == []:
            self.es.index(index="forum_posts_"+month_year, doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts,request_timeout=30)
            #else:
                #data_doc = res['hits']['hits'][0]
                #if (json_posts['links'] != data_doc['_source']['links']) or (json_posts['text'] != data_doc['_source']['text']):
                    #self.es.index(index="forum_posts", doc_type='post', id=hashlib.md5(str(post_url)).hexdigest(), body=json_posts)
            '''author_ = ''.join(sel.xpath('//a[@class="bigusername"]//text()').extract()).strip() or 'Null'
            json_authors = {
                'username': author_,
                'domain':  domain,
                'auth_sign': author_signature,
                'join_date':  join_date,
                'lastactive': lastactive,
                'totalposts':  totalposts,
                'groups': group_obt,
                'reputation': repo,
                'fetch_time':fetch_time,
                'credits': '',
                'credits': '',
                'awards': '',
                'rank': '',
                'activetimes': activetime,
                'contact_info': '',
            }
            self.es.index(index="forum_", doc_type='author', id=hashlib.md5(author_).hexdigest(), body=json_authors)'''

        next_page = set(response.xpath(xpaths.NEXT_PAGE).extract())
        if next_page:
            new_link = list(next_page)
            next_page_url = xpaths.SITE_DOMAIN + new_link[0]
            yield Request(next_page_url, self.parse)
                   
