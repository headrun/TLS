# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html
import random
from scrapy import signals
from user_agents import user_agent
user_agent_list = user_agent.split('\n')
from w3lib.http import basic_auth_header
import random
proxy_list = ['fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com','centralusa.torguardvpnaccess.com','west.usa.torguardvpnaccess.com','westusa.torguardvpnaccess.com','east.usa.torguardvpnaccess.com','eastusa.torguardvpnaccess.com']+ ['au.torguardvpnaccess.com', 'melb.au.torguardvpnaccess.com', 'bul.torguardvpnaccess.com', 'cp.torguardvpnaccess.com', 'egy.torguardvpnaccess.com', 'iom.torguardvpnaccess.com', 'isr.torguardvpnaccess.com', 'fin.torguardvpnaccess.com', 'br.torguardvpnaccess.com', 'ca.torguardvpnaccess.com', 'vanc.ca.west.torguardvpnaccess.com', 'frank.gr.torguardvpnaccess.com', 'ice.torguardvpnaccess.com', 'ire.torguardvpnaccess.com', 'in.torguardvpnaccess.com', 'jp.torguardvpnaccess.com', 'nl.torguardvpnaccess.com', 'lon.uk.torguardvpnaccess.com', 'ro.torguardvpnaccess.com', 'ru.torguardvpnaccess.com', 'mos.ru.torguardvpnaccess.com', 'swe.torguardvpnaccess.com', 'swiss.torguardvpnaccess.com', 'bg.torguardvpnaccess.com', 'hk.torguardvpnaccess.com', 'cr.torguardvpnaccess.com', 'hg.torguardvpnaccess.com', 'my.torguardvpnaccess.com', 'thai.torguardvpnaccess.com', 'turk.torguardvpnaccess.com', 'tun.torguardvpnaccess.com', 'mx.torguardvpnaccess.com', 'singp.torguardvpnaccess.com', 'saudi.torguardvpnaccess.com', 'fr.torguardvpnaccess.com', 'pl.torguardvpnaccess.com', 'czech.torguardvpnaccess.com', 'gre.torguardvpnaccess.com', 'it.torguardvpnaccess.com', 'sp.torguardvpnaccess.com', 'no.torguardvpnaccess.com', 'por.torguardvpnaccess.com', 'za.torguardvpnaccess.com', 'den.torguardvpnaccess.com', 'vn.torguardvpnaccess.com', 'sk.torguardvpnaccess.com', 'lv.torguardvpnaccess.com', 'lux.torguardvpnaccess.com', 'nz.torguardvpnaccess.com', 'md.torguardvpnaccess.com', 'uae.torguardvpnaccess.com', 'slk.torguardvpnaccess.com', 'fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com', 'centralusa.torguardvpnaccess.com', 'west.usa.torguardvpnaccess.com', 'westusa.torguardvpnaccess.com', 'east.usa.torguardvpnaccess.com', 'eastusa.torguardvpnaccess.com']


class BinrevSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class BinrevDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):
        # Called for each request that goes through the downloader
        # middleware.

        # Must either:
        # - return None: continue processing this request
        # - or return a Response object
        # - or return a Request object
        # - or raise IgnoreRequest: process_exception() methods of
        #   installed downloader middleware will be called
        #proxy =  random.choice(['fl.east.usa.torguardvpnaccess.com', 'atl.east.usa.torguardvpnaccess.com', 'ny.east.usa.torguardvpnaccess.com', 'chi.central.usa.torguardvpnaccess.com', 'dal.central.usa.torguardvpnaccess.com', 'la.west.usa.torguardvpnaccess.com', 'lv.west.usa.torguardvpnaccess.com', 'sa.west.usa.torguardvpnaccess.com', 'nj.east.usa.torguardvpnaccess.com', 'central.usa.torguardvpnaccess.com','centralusa.torguardvpnaccess.com','west.usa.torguardvpnaccess.com','westusa.torguardvpnaccess.com','east.usa.torguardvpnaccess.com','eastusa.torguardvpnaccess.com'])
	proxy = random.choice(proxy_list)
        request.meta['proxy'] = 'http://'+ proxy+':6060'
	request.headers['Proxy-Authorization'] = basic_auth_header('vinuthna@headrun.com','Hotthdrn591!')
	request.headers['User-Agent'] = random.choice(user_agent_list)
        #return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):
        # Called when a download handler or a process_request()
        # (from other downloader middleware) raises an exception.

        # Must either:
        # - return None: continue processing this exception
        # - return a Response object: stops process_exception() chain
        # - return a Request object: stops process_exception() chain
        pass

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
