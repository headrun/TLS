# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ThehubItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class Posts_item(scrapy.Item):
    doc = scrapy.Field()

class Author_item(scrapy.Item):
    rec = scrapy.Field()
