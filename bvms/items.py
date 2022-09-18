# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class BvmsItem(scrapy.Item):
  path = scrapy.Field()
  mis = scrapy.Field()
  body = scrapy.Field()

  def __str__(self):
    return ""
