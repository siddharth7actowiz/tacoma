# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class TacomaItem(scrapy.Item):
    # define the fields for your item here like:
    category=scrapy.Field()
    sub_category=scrapy.Field()
    name = scrapy.Field()
    prod_url=scrapy.Field()
  
