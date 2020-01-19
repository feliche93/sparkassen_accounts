# -*- coding: utf-8 -*-
import scrapy


class SparkassenAccountsSpider(scrapy.Spider):
    name = 'sparkassen_accounts'
    allowed_domains = ['sparkasse.de']
    start_urls = ['http://sparkasse.de/']

    def parse(self, response):
        pass
