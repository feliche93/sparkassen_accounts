# -*- coding: utf-8 -*-
import json
import os
import re

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess


class SparkassenAccountsSpider(scrapy.Spider):
    name = 'sparkassen_accounts'
    df = pd.read_csv(
        '/Users/felixvemmer/Desktop/sparkassen_accounts/sparkassen_list/sparkassen_rangliste_cleaned.csv')

    allowed_domains = list(df['links'].str.split('/', expand=True)
                           [2].str.split('www.', expand=True)[1])

    #start_urls = ['http://sparkasse.de/']

    sparkassen_links = df['links'].to_list()

    start_urls = []

    for link in sparkassen_links:
        base_link = link.replace('.html', '/privatkunden/')
        first_pattern = base_link + '/girokonto.html?'
        second_pattern = base_link + '/girokonto-uebersicht.html?'
        third_pattern = base_link + '/girokonten-und-karten/girokonten.html?'
        start_urls.append(first_pattern)
        start_urls.append(second_pattern)
        start_urls.append(third_pattern)

    def parse(self, response):
        base_url = response.url.split('/de/home')[0]
        if response.status == 200:
            accounts = response.xpath(
                '//div[@class="cbox cbox-product cbox-small section"]')
            for account in accounts:

                account_name = account.xpath(
                    './/span//text()').extract_first()
                if account_name == None:
                    account_name = account.xpath(
                        './/h2//text()').extract_first()

                try:
                    account_link = base_url + \
                        account.xpath('.//a/@href').extract_first()
                except:
                    print(response.url)
                print(account_name, account_link)

                yield scrapy.Request(account_link, callback=self.parse_account_details, meta={'account_name': account_name, 'account_link': account_link})

    def parse_account_details(self, response):
        table = response.xpath('//table')
        print(table)


process = CrawlerProcess(settings={
    'FEED_FORMAT': 'csv',
    'FEED_URI': 'test.csv'
})

process.crawl(SparkassenAccountsSpider)
process.start()
