# -*- coding: utf-8 -*-
import json
import os
import re

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess


class SparkassenAccountsScraper(scrapy.Spider):
    name = 'sparkassen_accounts_scraper'
    df = pd.read_csv(
        '/Users/felixvemmer/Desktop/sparkassen_accounts/sparkassen_list/final_to_scrape.csv')

    # Take a df column and extract domain from full urls
    allowed_domains = list(df['domain'])

    # Read in all urls and from sparkassen
    sparkassen_links = df['account_info_url'].to_list()
    start_urls = sparkassen_links

    def parse(self, response):
        accounts = response.xpath(
            '//div[@class="cbox cbox-product cbox-small section"]')

        # Second site script to find accounts
        if len(accounts) == 0:
            accounts = response.xpath(
                '//div[contains(@class, "cbox-image")]')

        for account in accounts:
            base_url = response.url.split('.de/')[0] + '.de'

            if account.xpath('.//a/@href').extract_first() and account.xpath('.//span/text()').extract_first():
                if re.search(r"(Konto|konto|giro|Giro)", account.xpath('.//span/text()').extract_first()):
                    account_name = account.xpath(
                        './/span/text()').extract_first()
                    account_detail_url = base_url + account.xpath(
                        './/a/@href').extract_first()

                    yield scrapy.Request(url=account_detail_url, callback=self.parse_account_details, meta={'account_name': account_name, 'account_detail_url': account_detail_url, 'requested_url': response.url})

            elif account.xpath('.//a/@href').extract_first() and account.xpath('.//h2/text()').extract_first():
                if re.search(r"(Konto|konto|giro|Giro)", account.xpath('.//h2/text()').extract_first()):
                    account_name = account.xpath(
                        './/h2/text()').extract_first()
                    account_detail_url = base_url + account.xpath(
                        './/a/@href').extract_first()

                    yield scrapy.Request(url=account_detail_url, callback=self.parse_account_details, meta={'account_name': account_name, 'account_detail_url': account_detail_url, 'requested_url': response.url})

            else:
                print(response.url)

    def parse_account_details(self, response):

        tables = response.xpath('//table')
        print(len(tables))

        yield {
            'account_name': response.meta['account_name'],
            'account_detail_url': response.meta['account_detail_url'],
            'requested_url': response.meta['requested_url'],
            'tables_on_website': len(tables)
        }


process = CrawlerProcess(settings={
    'FEED_FORMAT': 'csv',
    'FEED_URI': 'scraping_test.csv'
})


process.crawl(SparkassenAccountsScraper)
process.start()

#
