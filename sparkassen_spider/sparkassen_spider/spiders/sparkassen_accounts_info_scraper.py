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

        if len(accounts) == 0:
            accounts = response.xpath(
                '//div[contains(@class, "cbox-image")]')

        yield {
            'requested_url': response.url,
            'tables_on_website': len(accounts)
        }


process = CrawlerProcess(settings={
    'FEED_FORMAT': 'csv',
    'FEED_URI': 'scraping_test.csv'
})


process.crawl(SparkassenAccountsScraper)
process.start()

#
