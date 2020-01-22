# -*- coding: utf-8 -*-
import json
import os
import re

import pandas as pd
import scrapy
from scrapy.crawler import CrawlerProcess


class SparkassenLinkTester(scrapy.Spider):
    name = 'sparkassen_link_tester'
    df = pd.read_csv(
        '/Users/felixvemmer/Desktop/sparkassen_accounts/sparkassen_list/sparkassen_rangliste_cleaned.csv')

    # Take a df column and extract domain from full urls
    allowed_domains = list(df['links'].str.split('/', expand=True)
                           [2].str.split('www.', expand=True)[1])

    # Read in all urls and from sparkassen
    sparkassen_links = df['links'].to_list()

    start_urls = set()

    # Iterates over every link and creates three possible url paths for each sparkasse
    for link in sparkassen_links:
        base_link = link.replace('.html', '')
        patterns = set([
            '/girokonto.html?',
            '/girokonto-uebersicht.html?',
            '/girokonten-und-karten/girokonten.html?',
            '/konten-und-karten.html?',
            '/neue-girowelt.html?',
            '/girokonten.html?',
            '/produkte/girokonto.html?',
            '/konten-und-karten.html?',
            '/girokonto1.html?',
            '/produkte/konten.html?'
            '/privatkunden/girokonto.html?',
            '/privatkunden/girokonto-uebersicht.html?',
            '/privatkunden/girokonten-und-karten/girokonten.html?',
            '/privatkunden/konten-und-karten.html?',
            '/privatkunden/neue-girowelt.html?',
            '/privatkunden/girokonten.html?',
            '/privatkunden/produkte/girokonto.html?',
            '/privatkunden/konten-und-karten.html?',
            '/privatkunden/girokonto1.html?',
            '/privatkunden/produkte/konten.html?',
            '/konten---karten/girokonto.html?',
            '/privatkunden/girokonto.html?n=true&stref=hnav',
            '/produktangebot/girokonto.html?'
        ])

        for pattern in patterns:
            account_link_pattern = base_link + pattern
            start_urls.add(account_link_pattern)

    def parse(self, response):

       #base_url = response.url.split('/de/home')[0]

        if response.status == 200:

            yield {
                'verified_url': response.url
            }


process = CrawlerProcess(settings={
    'FEED_FORMAT': 'csv',
    'FEED_URI': 'verified_links.csv'
})


process.crawl(SparkassenLinkTester)
process.start()
