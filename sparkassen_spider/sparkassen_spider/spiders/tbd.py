
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
                    print('Issue with following {} for {}'.format(
                        account_name, response.url))

                yield scrapy.Request(account_link, callback=self.parse_account_details, meta={'account_name': account_name, 'account_link': account_link})

    def parse_account_details(self, response):
        account_name = response.meta['account_name']
        account_link = response.meta['account_link']

        try:
            tables = response.xpath('//table')
            if len(tables) > 1:
                table = tables[1]
            else:
                table = tables[0]

            rows = table.xpath('.//tr')

            for row in rows:
                info = row.xpath('th//text()').extract_first()
                value = row.xpath('td//text()').extract_first()

                if 'Kontoführung' in info:
                    activitiy_charge = value

                if 'Dispositionskredit' or 'Dispo' in info:
                    agreed_overdraft_interest = value

                if 'Überziehungskredit' in info:
                    non_agreed_overdraft_interest = value

        except:
            table = 'Error'

        yield {
            'Konto Name': account_name,
            'Konto Link': account_link,
            'Kontoführungsgebühr': activitiy_charge,
            'Sollzins Dispokredit': agreed_overdraft_interest,
            'Sollzins Überziehungskredit': non_agreed_overdraft_interest
        }


process = CrawlerProcess(settings={
    'FEED_FORMAT': 'csv',
    'FEED_URI': 'test_run.csv'
})

process.crawl(SparkassenAccountsSpider)
process.start()


# scrapy crawl sparkassen_accounts -o test.csv
