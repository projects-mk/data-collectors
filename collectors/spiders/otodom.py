import scrapy
from scrapy.responsetypes import Response
import random
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()
conn_str = os.getenv('PG_CONN_STRING')
user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.5195.52 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:91.0) Gecko/20100101 Firefox/91.0",
    "Mozilla/5.0 (Linux; Android 11; SM-G965U) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:93.0) Gecko/20100101 Firefox/93.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0",
    "Mozilla/5.0 (Linux; U; Android 11; en-us; SM-G973U Build/RP1A.200720.012) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/91.0.4472.164 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
]

class OtodomSpider(scrapy.Spider):
    name = "otodom"
    allowed_domains = ["otodom.pl"]
    start_url = "https://otodom.pl"

    def start_requests(self):
        search_domains = [
            *[f"/pl/wyniki/sprzedaz/mieszkanie/cala-polska?viewType=listing&page={page}" for page in range(1,4300)],
            *[f"/pl/wyniki/sprzedaz/dom/cala-polska?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing&page={page}" for page in range(1,1500)],
            
        ]
        for domain in search_domains:
            user_agent = random.choice(user_agents)
            if 'mieszkanie' in domain:
                yield scrapy.Request(url=self.start_url + domain, headers={'User-Agent': user_agent}, callback=self.parse_mieszkania)

            elif 'dom' in domain:
                yield scrapy.Request(url=self.start_url + domain, headers={'User-Agent': user_agent}, callback=self.parse_domy)

    def parse_mieszkania(self, response: Response):
        links = response.css("a::attr(href)").getall()

        for link in links:
            if link.startswith("/pl/oferta"):
                full_link = self.start_url + link
                user_agent = random.choice(user_agents)
                yield scrapy.Request(url=full_link, headers={'User-Agent': user_agent}, callback=self.get_specification_mieszkania)

    def get_specification_mieszkania(self, response: Response):

        data = {
            'cena': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[1]/div[3]/div[1]/div[1]/strong/text()').get(),
            'm2': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[1]/button[1]/div[2]/text()').get(),
            'pokoje': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[1]/button[2]/div[2]/text()').get(),
            'ogrzewanie': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[1]/p[2]/text()').get(),
            'pietro': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[3]/p[2]/text()').get(),
            'czynsz': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[5]/p[2]/text()').get(),
            'rynek': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[9]/p[2]/text()').get(),
            'forma_wlasnosci': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[11]/p[2]/text()').get(),
            'sprzedawca': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[15]/p[2]/text()').get(),
            'rynek': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[9]/p[2]/text()').get(),

            'rok_budowy': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[9]/p[2]/text()').get(),
            'winda': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[3]/p[2]/text()').get(),
            'rodzaj_zabudowy': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[5]/p[2]/text()').get(),
            'material_budynku': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[7]/p[2]/text()').get(),
            'okna': response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[9]/p[2]/text()').get(),

            'opis': ''.join(response.xpath('//*[@id="__next"]/main/div[3]/div[1]/div[4]/div[1]/p/text()').getall())
        }

        pd.DataFrame([data]).to_sql("otodom_mieszkania_raw", conn_str, if_exists='append')

    def parse_domy(self, response: Response):
        links = response.css("a::attr(href)").getall()

        for link in links:
            if link.startswith("/pl/oferta"):
                full_link = self.start_url + link
                print('link', full_link)
                user_agent = random.choice(user_agents)
                yield scrapy.Request(url=full_link, headers={'User-Agent': user_agent}, callback=self.get_specification_domy)

    def get_specification_domy(self, response: Response):

        data = {
            'cena': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[1]/div[3]/div[1]/div[1]/strong/text()').get(),

            'zabudowa': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[1]/p[2]/text()').get(),
            'wykonczenie': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[3]/p[2]/text()').get(),
            'rynek': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[5]/p[2]/text()').get(),
            'sprzedawca': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[9]/p[2]/text()').get(),
            'czynsz': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[11]/p[2]/text()').get(),
            'ogrzewanie': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[13]/p[2]/text()').get(),

            'pietra': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[1]/p[2]/text()').get(),
            'rok_budowy': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[3]/p[2]/text()').get(),
            'material_budynku': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[5]/p[2]/text()').get(),

            'okna': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[7]/p[2]/text()').get(),
            'dach': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[9]/p[2]/text()').get(),
            'pokrycie_dachu': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[11]/p[2]/text()').get(),
            'poddasze': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[13]/p[2]/text()').get(),

            'powierzchnia_dzialki': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[1]/p[2]/text()').get(),
            'polozenie': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[3]/p[2]/text()').get(),
            'okolica': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[5]/p[2]/text()').get(),
            'ogrodzenie': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[7]/p[2]/text()').get(),
            'dojazd': response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[9]/p[2]/text()').get(),
            
            'opis': ''.join(response.xpath('//*[@id="__next"]/main/div[1]/div[1]/div[4]/div[1]/p[1]/text()').getall())

        }
        pd.DataFrame([data]).to_sql("otodom_domy_raw", conn_str, if_exists='append')