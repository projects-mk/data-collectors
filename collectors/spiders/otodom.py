import scrapy
from scrapy.responsetypes import Response
import random
import pandas as pd
import os
from dotenv import load_dotenv
from utils import user_agents, generate_conn_string

load_dotenv()
conn_str = generate_conn_string(db="projects")


class OtodomSpider(scrapy.Spider):
    name = "otodom"
    allowed_domains = ["otodom.pl"]
    start_url = "https://otodom.pl"

    def start_requests(self):
        search_domains = [
            *[
                f"/pl/wyniki/sprzedaz/mieszkanie/cala-polska?viewType=listing&page={page}"
                for page in range(1, 4300)
            ],
            *[
                f"/pl/wyniki/sprzedaz/dom/cala-polska?ownerTypeSingleSelect=ALL&by=DEFAULT&direction=DESC&viewType=listing&page={page}"
                for page in range(1, 1500)
            ],
        ]
        for domain in search_domains:
            user_agent = random.choice(user_agents)
            if "mieszkanie" in domain:
                yield scrapy.Request(
                    url=self.start_url + domain,
                    headers={"User-Agent": user_agent},
                    callback=self.parse_mieszkania,
                )

            elif "dom" in domain:
                yield scrapy.Request(
                    url=self.start_url + domain,
                    headers={"User-Agent": user_agent},
                    callback=self.parse_domy,
                )

    def parse_mieszkania(self, response: Response):
        links = response.css("a::attr(href)").getall()

        for link in set(links):
            if link.startswith("/pl/oferta"):
                full_link = self.start_url + link
                user_agent = random.choice(user_agents)
                yield scrapy.Request(
                    url=full_link,
                    headers={"User-Agent": user_agent},
                    callback=self.get_specification_mieszkania,
                )

    def get_specification_mieszkania(self, response: Response):

        data = {
            "cena": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[1]/div[3]/div[1]/div[1]/strong/text()'
            ).get(),
            "adres": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[1]/div[3]/div[2]/a/text()'
            ).get(),
            "m2": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[1]/button[1]/div[2]/text()'
            ).get(),
            "pokoje": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[1]/button[2]/div[2]/text()'
            ).get(),
            "ogrzewanie": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[1]/p[2]/text()'
            ).get(),
            "pietro": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[3]/p[2]/text()'
            ).get(),
            "czynsz": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[5]/p[2]/text()'
            ).get(),
            "rynek": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[9]/p[2]/text()'
            ).get(),
            "forma_wlasnosci": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[11]/p[2]/text()'
            ).get(),
            "sprzedawca": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[15]/p[2]/text()'
            ).get(),
            "rynek": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[9]/p[2]/text()'
            ).get(),
            "rok_budowy": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[2]/div[9]/p[2]/text()'
            ).get(),
            "winda": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[3]/p[2]/text()'
            ).get(),
            "rodzaj_zabudowy": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[5]/p[2]/text()'
            ).get(),
            "material_budynku": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[7]/p[2]/text()'
            ).get(),
            "okna": response.xpath(
                '//*[@id="__next"]/main/div[3]/div[1]/div[2]/div[3]/div[1]/div/div/div[9]/p[2]/text()'
            ).get(),
            "opis": "".join(
                response.xpath(
                    '//*[@id="__next"]/main/div[3]/div[1]/div[4]/div[1]/p/text()'
                ).getall()
            ),
        }

        pd.DataFrame([data]).to_sql(
            "otodom_mieszkania_raw", conn_str, if_exists="append"
        )

    def parse_domy(self, response: Response):
        links = response.css("a::attr(href)").getall()

        for link in set(links):
            if link.startswith("/pl/oferta"):
                full_link = self.start_url + link
                print("link", full_link)
                user_agent = random.choice(user_agents)
                yield scrapy.Request(
                    url=full_link,
                    headers={"User-Agent": user_agent},
                    callback=self.get_specification_domy,
                )

    def get_specification_domy(self, response: Response):

        data = {
            "cena": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[1]/div[3]/div[1]/div[1]/strong/text()'
            ).get(),
            "zabudowa": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[1]/p[2]/text()'
            ).get(),
            "wykonczenie": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[3]/p[2]/text()'
            ).get(),
            "rynek": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[5]/p[2]/text()'
            ).get(),
            "sprzedawca": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[9]/p[2]/text()'
            ).get(),
            "czynsz": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[11]/p[2]/text()'
            ).get(),
            "ogrzewanie": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[2]/div[13]/p[2]/text()'
            ).get(),
            "pietra": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[1]/p[2]/text()'
            ).get(),
            "rok_budowy": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[3]/p[2]/text()'
            ).get(),
            "material_budynku": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[5]/p[2]/text()'
            ).get(),
            "okna": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[7]/p[2]/text()'
            ).get(),
            "dach": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[9]/p[2]/text()'
            ).get(),
            "pokrycie_dachu": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[11]/p[2]/text()'
            ).get(),
            "poddasze": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[1]/div/div/div[13]/p[2]/text()'
            ).get(),
            "powierzchnia_dzialki": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[1]/p[2]/text()'
            ).get(),
            "polozenie": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[3]/p[2]/text()'
            ).get(),
            "okolica": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[5]/p[2]/text()'
            ).get(),
            "ogrodzenie": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[7]/p[2]/text()'
            ).get(),
            "dojazd": response.xpath(
                '//*[@id="__next"]/main/div[1]/div[1]/div[2]/div[3]/div[2]/div/div/div[9]/p[2]/text()'
            ).get(),
            "opis": "".join(
                response.xpath(
                    '//*[@id="__next"]/main/div[1]/div[1]/div[4]/div[1]/p[1]/text()'
                ).getall()
            ),
        }
        pd.DataFrame([data]).to_sql("otodom_domy_raw", conn_str, if_exists="append")
