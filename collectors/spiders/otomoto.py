import scrapy
from scrapy.responsetypes import Response
import random
import pandas as pd
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from lxml import html

load_dotenv()
conn_str = os.getenv("PG_CONN_STRING")
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
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_4 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1",
]


class OtomotoSpider(scrapy.Spider):
    name = "otomoto"
    allowed_domains = ["otomoto.pl"]
    start_url = "https://otomoto.pl"

    def start_requests(self):
        search_domains = [
            *[f"/osobowe?page={page}" for page in range(1, 2)],
        ]
        for domain in search_domains:
            user_agent = random.choice(user_agents)
            yield scrapy.Request(
                url=self.start_url + domain,
                headers={"User-Agent": user_agent},
                callback=self.parse,
            )

    def parse(self, response: Response):
        links = response.css("a::attr(href)").getall()

        for link in set(links):
            if link.startswith("https://www.otomoto.pl/osobowe/oferta"):
                user_agent = random.choice(user_agents)
                yield scrapy.Request(
                    url=link,
                    headers={"User-Agent": user_agent},
                    callback=self.get_specification,
                )

    def get_specification(self, response: Response):

        body = response.body
        soup = BeautifulSoup(body, features="html.parser")

        price_selector = "#__next > div > div > div > main > div > aside > div.ooa-yd8sa2.etrkop90 > div.ooa-14tsqkp.e7asd8h0 > div.ooa-67fgj7.e7asd8h1 > div > h3"
        price_element = soup.select_one(price_selector)

        tree = html.fromstring(body)

        xpath_selector = (
            '//*[@id="__next"]/div/div/div/main/div/section[2]/div[4]/div[2]'
        )
        info = tree.xpath(xpath_selector)

        info_element = info[0]
        elements = info_element.xpath(".//p | .//a")
        info_elements_dict = {
            elements[i].text_content(): elements[i + 1].text_content()
            for i in range(0, len(elements) - 1, 2)
        }

        data = {
            "cena": price_element.text if price_element else None,
            "marka": info_elements_dict.get("Marka pojazdu"),
            "model": info_elements_dict.get("Model pojazdu"),
            "wersja": info_elements_dict.get("Wersja"),
            "generacja": info_elements_dict.get("Generacja"),
            "rok_produkcji": info_elements_dict.get("Rok produkcji"),
            "przebieg": info_elements_dict.get("Przebieg"),
            "pojemosc_skokowa": info_elements_dict.get("Pojemność skokowa"),
            "moc": info_elements_dict.get("Moc"),
            "rodzaj_paliwa": info_elements_dict.get("Rodzaj paliwa"),
            "skrzynia_biegow": info_elements_dict.get("Skrzynia biegów"),
            "naped": info_elements_dict.get("Napęd"),
            "spalanie_w_miescie": info_elements_dict.get("Spalanie W Mieście"),
            "nadwozie": info_elements_dict.get("Typ nadwozia"),
            "liczba_drzwi": info_elements_dict.get("Liczba drzwi"),
            "liczba_miejsc": info_elements_dict.get("Liczba miejsc"),
            "bezwypadkowy": info_elements_dict.get("Bezwypadkowy"),
            "serwisowany_w_aso": info_elements_dict.get("Serwisowany w ASO"),
            "stan": info_elements_dict.get("Stan"),
        }

        pd.DataFrame([data]).to_sql("otomoto_raw", conn_str, if_exists="append")
