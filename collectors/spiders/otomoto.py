import scrapy
from scrapy.responsetypes import Response
import random
import pandas as pd
import os
from dotenv import load_dotenv
from bs4 import BeautifulSoup
from lxml import html
from collectors.utils import user_agents, generate_conn_string

load_dotenv()
conn_str = generate_conn_string(db="projects")


class OtomotoSpider(scrapy.Spider):
    name = "otomoto"
    allowed_domains = ["otomoto.pl"]
    start_url = "https://otomoto.pl"

    def start_requests(self):
        search_domains = [
            *[f"/osobowe?page={page}" for page in range(1, 1400)],
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
