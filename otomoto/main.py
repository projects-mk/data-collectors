import asyncio
from typing import List, Dict, Any

import aiohttp
import numpy as np
import pandas as pd
import soupsieve as sv
import logging
from bs4 import BeautifulSoup
from sqlalchemy import create_engine
import random

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 13_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-A505FN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.96 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 10; SM-A505GN) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; U; Android 10; en-us; SM-A505F) Build/QP1A.190711.020) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/78.0.3904.108 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:77.0) Gecko/20100101 Firefox/77.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36",
    "Mozilla/5.0 (X11; Linux i686; rv:78.0) Gecko/20100101 Firefox/78.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:54.0) Gecko/20100101 Firefox/73.0",
    "Mozilla/5.0 (Android 8.1.0; Mobile; rv:61.0) Gecko/61.0 Firefox/68.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/7046A194A",
    "Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36"
]


def get_headers():

    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Cache-Control': 'max-age=0',
        'TE': 'Trailers',
    }

    return headers

class OtomotoScraper:
    def __init__(self, start_page: int = 1, end_page: int = 500):
        self.data: List[Dict[str, Any]] = []
        self.links: List[str] = []
        self.start_page: int = start_page
        self.end_page: int = end_page
        if asyncio.get_event_loop().is_running():
            import nest_asyncio
            nest_asyncio.apply()

    async def fetch(self, session: aiohttp.ClientSession, url: str) -> str:
        headers = get_headers()
        async with session.get(url, headers=headers) as response:
            return await response.text()

    async def collect_links(self, page: int) -> List[str]:
        links: List[str] = []
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, f'https://www.otomoto.pl/osobowe?page={page}')
            soup = BeautifulSoup(html, 'html.parser')
            a_tags = soup.find_all('a')
            for tag in a_tags:
                # await asyncio.sleep(5)
                if not type(tag.get('href')) == type(None) and 'https://www.otomoto.pl/osobowe/oferta/' in tag.get('href'):
                    links.append(tag.get('href'))
        return set(links)

    async def main_links(self) -> None:
        pages = range(self.start_page, self.end_page+1)
        tasks = []
        for page in pages:
            # await asyncio.sleep(5)
            tasks.append(self.collect_links(page))

        pages_links = await asyncio.gather(*tasks)
        self.links = [link for links in pages_links for link in links]

    async def parse(self, link: str) -> None:
        # await asyncio.sleep(5)
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, link)

            soup = BeautifulSoup(html, 'html.parser')
            css_selector = '#__next > div > div > div > main > div > section.ooa-1dludu4.ex721dv0 > div.ooa-w4tajz.e18eslyg0'
            element = sv.select_one(css_selector, soup)
            elements = element.find_all(['p', 'a']) if element else []
            elements_dict = {elements[i].text: elements[i+1].text for i in range(0, len(elements)-1, 2)}

            try:
                price_element = soup.find_all('h3')[-1].text.replace(' ', '')
                elements_dict['Cena'] = price_element
            except IndexError:
                elements_dict['Cena'] = np.nan

            self.data.append(elements_dict)

    async def main_data(self) -> None:
        tasks = [self.parse(link) for link in self.links]
        await asyncio.gather(*tasks)

    @staticmethod
    def _keep_selected_columns(df: pd.DataFrame) -> pd.DataFrame:
        columns_to_keep = ['Cena', 'Marka pojazdu', 'Model pojazdu', 'Wersja', 'Generacja',
            'Rok produkcji', 'Przebieg', 'Pojemność skokowa', 'Rodzaj paliwa',
            'Moc', 'Skrzynia biegów', 'Napęd', 'Spalanie W Mieście', 'Typ nadwozia','Liczba drzwi',
            'Liczba miejsc', 'Kolor', 'Kraj pochodzenia','Stan', 'Uszkodzony']
        return df[columns_to_keep]

    def to_sql(self, conn_str: str, table_name: str) -> None:
        engine = create_engine(conn_str)
        df = pd.DataFrame(self.data)
        df = df.dropna(how='all')
        df = self._keep_selected_columns(df)
        logger.info('Collected total of %s records. Saving to database...', len(df))
        df.to_sql(table_name, engine, if_exists='append')

    def run(self) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_links())
        loop.run_until_complete(self.main_data())