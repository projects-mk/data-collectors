import asyncio
from typing import List, Dict, Any

import aiohttp
import numpy as np
import pandas as pd
import soupsieve as sv
import logging
from bs4 import BeautifulSoup
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


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
        async with session.get(url) as response:
            return await response.text()

    async def collect_links(self, page: int) -> List[str]:
        links: List[str] = []
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, f'https://www.otomoto.pl/osobowe?page={page}')
            soup = BeautifulSoup(html, 'html.parser')
            a_tags = soup.find_all('a')
            for tag in a_tags:
                await asyncio.sleep(1)
                if not type(tag.get('href')) == type(None) and 'https://www.otomoto.pl/osobowe/oferta/' in tag.get('href'):
                    links.append(tag.get('href'))
        return set(links)

    async def main_links(self) -> None:
        pages = range(self.start_page, self.end_page+1)
        tasks = []
        for page in pages:
            await asyncio.sleep(1)
            tasks.append(self.collect_links(page))

        pages_links = await asyncio.gather(*tasks)
        self.links = [link for links in pages_links for link in links]

    async def parse(self, link: str) -> None:
        await asyncio.sleep(15)
        async with aiohttp.ClientSession() as session:
            html = await self.fetch(session, link)

            soup = BeautifulSoup(html, 'html.parser')
            css_selector = '#__next > div > div > div > main > div > section.ooa-j2bofk.e133w2fw0 > div.ooa-w4tajz.e18eslyg0'
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

    def to_sql(self, conn_str: str, table_name: str) -> None:
        engine = create_engine(conn_str)
        df = pd.DataFrame(self.data)
        df = df.dropna(how='all')
        logger.info('Collected total of %s records. Saving to database...', len(df))
        df.to_sql(table_name, engine, if_exists='append')

    def run(self) -> None:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.main_links())
        loop.run_until_complete(self.main_data())