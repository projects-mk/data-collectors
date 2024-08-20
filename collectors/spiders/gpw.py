import scrapy
from scrapy.responsetypes import Response
import random
import pandas as pd
import os
from dotenv import load_dotenv
from typing import List, Union
from datetime import datetime, timedelta
import numpy as np
import re
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
import pytz
from datetime import datetime

warsaw_tz = pytz.timezone("Europe/Warsaw")
current_time_warsaw = datetime.now(tz=warsaw_tz).strftime("%d-%m-%Y")


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


def generate_urls(start_date: str, end_date: str) -> List[str]:
    """
    Generate a list of URLs based on a range of dates.

    Parameters:
        start_date (str): Starting date in format "DD-MM-YYYY".
        end_date (str): Ending date in format "DD-MM-YYYY".

    Returns:
        List[str]: A list of URLs.
    """
    start_date_obj = datetime.strptime(start_date, "%d-%m-%Y")
    end_date_obj = datetime.strptime(end_date, "%d-%m-%Y")
    urls = []
    current_date = start_date_obj

    while current_date <= end_date_obj:
        date_str = current_date.strftime("%d-%m-%Y")
        url = f"https://www.gpw.pl/archiwum-notowan?fetch=0&type=10&instrument=&date={date_str}&show_x=Poka%C5%BC+wyniki"
        urls.append(url)

        current_date += timedelta(days=1)

    return urls


def extract_date(url: str):
    pattern = r"date=(\d{2}-\d{2}-\d{4})"
    match = re.search(pattern, url)

    if match:
        return match.group(1)
    else:
        return None


def to_float(s: str) -> Union[None, float]:
    s = str(s)
    s = s.replace(",", ".").strip()
    s = "".join([c for c in s if c.isdigit() or c == "." or c == "-"])
    try:
        return float(s)
    except ValueError:
        return np.nan


def preprocess(x: float) -> float:
    return x / 10000


class GpwSpider(scrapy.Spider):
    name = "gpw"
    allowed_domains = ["gpw.pl"]
    start_urls = "https://gpw.pl"
    start_date = current_time_warsaw
    end_date = current_time_warsaw

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_fixed(2),
    )
    def make_request_with_retry(self, url, user_agent):
        return scrapy.Request(
            url=url, headers={"User-Agent": user_agent}, callback=self.parse
        )

    def start_requests(self):
        urls = generate_urls(self.start_date, self.end_date)
        for url in urls:
            user_agent = random.choice(user_agents)
            try:
                yield self.make_request_with_retry(url, user_agent)
            except Exception as e:
                self.logger.error(
                    f"Failed to retrieve {url} after multiple retries: {e}"
                )

    def parse(self, response: Response):
        body = response.body
        tabs = pd.read_html(body, thousands=",")

        df = tabs[1]
        del df["Wartość obrotu (w tys.)"]
        price_cols = [
            "Kurs otwarcia",
            "Kurs maksymalny",
            "Kurs minimalny",
            "Kurs zamknięcia",
        ]

        for col in df.columns:
            if col not in ["Nazwa", "Waluta"]:
                df[col] = df[col].apply(to_float)
                if col in price_cols:
                    df[col] = df[col].apply(preprocess)

        df["Data"] = extract_date(response.url)

        df.to_sql("notowania_raw", conn_str, if_exists="append")
