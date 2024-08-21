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
from collectors.utils import user_agents, generate_conn_string
from typing import Any
import time

load_dotenv()
conn_str = generate_conn_string(db="gpw_app")


class CompaniesDataSpider(scrapy.Spider):
    name = "companies-data"
    allowed_domains = ["biznesradar.pl"]
    start_urls = ["https://biznesradar.pl"]
    companies_dict = {}

    def start_requests(self):
        companies_df = pd.read_sql_table("list_of_companies", con=conn_str).drop(
            columns="index"
        )
        self.companies_dict = dict(
            list(zip(companies_df["ticker"], companies_df["company"]))
        )

        for company in self.companies_dict.keys():

            yield self.send_request(
                url=f"https://www.biznesradar.pl/raporty-finansowe-rachunek-zyskow-i-strat/{company}",
                user_agents=user_agents,
                callback=self.collect_pl_info,
            )

            yield self.send_request(
                url=f"https://www.biznesradar.pl/raporty-finansowe-bilans/{company}",
                user_agents=user_agents,
                callback=self.collect_bs_info,
            )

            yield self.send_request(
                url=f"https://www.biznesradar.pl/raporty-finansowe-przeplywy-pieniezne/{company}",
                user_agents=user_agents,
                callback=self.collect_cf_info,
            )

    @retry(
        stop=stop_after_attempt(10),
        wait=wait_fixed(2),
    )
    def send_request(self, url: str, callback: Any, user_agents: list):
        user_agent = random.choice(user_agents)
        time.sleep(random.randint(1, 5))
        return scrapy.Request(
            url=url,
            headers={"User-Agent": user_agent},
            callback=callback,
        )

    def clean_data(self, s: str | int | float):

        if not isinstance(s, str):
            return s

        for keyword in ["r/r", "k/k"]:
            if keyword in s:
                s = s.split(keyword)[0]
                break

        s = s.replace(" ", "").strip()

        try:
            s = int(s)
        except ValueError:
            try:
                s = float(s)
            except ValueError:
                pass

        return s

    def correct_col_name(self, col: str) -> str:
        col_name = (
            col.strip().split("(")[1].replace(")", "").split(" ")[1].replace("*", "")
        )

        if col_name != "kategorie":
            return "20" + col_name

        return col_name

    def clean_df(self, df: pd.DataFrame, company: str) -> pd.DataFrame:
        df = df.rename({"Unnamed: 0": " ( kategorie"}, axis=1)
        df = df.drop(columns=[col for col in df.columns if "Unnamed" in col])
        df.columns = [self.correct_col_name(col) for col in df.columns]

        for col in df.columns:
            df[col] = df[col].apply(lambda x: self.clean_data(x))

        df = df.T
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        df["firma"] = company

        return df

    def extract_company_name(self, url: str) -> str:
        return url.split("/")[-1]

    def extract_data(self, company: str, response: Response) -> pd.DataFrame:
        for i in range(4):
            try:
                df = pd.read_html(response.body)[i].fillna(0)
                df = self.clean_df(df, company)
            except Exception:
                pass

    def collect_bs_info(self, response: Response):
        company = self.extract_company_name(response.url)
        df = self.extract_data(company, response)
        df.to_sql("companies_bs_raw", con=conn_str, if_exists="append")

    def collect_pl_info(self, response: Response):
        company = self.extract_company_name(response.url)
        df = self.extract_data(company, response)
        df.to_sql("companies_pl_raw", con=conn_str, if_exists="append")

    def collect_cf_info(self, response: Response):
        company = self.extract_company_name(response.url)
        df = self.extract_data(company, response)
        df.to_sql("companies_cf_raw", con=conn_str, if_exists="append")
