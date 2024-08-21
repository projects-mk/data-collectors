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


def rename_duplicates(columns: list):
    """
    Rename duplicate column names by appending a suffix _1, _2, etc.

    Parameters:
        columns (pd.Index or list): The list of column names to be processed.

    Returns:
        pd.Index: A new Index with renamed duplicate columns.
    """
    counts = {}
    new_columns = []

    for col in columns:
        if col in counts:
            counts[col] += 1
            new_column_name = f"{col}_{counts[col]}"
            new_columns.append(new_column_name)
        else:
            counts[col] = 0
            new_columns.append(col)

    return pd.Index(new_columns)


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
        """Clean the data by handling specific string cases and converting to appropriate types."""
        if not isinstance(s, str):
            return s

        # Remove specific keywords and anything following them
        for keyword in ["r/r", "k/k"]:
            if keyword in s:
                s = s.split(keyword)[0]
                break

        # Clean up whitespace and try to convert to int or float
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
        """Correct the column name according to predefined rules."""
        col_name = (
            col.strip().split("(")[1].replace(")", "").split(" ")[1].replace("*", "")
        )
        return "20" + col_name if col_name != "kategorie" else col_name

    def clean_df(self, df: pd.DataFrame, company: str) -> pd.DataFrame:
        """Clean and process the DataFrame by renaming, dropping, transforming, and adding necessary columns."""
        df = df.rename({"Unnamed: 0": " ( kategorie"}, axis=1)
        df = df.drop(columns=[col for col in df.columns if "Unnamed" in col])

        df.columns = [self.correct_col_name(col) for col in df.columns]

        df = df.T
        df.columns = df.iloc[0]
        df = df.iloc[1:]
        df.columns = rename_duplicates(df.columns)

        for col in df.columns:
            df[col] = df[col].apply(lambda x: self.clean_data(x))

        df["firma"] = company
        return df.fillna(0)

    def extract_company_name(self, url: str) -> str:
        """Extract the company name from the provided URL."""
        return url.split("/")[-1]

    def extract_data(self, company: str, response) -> pd.DataFrame:
        """Attempt to extract and clean data from multiple HTML tables until successful."""
        df = pd.DataFrame()

        for i in range(4):  # Attempt up to 4 tables
            try:
                df = pd.read_html(response.body)[i]
                df = self.clean_df(df, company)

                break
            except Exception as e:
                print(f"Error processing table index {i}: {e}")

        return df

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
