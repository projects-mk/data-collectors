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

load_dotenv()
conn_str = generate_conn_string(db="gpw_app")


class CompaniesListSpider(scrapy.Spider):
    name = "companies-list"
    allowed_domains = ["biznesradar.pl"]
    url = "https://www.biznesradar.pl/gielda/akcje_gpw"

    def start_requests(self):
        user_agent = random.choice(user_agents)
        yield scrapy.Request(
            url=self.url,
            headers={"User-Agent": user_agent},
            callback=self.parse,
        )

    def parse(self, response: Response):
        body = response.body

        dfs = pd.read_html(body)

        data = {}

        for el in dfs[0]["Profil"].tolist():
            try:
                val, key = el.split(" ")

            except (ValueError, IndexError):
                key, val = el, el

            val = val.strip()
            key = key.replace("(", "").replace(")", "").strip()
            data[key] = val

        pd.DataFrame([data]).T.reset_index().rename(
            {"index": "company", 0: "ticker"}, axis=1
        ).to_sql("list_of_companies", con=conn_str, if_exists="replace")
