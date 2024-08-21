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
from utils import user_agents


warsaw_tz = pytz.timezone("Europe/Warsaw")
current_time_warsaw = datetime.now(tz=warsaw_tz).strftime("%d-%m-%Y")


load_dotenv()
conn_str = os.getenv("PG_CONN_STRING")


class CompaniesDataSpider(scrapy.Spider):
    name = "companies-data"
    allowed_domains = ["biznesradar.pl"]
    start_urls = ["https://biznesradar.pl"]

    def parse(self, response):
        pass
