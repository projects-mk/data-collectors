import requests
import os
from pandas import DataFrame
import time
import sqlalchemy

def generate_conn_string(db: str) -> str:

    url = os.environ["VAULT_URL"]
    token = os.environ["VAULT_TOKEN"]

    resp = requests.get(url, headers={"X-Vault-Token": token}).json()
    if not os.getenv("IS_TEST_ENV"):
        return resp["data"]["data"]["postgres"] + db

    return resp["data"]["data"]["postgres"] + "test_db"


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
]

def save_to_db(df: DataFrame, table_name:str, conn_str: str, if_exist: str):

    retries = 0
    while True:
        if retries>=5:
            break 
        try:
            df.to_sql(table_name, con=conn_str, if_exists=if_exist)
            break
        
        except sqlalchemy.exc.OperationalError:
            time.sleep(15)
            retries+=1