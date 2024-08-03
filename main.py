from fastapi.logger import logger
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
import os
import logging
from otomoto.main import OtomotoScraper
from typing import List, Dict

logging.basicConfig(level=logging.DEBUG)

gunicorn_logger = logging.getLogger("gunicorn.error")
logger.handlers = gunicorn_logger.handlers

if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)


app = FastAPI(title=os.getenv("APP_NAME"), version=os.getenv("APP_VERSION"))


def get_otomoto_data(start_page: int = 1, end_page: int = 500):
    engine = create_engine(os.environ["DB_CONNECTION_STRING"])
    if engine.connect():
        logger.info("Connected to database")
        engine.dispose()
    scraper = OtomotoScraper(start_page, end_page)
    scraper.run()
    scraper.to_sql(os.environ["DB_CONNECTION_STRING"], "otomoto_data")
    logger.info("Data saved to database")


@app.get("/api/v1/getdata/otomoto")
async def scrape_otomoto_data():
    pages_to_scrape = [(page, page) for page in range(1, 1001)]
    results: List[Dict] = []

    for start_page, end_page in pages_to_scrape:
        try:
            get_otomoto_data(start_page, end_page)
            results.append(
                {"start_page": start_page, "end_page": end_page, "status": "success"}
            )
        except Exception as error:
            logger.error(f"Error has occurred: {error}")
            results.append(
                {
                    "start_page": start_page,
                    "end_page": end_page,
                    "status": "error",
                    "error": str(error),
                }
            )

    return results


@app.get("/test/api/v1/getdata/otomoto")
async def scrape_otomoto_data_test():
    try:
        get_otomoto_data(1, 2)
        return {"msg": "success"}

    except Exception as error:
        logger.error(f"Error has occured {error}")
        raise HTTPException(status_code=500, detail=str(error))
