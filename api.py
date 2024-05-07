from fastapi.logger import logger
from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine
import os
import logging
from otomoto.main import OtomotoScraper

logging.basicConfig(level=logging.DEBUG)

gunicorn_logger = logging.getLogger('gunicorn.error')
logger.handlers = gunicorn_logger.handlers
if __name__ != "main":
    logger.setLevel(gunicorn_logger.level)
else:
    logger.setLevel(logging.DEBUG)

app = FastAPI()


def get_otomoto_data(max_pages: int):
    engine = create_engine(os.environ['DB_CONNECTION_STRING'])
    if engine.connect():
        logger.info('Connected to database')
        engine.dispose()
    scraper = OtomotoScraper(max_pages)
    scraper.run()
    scraper.to_sql(os.environ['DB_CONNECTION_STRING'], 'otomoto_data')
    logger.info('Data saved to database')


@app.get("/api/v1/getdata/otomoto")
async def scrape_otomoto_data():
    try:
        get_otomoto_data(500)
        return {'msg': 'success'}
    
    except Exception as error:
        logger.error(f"Error has occured {error}")
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/test/api/v1/getdata/otomoto")
async def scrape_otomoto_data_test():
    try:
        get_otomoto_data(5)
        return {'msg': 'success'}
    
    except Exception as error:
        logger.error(f"Error has occured {error}")
        raise HTTPException(status_code=500, detail=str(error))