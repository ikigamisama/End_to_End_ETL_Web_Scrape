import random
import requests
import pandas as pd
from datetime import datetime as dt
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from sqlalchemy.engine import Engine
from ETL.libs.utils import get_sql_from_file, update_url_scrape_status, execute_query
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random,
)
import asyncio
import nest_asyncio
from playwright.async_api import async_playwright
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
nest_asyncio.apply()

MAX_RETRIES = 10
MAX_WAIT_BETWEEN_REQ = 2
MIN_WAIT_BETWEEN_REQ = 0
REQUEST_TIMEOUT = 30

headers = {
    "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    "User-Agent": UserAgent().random,
    'Priority': "u=0, i",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"99\", \"Opera GX\";v=\"118\", \"Chromium\";v=\"133\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1"
}


class ProductsETL(ABC):
    def __init__(self):
        self.session = requests.Session()
        self.SHOP = ""
        self.URL = ""
        self.EXTRACT_URL_LINK = ""
        self.HEADERS = headers

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    async def extract_scrape_content(url, selector):
        soup = None
        browser = None
        try:
            async with async_playwright() as p:
                browser_args = {
                    "headless": True,
                    "args": ["--disable-blink-features=AutomationControlled"]
                }

                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(
                    locale="en-US",
                    user_agent=UserAgent().random,
                    viewport={"width": 1280, "height": 800},
                    device_scale_factor=1,
                    is_mobile=False,
                    has_touch=False,
                    screen={"width": 1280, "height": 800},
                    permissions=["geolocation"],
                    geolocation={"latitude": 14.5995, "longitude": 120.9842},
                    timezone_id="Asia/Manila"
                )

                page = await context.new_page()

                await page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                await page.set_extra_http_headers(headers)
                await page.goto(url, wait_until="networkidle")
                await page.wait_for_selector(selector, timeout=300000)

                for _ in range(random.randint(3, 6)):
                    await page.mouse.wheel(0, random.randint(300, 700))
                    await asyncio.sleep(random.uniform(0.5, 1))

                for _ in range(random.randint(5, 10)):
                    await page.mouse.move(random.randint(0, 800), random.randint(0, 600))
                    await asyncio.sleep(random.uniform(0.5, 1))

                rendered_html = await page.content()
                return BeautifulSoup(rendered_html, "html.parser")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    @retry(
        wait=wait_random(min=MIN_WAIT_BETWEEN_REQ, max=MAX_WAIT_BETWEEN_REQ),
        stop=stop_after_attempt(MAX_RETRIES),
        retry=retry_if_exception_type(requests.RequestException),
        reraise=True,
    )
    def extract_from_url(self, method: str, url: str, params: dict = None, data: dict = None, headers: dict = None, verify: bool = True) -> BeautifulSoup:
        try:
            # Parse request response
            response = self.session.request(
                method=method, url=url, params=params, data=data, headers=headers, verify=verify)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, "html.parser")
            print(
                f"Successfully extracted data from {url} {response.status_code}"
            )
            sleep_time = random.uniform(
                MIN_WAIT_BETWEEN_REQ, MAX_WAIT_BETWEEN_REQ)
            print(f"Sleeping for {sleep_time} seconds...")
            return soup

        except Exception as e:
            print(f"Error in parsing {url}: {e}")

    @abstractmethod
    def extract_links(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def transform(self, soup: BeautifulSoup, url: str) -> pd.DataFrame:
        pass

    def extract_from_sql(self, db_conn: Engine, sql: str) -> pd.DataFrame:
        try:
            return pd.read_sql(sql, db_conn)

        except Exception as e:
            print(e)
            raise e

    def load(self, data: pd.DataFrame, db_conn: Engine, table_name: str):
        try:
            n = data.shape[0]
            data.to_sql(table_name, db_conn, if_exists="append", index=False)
            print(
                f"Successfully loaded {n} records to the {table_name}.")

        except Exception as e:
            print(e)
            raise e

    def run(self, db_conn: Engine, table_name: str, selector: str = None):
        sql = get_sql_from_file("select_unscraped_urls.sql")
        sql = sql.format(shop=self.SHOP)
        df_urls = self.extract_from_sql(db_conn, sql)

        for i, row in df_urls.iterrows():
            pkey = row["id"]
            url = row["url"]

            now = dt.now().strftime("%Y-%m-%d %H:%M:%S")

            soup = asyncio.run(self.extract_scrape_content(url, selector))
            df = self.transform(soup, url)

            if df is not None:
                self.load(df, db_conn, table_name)
                update_url_scrape_status(db_conn, pkey, "DONE", now)

            else:
                update_url_scrape_status(db_conn, pkey, "FAILED", now)

    def refresh_links(self, db_conn: Engine, table_name: str):
        df = self.extract_links(self.URL + self.EXTRACT_URL_LINK)
        if df is not None:
            self.load(df, db_conn, table_name)

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(db_conn, sql)
