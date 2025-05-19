import math
import re
import json
import asyncio
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class PcxETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def extract_links(self, url: str) -> pd.DataFrame:
        soup = asyncio.run(self.extract_scrape_content(url, '#MainContent'))
        urls = [self.URL + product.find('a').get('href') for product in soup.find(
            'div', class_="t4s_box_pr_grid").find_all('div', class_="t4s-product")]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", "PcExpress")
        return df
