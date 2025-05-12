import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL


class VivoGlobalETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        pass

    def extract_links(self, category: str) -> pd.DataFrame:
        pass
