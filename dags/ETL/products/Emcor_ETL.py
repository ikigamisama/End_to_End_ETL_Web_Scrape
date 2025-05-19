import math
import re
import json
import asyncio
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class EmcorETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        product_shop = self.SHOP
        product_name = soup.find(
            'meta', attrs={'property': 'og:title'}).get('content')
        product_brand = soup.find(
            'script', attrs={'data-flix-fallback-language': True}).get('data-flix-brand')
        product_rating = '0/5'

        product_description = soup.find(
            'meta', attrs={'name': 'description'}).get('content')
        product_url = url

        product_image_url = []
        product_variant = []
        prices = []
        discounted_price = []
        discount_percentage = []

        variant = json.loads(
            soup.find('form', class_="variations_form").get('data-product_variations'))

        for v in variant:
            product_variant.append(v['sku'])
            product_image_url.append(v['image']['src'])
            prices.append(v['display_price'])
            discounted_price.append(None)
            discount_percentage.append(None)

        feature_data = {
            'height': None,
            'width': None,
            'length': None,
            'gross_weight': None,
            'net_weight': None,
            'screen_size': None,
            'sim_slot': None,
            'processor': None,
            'memory': None,
            'camera': None,
            'battery': None
        }

        spec_div = soup.find('div', id='tab-description')
        text = spec_div.get_text(separator='\n', strip=True)
        patterns = {
            'height': r'Height:\s*([\d.]+)\s*mm',
            'width': r'Width:\s*([\d.]+)\s*mm',
            'length': r'(?:Depth|Length):\s*([\d.]+)\s*mm',
            'gross_weight': r'Weight:\s*(\d+\.?\d*)\s*g',
            'net_weight': r'Net Weight:\s*(\d+\.?\d*)\s*g',
            'screen_size': r'Size:\s*([\d.]+)\s*inches|(\d+\.\d+‑inch)',
            'sim_slot': r'SIM.*(?:Dual SIM|nano-SIM|eSIM)',
            'processor': r'(CPU Model|Chip|Processor):\s*(.*?)(?:\n|$)',
            'memory': r'Memory.*\n•\s*([\dA-Z +]+)',
            'camera': r'(?:Camera|Rear Camera|Advanced dual-camera system|TrueDepth Camera)',
            'battery': r'Battery.*\n•\s*Capacity:?\s*(.*?)(?:\n|$)|Power and Battery.*Video playback:.*'
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                feature_data[key] = next(
                    (g for g in match.groups() if g), match.group())

        df = pd.DataFrame({
            'variant': product_variant,
            'image_url': product_image_url,
            'price': prices,
            'discounted_price': discounted_price,
            'discount_percentage': discount_percentage,
        })
        df.insert(0, 'shop', product_shop)
        df.insert(0, 'name', product_name)
        df.insert(0, 'brand', product_brand)
        df.insert(0, 'rating', product_rating)
        df.insert(0, 'description', product_description)
        df.insert(0, 'url', product_url)
        df.insert(0, 'height', feature_data['height'])
        df.insert(0, 'width', feature_data['width'])
        df.insert(0, 'length', feature_data['length'])
        df.insert(0, 'gross_weight', feature_data['gross_weight'])
        df.insert(0, 'net_weight', feature_data['net_weight'])
        df.insert(0, 'screen_size', feature_data['screen_size'])
        df.insert(0, 'sim_slot', feature_data['sim_slot'])
        df.insert(0, 'processor', feature_data['processor'])
        df.insert(0, 'memory', feature_data['memory'])
        df.insert(0, 'camera', feature_data['camera'])
        df.insert(0, 'battery', feature_data['battery'])

        return df

    def extract_links(self, url: str) -> pd.DataFrame:
        urls = []
        soup_product_list = asyncio.run(
            self.extract_scrape_content(url, '#content-area'))
        tag = soup_product_list.find('p', class_="woocommerce-result-count")
        total_results = int(
            re.search(r'of\s+(\d+)', tag.text).group(1)) if tag else None
        n_pagination = math.ceil(total_results / 20)

        for i in range(1, n_pagination + 1):
            page_url = f"https://emcor.com.ph/product-category/it-products/smartphone/page/{i}/"
            product_list_soup = asyncio.run(
                self.extract_scrape_content(page_url, '#content-area'))

            urls.extend([product.find('a', class_="woocommerce-loop-product__link").get('href')
                        for product in product_list_soup.find_all('li', class_="type-product")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
