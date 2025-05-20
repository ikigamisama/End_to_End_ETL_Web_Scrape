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
        product_data = json.loads(soup.find('main').find(
            'script', attrs={'type': 'application/ld+json'}).get_text())
        product_shop = self.SHOP
        product_name = product_data['name']
        product_brand = product_data['brand']['name']
        product_rating = '0/5'

        product_description = product_data['description']
        product_url = url
        product_variant = soup.find(
            'div', class_="t4s-sku-wrapper").find('span').get_text()
        product_image_url = soup.find(
            'meta', attrs={'property': 'og:image'}).get('content')

        price_component = json.loads(soup.find('div', class_="t4s-main-area").find(
            'div', attrs={'data-t4s-zoom-main': True}).get('data-product-featured'))

        if price_component['compare_at_price'] is None:
            price = price_component['price'] / 100
            discounted_price = None
            discount_percentage = None
        else:
            price = price_component['compare_at_price'] / 100
            discounted_price = price_component['price'] / 100
            discount_percentage = round(
                (float(price) - float(discounted_price)) / float(price), 2)

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

        spec_data = soup.find('table', class_='MsoNormalTable')
        rows = spec_data.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 2:
                continue
            label = cells[0].get_text(strip=True)
            value = cells[1].get_text(separator=" ", strip=True)

            if 'Display' in label and 'Screen Size' in value:
                match = re.search(r'Screen Size:\s*([\d.]+)"', value)
                if match:
                    feature_data['screen_size'] = match.group(1)

            elif 'Display' in label:
                match = re.search(r'([\d.]+)[”"]', value)
                if match:
                    feature_data['screen_size'] = float(match.group(1))

            elif 'Chipset' in label or 'Processor' in label or 'Chip' in label:
                feature_data['processor'] = value

            elif 'Memory' in label:
                feature_data['memory'] = value

            elif 'Memory & Storage' in label:
                feature_data['memory'] = value

            elif 'Camera' in label:
                feature_data['camera'] = value

            elif ('Battery' in label or 'Power' in label or 'Charging' in label or 'Battery' in value):
                match = re.search(r'(\d{4,5})\s?mAh', value, re.IGNORECASE)
                if match:
                    feature_data['battery'] = match.group(1)

            elif 'Weight & Dimensions' in label or 'Size & Weight' in label:
                # Extract weight in grams
                match_weight = re.search(r'(\d+(\.\d+)?)\s*g', value)
                if match_weight:
                    feature_data['net_weight'] = float(match_weight.group(1))

                # Extract dimensions: format can be 163.8 x 76.8 x 8.9 mm
                match_dimensions = re.search(
                    r'([\d.]+)\s*x\s*([\d.]+)\s*x\s*([\d.]+)\s*mm', value, re.IGNORECASE)
                if match_dimensions:
                    feature_data['length'] = float(match_dimensions.group(1))
                    feature_data['width'] = float(match_dimensions.group(2))
                    feature_data['height'] = float(match_dimensions.group(3))

            elif 'Ports' in label and 'SIM' in value:
                match = re.search(r'(\d+)\s*Nano SIM', value, re.IGNORECASE)
                if match:
                    feature_data['sim_slot'] = match.group(1)

            elif 'Height' in label:
                feature_data['height'] = int(re.sub(r'\D', '', value))

            elif 'Width' in label:
                feature_data['width'] = int(re.sub(r'\D', '', value))

            elif 'Weight' in label:
                feature_data['net_weight'] = int(re.sub(r'\D', '', value))

            elif "SIM Card" in label:
                feature_data['sim_slot'] = value

            elif "SIM Support" in label:
                feature_data['sim_slot'] = value

        data = {
            'shop': product_shop,
            'name': product_name,
            'brand': product_brand,
            'rating': product_rating,
            'description': product_description,
            'url': product_url,
            'variant': product_variant,
            'price': price,
            'discounted_price': discounted_price,
            'discount_percentage': discount_percentage,
            'image_url': product_image_url,
            'height': feature_data['height'],
            'width': feature_data['width'],
            'length': feature_data['length'],
            'gross_weight': feature_data['gross_weight'],
            'net_weight': feature_data['net_weight'],
            'screen_size': feature_data['screen_size'],
            'sim_slot': feature_data['sim_slot'],
            'processor': feature_data['processor'],
            'memory': feature_data['memory'],
            'camera': feature_data['camera'],
            'battery': feature_data['battery']
        }

        return pd.DataFrame([data])

    def extract_links(self, url: str) -> pd.DataFrame:
        soup = asyncio.run(self.extract_scrape_content(url, '#MainContent'))
        urls = [self.URL + product.find('a').get('href') for product in soup.find(
            'div', class_="t4s_box_pr_grid").find_all('div', class_="t4s-product")]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
