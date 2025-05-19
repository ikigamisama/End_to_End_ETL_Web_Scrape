import math
import re
import json
import asyncio
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class MxmemoxpressETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        product = json.loads(
            soup.find('script', attrs={'type': 'application/ld+json'}).get_text())

        product_shop = self.SHOP
        product_name = product['name']
        product_brand = product['name'].split(' ')[0]
        product_rating = '0/5'

        if product.get('aggregateRating') is not None:
            product_rating = product['aggregateRating']['ratingValue']

        product_description = product['description']
        product_url = url

        product_image_url = []
        product_variant = []
        prices = []
        discounted_price = []
        discount_percentage = []

        variant = json.loads(
            soup.find('form', class_="variations_form").get('data-product_variations'))

        for v in variant:
            product_image_url.append(v['image']['src'])

            variant_name = v.get('attributes', {})
            color = variant_name.get('attribute_color')
            storage = variant_name.get('attribute_pa_storage')

            combined = " / ".join([val for val in [color, storage] if val])
            product_variant.append(combined)

            price = None
            discount_price = None
            discount_percent = None
            if v['display_price'] != v['display_regular_price']:
                price = v['display_regular_price']
                discount_price = v['display_price']
                discount_percent = (price - discount_price) / price
            else:
                price = v['display_price']
                discount_price = None
                discount_percent = None

            prices.append(price)
            discounted_price.append(discount_price)
            discount_percentage.append(discount_percent)

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

        spec_div = soup.find('div', class_="et_pb_post_content")
        lis = spec_div.find_all('li')

        for li in lis:
            text = li.get_text(strip=True).lower()

            if "display" in text:
                feature_data['screen_size'] = text.split(":")[1].strip()
            elif "chip" in text or "processor" in text:
                feature_data['processor'] = text.split(":")[1].strip()
            elif "ram" in text:
                feature_data['memory'] = text.split(":")[1].strip()
            elif "rear camera" in text or "front camera" in text or "camera" in text:
                feature_data['camera'] = text.split(":")[1].strip()
            elif "battery" in text:
                feature_data['battery'] = text.split(":")[1].strip()
            elif "sim" in text:
                feature_data['sim_slot'] = text.split(":")[1].strip()

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
        soup = asyncio.run(self.extract_scrape_content(url, '#main-content'))

        urls = [product.find('a').get('href') for product in soup.find_all(
            'li', attrs={'class': ["product", "type-product", 'status-publish ']})]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
