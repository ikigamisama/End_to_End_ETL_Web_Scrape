import math
import re
import json
import asyncio
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class SavenearnETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        product_data = json.loads(soup.find_all(
            'script', attrs={'type': 'application/ld+json'})[0].get_text())
        product_shop = self.SHOP
        product_name = product_data['name']
        product_brand = product_data['brand']['name']
        product_rating = '0/5'

        product_description = product_data['description']
        product_url = url
        product_image_url = soup.find(
            'meta', attrs={'property': 'og:image'}).get('content')
        variant_component = json.loads(
            soup.find('div', id="widget-fave-html").find('div').get('data-params'))

        product_variant = []
        prices = []
        discounted_price = []
        discount_percentage = []

        for variant in variant_component['variants']:
            if variant['compare_price'] is None:
                price = variant['price']
                discount_price = None
                discount_percent = None
            else:
                price = variant['compare_price']
                discount_price = variant['price']
                discount_percent = round(
                    (float(price) - float(discount_price)) / float(price), 2)

            product_variant.append(variant['title'])
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

        spec_div = soup.find(
            'div', class_="product-block-list__item--description")
        text = re.sub(r'\s+', ' ', spec_div.get_text(separator='\n'))

        # -------- Dimensions (height, width, length)
        dim_match = re.search(
            r'Dimensions\s+(\d+\.?\d*) x (\d+\.?\d*) x (\d+\.?\d*)\s*mm', text)
        if dim_match:
            feature_data['height'] = float(dim_match.group(1))
            feature_data['width'] = float(dim_match.group(2))
            feature_data['length'] = float(dim_match.group(3))

        # -------- Weight
        weight_match = re.search(r'Weight\s+(\d+)\s*g', text)
        if weight_match:
            feature_data['net_weight'] = int(weight_match.group(1))

        # -------- Screen Size
        screen_match = re.search(r'Size\s+(\d+\.?\d*)\s*inches', text)
        if screen_match:
            feature_data['screen_size'] = float(screen_match.group(1))

        # -------- SIM Slot
        sim_match = re.search(r'SIM\s+(.*?)Display', text)
        if sim_match:
            feature_data['sim_slot'] = sim_match.group(1).strip()

        # -------- Processor
        processor_match = re.search(r'Chipset\s+(.*?)CPU', text)
        if processor_match:
            feature_data['processor'] = processor_match.group(1).strip()

        # -------- Memory
        memory_match = re.search(r'Internal\s+([0-9A-Za-z\s,]+)\s+NVMe', text)
        if memory_match:
            feature_data['memory'] = memory_match.group(1).strip()

        # -------- Camera
        camera_match = re.search(r'Main Camera\s+Single\s+(.*?)Features', text)
        if camera_match:
            feature_data['camera'] = camera_match.group(1).strip()

        # -------- Battery
        battery_match = re.search(
            r'Battery\s+Type\s+Li-Ion\s+(\d+)\s*mAh', text)
        if battery_match:
            feature_data['battery'] = int(battery_match.group(1))

        df = pd.DataFrame({
            'variant': product_variant,
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
        df.insert(0, 'image_url', product_image_url)
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
        soup = asyncio.run(self.extract_scrape_content(url, '#main'))
        urls = []
        n_product = int(soup.find(
            'span', class_="collection__showing-count").get_text().split('of ')[-1].replace(' products', ''))
        pagination_page_num = math.ceil(n_product / 24)

        for i in range(1, pagination_page_num + 1):
            page_url = f"https://savenearn.com.ph/collections/smartphone?page={i}"
            product_list_soup = asyncio.run(
                self.extract_scrape_content(page_url, '#main'))

            urls.extend([self.URL + product.find('a').get('href')
                        for product in product_list_soup.find_all('div', class_="product-item--vertical")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
