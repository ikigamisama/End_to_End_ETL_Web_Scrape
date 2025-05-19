import re
import json
import asyncio
import nest_asyncio

import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class KimStoreETL(ProductsETL):
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
            'span', class_="product__text-type").get_text(strip=True)
        product_rating = '0/5'

        product_description = soup.find(
            'meta', attrs={'property': 'og:description'}).get('content')
        product_url = url

        product_image_url = []
        product_variant = []
        prices = []
        discounted_price = []
        discount_percentage = []

        variant = json.loads(soup.find_all(
            'script', attrs={'type': 'application/json'})[-1].get_text())
        for v in variant:
            product_variant.append(v['name'])

            if v.get('feature_image') is None:
                product_image_url.append(soup.find(
                    'meta', attrs={'property': 'og:image'}).get('content'))
            else:
                product_image_url.append(v['featured_image']['src'])

            price = None
            discount_price = None
            discount_percent = None
            if v['price'] != v['compare_at_price']:
                price = v['compare_at_price'] / 100
                discount_price = v['price'] / 100
                discount_percent = (price - discount_price) / price
            else:
                price = v['price'] / 100
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

        div = soup.find_all('div', class_="about__accordion-description")[0]
        lines = div.get_text(separator='\n', strip=True).splitlines()

        for line in lines:
            # Normalize for matching
            norm_line = line.lower()

            # Dimensions
            if 'height' in norm_line and 'mm' in norm_line:
                match = re.search(r'height.*?([\d.]+)\s*mm', norm_line)
                if match:
                    feature_data['height'] = match.group(1)
            if 'width' in norm_line and 'mm' in norm_line:
                match = re.search(r'width.*?([\d.]+)\s*mm', norm_line)
                if match:
                    feature_data['width'] = match.group(1)
            if ('depth' in norm_line or 'length' in norm_line) and 'mm' in norm_line:
                match = re.search(
                    r'(?:depth|length).*?([\d.]+)\s*mm', norm_line)
                if match:
                    feature_data['length'] = match.group(1)

            # Weight
            if 'weight' in norm_line and 'g' in norm_line:
                match = re.findall(r'([\d,]+)\s*g', norm_line)
                if match:
                    # Assign first match to net_weight if not set, otherwise gross
                    weight = match[0].replace(',', '')
                    if not feature_data['net_weight']:
                        feature_data['net_weight'] = weight
                    elif not feature_data['gross_weight']:
                        feature_data['gross_weight'] = weight

            # Screen
            if 'display' in norm_line or 'screen' in norm_line:
                match = re.search(
                    r'(\d{1,2}\.?\d*)\s*(inches|inch)', norm_line)
                if match:
                    feature_data['screen_size'] = match.group(1)

            # SIM Slot
            if 'sim' in norm_line:
                if 'dual' in norm_line:
                    feature_data['sim_slot'] = 'Dual SIM'
                elif 'single' in norm_line:
                    feature_data['sim_slot'] = 'Single SIM'
                elif 'eSIM' in norm_line:
                    feature_data['sim_slot'] = 'eSIM'

            # Processor
            if any(term in norm_line for term in ['chip', 'chipset', 'processor', 'cpu']):
                feature_data['processor'] = line.strip()

            # Memory
            if 'ram' in norm_line and 'rom' in norm_line:
                feature_data['memory'] = line.strip()

            # Camera
            if 'camera' in norm_line:
                if not feature_data['camera']:
                    feature_data['camera'] = line.strip()

            # Battery
            if 'battery' in norm_line and not feature_data['battery']:
                match = re.search(r'(\d{3,5})\s*mAh', norm_line)
                if match:
                    feature_data['battery'] = match.group(1)
                else:
                    feature_data['battery'] = line.strip()

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
        soup = asyncio.run(self.extract_scrape_content(url, '#product-grid'))
        n_page = int(soup.find_all('a', class_="pagination__item")
                     [-1].find('span').get_text())

        for i in range(1, n_page + 1):
            page_url = f"https://www.kimstore.com/collections/smartphones?page={i}"
            product_list_soup = asyncio.run(
                self.extract_scrape_content(page_url, '#product-grid'))

            urls.extend([self.URL + product.find('a').get('href')
                        for product in product_list_soup.find_all('li', class_="collection-product-card")])
        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
