import asyncio
import nest_asyncio
import math
import re
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class AnsonsETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        product_shop = self.SHOP
        product_name = soup.find('h1', class_="product_title").get_text()
        product_brand = soup.find(
            'meta', attrs={'property': 'product:brand'}).get('content')

        product_rating = '0/5'

        if soup.find('div', class_="product-rating-summary"):
            product_rating = soup.find(
                'div', class_="product-rating-summary").find('h3').get_text().split(' out')[0] + '/5'

        product_description = soup.find(
            'meta', attrs={'name': 'description'}).get('content')
        product_url = url
        product_variant = None
        product_image_url = soup.find(
            'meta', attrs={'property': 'og:image'}).get('content')

        discount_price_soup = soup.find('p', class_='price').find('del')
        if discount_price_soup:
            price = float(discount_price_soup.get_text(
                strip=True).replace('₱', '').replace(',', ''))
            discounted_price = float(soup.find('p', class_='price').find(
                'ins').get_text(strip=True).replace('₱', '').replace(',', ''))
            discount_percentage = float(soup.find('p', class_='price').find(
                'span', class_='discount').get_text(strip=True).replace('-', '').replace('%', '')) / 100

        else:
            price = float(soup.find('p', class_='price').get_text(
                strip=True).replace('₱', '').replace(',', ''))
            discounted_price = None
            discount_percentage = None

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

        spec_section = soup.find('div', id="tab-specification")
        if not spec_section:
            return feature_data

        table1 = spec_section.find(
            'table', class_="woocommerce-product-attributes")
        if table1:
            for row in table1.find_all('tr'):
                label = row.find('th').get_text(strip=True).lower()
                value = row.find('td').get_text(strip=True)

                if 'weight' in label and 'kg' in value:
                    weight = float(value.replace('kg', '').strip())
                    feature_data['gross_weight'] = weight
                    feature_data['net_weight'] = weight

                elif 'dimensions' in label:
                    match = re.search(
                        r"L\s*([\d.]+)\s*x\s*W\s*([\d.]+)\s*x\s*H\s*([\d.]+)", value)
                    if match:
                        feature_data['length'] = float(match.group(1))
                        feature_data['width'] = float(match.group(2))
                    feature_data['height'] = float(match.group(3))

        tables = spec_section.find_all('table')
        if len(tables) > 1:
            table2 = tables[1]
            ram = rom = None
            front_camera = rear_camera = None

            for row in table2.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) != 2:
                    continue
                key = cells[0].get_text(strip=True).lower()
                val = cells[1].get_text(strip=True)

                if 'display' in key or 'screen' in key:
                    feature_data['screen_size'] = val

                elif 'processor' in key:
                    feature_data['processor'] = val

                elif 'ram' in key:
                    ram = val.upper()

                elif 'rom' in key:
                    rom = val.upper()

                elif 'internal storage' in key:
                    rom = val.upper()

                elif 'rear camera' in key:
                    rear_camera = val

                elif 'front camera' in key:
                    front_camera = val

                elif 'battery' in key:
                    feature_data['battery'] = val

            if ram and rom:
                feature_data['memory'] = f"{ram} RAM + {rom} ROM"

            if rear_camera or front_camera:
                camera = ""
                if rear_camera:
                    camera += f"Rear Camera: {rear_camera}"
                if front_camera:
                    camera += f" | Front Camera: {front_camera}"
                feature_data['camera'] = camera

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
        urls = []
        soup_product_list = asyncio.run(
            self.extract_scrape_content(url, '#main'))
        n_product = int(soup_product_list.find('span', class_="br_product_result_count").get(
            'data-text').split(' of ')[1].replace(' results', ''))
        pagination_page_num = math.ceil(n_product / 24)

        for i in range(1, pagination_page_num + 1):
            page_url = f"https://ansons.ph/product-category/smartphones/page/{i}/"
            product_list_soup = asyncio.run(
                self.extract_scrape_content(page_url, '#main'))

            urls.extend([product.find('a', class_="woocommerce-loop-product__link").get('href')
                        for product in product_list_soup.find_all('li', class_="type-product")])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
