import math
import re
import json
import asyncio
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class MyPhoneETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        product_shop = self.SHOP
        product_name = soup.find(
            'meta', attrs={'property': 'og:title'}).get('content')
        product_brand = self.SHOP
        product_rating = '0/5'

        product_description = soup.find(
            'meta', attrs={'property': 'og:description'}).get('content')
        product_url = url
        product_variant = None
        product_image_url = soup.find(
            'meta', attrs={'itemprop': 'image'}).get('content')

        price_del_tag = soup.find('del')
        price_ins_tag = soup.find('ins')

        if price_del_tag and price_ins_tag:
            price = price_del_tag.get_text(
                strip=True).replace('₱', '').replace(',', '')
            discounted_price = price_ins_tag.get_text(
                strip=True).replace('₱', '').replace(',', '')
            discount_percentage = round(
                (float(price) - float(discounted_price)) / float(price), 2)
        else:
            price_tag = soup.find(
                'div', class_='w-post-elm product_field price')
            if price_tag:
                amount_tag = price_tag.find(
                    'span', class_="woocommerce-Price-amount")
                if amount_tag:
                    price = amount_tag.get_text(strip=True).replace(
                        '₱', '').replace(',', '')
                    discounted_price = None
                    discount_percentage = None
                else:
                    price = discounted_price = discount_percentage = None
            else:
                price = discounted_price = discount_percentage = None

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

        specs = soup.find_all(
            'div', class_='g-cols wpb_row  type_default valign_top vc_inner')

        for spec in specs:
            label_tag = spec.find('p')
            value_tag = spec.find('div', class_='w-post-elm')
            if label_tag and value_tag:
                label = label_tag.get_text(strip=True).lower()
                value = value_tag.get_text(strip=True)

                if 'height' in label:
                    feature_data['height'] = value
                elif 'width' in label:
                    feature_data['width'] = value
                elif 'length' in label:
                    feature_data['length'] = value
                elif 'gross' in label:
                    feature_data['gross_weight'] = value
                elif 'net' in label:
                    feature_data['net_weight'] = value
                elif 'screen' in label:
                    feature_data['screen_size'] = value
                elif 'sim' in label:
                    feature_data['sim_slot'] = value
                elif 'processor' in label:
                    feature_data['processor'] = value
                elif 'ram' in label or 'rom' in label or 'memory' in label:
                    feature_data['memory'] = value
                elif 'camera' in label:
                    feature_data['camera'] = value
                elif 'battery' in label:
                    feature_data['battery'] = value

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
        soup = asyncio.run(self.extract_scrape_content(url, '#page-content'))
        urls = [product.find('a').get('href') for product in soup.find_all(
            'article',  attrs={'class': ["product", 'type-product']})]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
