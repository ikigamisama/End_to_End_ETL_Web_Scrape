import re
import json
import asyncio
import nest_asyncio
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL

nest_asyncio.apply()


class CompAsiaETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    def transform(self, soup: BeautifulSoup, url: str):
        data = json.loads(
            soup.find('script', attrs={'data-product-json': True}).string.strip())

        product_shop = 'CompAsia'
        product_name = data['product']['title']
        product_brand = data['product']['vendor']
        product_rating = '0/5'

        product_description = data['product']['description']
        product_url = url

        product_image_url = []
        product_variant = []
        prices = []
        discounted_price = []
        discount_percentage = []
        for variant in data['product']['variants']:
            product_variant.append(variant['title'])
            product_image_url.append(
                'https:' + variant['featured_image']['src'])

            price = variant['price'] / 100
            discount_price = variant['compare_at_price'] / 100
            if price != discount_price:
                prices.append(discount_price)
                discounted_price.append(price)

                discount_price = (discount_price - price) / discount_price
                discount_percentage.append("{:.2f}".format(discount_price))
            else:
                prices.append(price)
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

        spec_div = soup.find('div', id='pdp-product-spec')

        text = spec_div.get_text(separator='\n')

        dim_match = re.search(
            r'Dimensions:\s*(\d+(\.\d+)?)\s*x\s*(\d+(\.\d+)?)\s*x\s*(\d+(\.\d+)?)\s*mm', text)
        if dim_match:
            length, width, height = map(float, dim_match.groups()[::2])
            feature_data['length'] = length
            feature_data['width'] = width
            feature_data['height'] = height

        weight_match = re.search(r'Weight:\s*(\d+(\.\d+)?)\s*g', text)
        if weight_match:
            weight = float(weight_match.group(1))
            feature_data['gross_weight'] = weight
            feature_data['net_weight'] = weight

        sim_match = re.search(r'SIM:\s*(.+)', text)
        if sim_match:
            feature_data['sim_slot'] = sim_match.group(1).strip()

        cpu_match = re.search(r'CPU:\s*(.+)', text)
        if cpu_match:
            feature_data['processor'] = cpu_match.group(1).strip()

        ram_match = re.search(r'RAM:\s*(\d+GB)', text)
        rom_match = re.search(r'ROM:\s*(\d+GB)', text)
        if ram_match and rom_match:
            feature_data['memory'] = f"{ram_match.group(1)} RAM + {rom_match.group(1)} ROM"

        rear_camera_match = re.search(
            r'Rear Camera:\s*(.+?)(?=Selfie Camera:)', text, re.DOTALL)
        selfie_camera_match = re.search(r'Selfie Camera:\s*(.+)', text)
        camera_parts = []
        if rear_camera_match:
            camera_parts.append(
                "Rear: " + rear_camera_match.group(1).replace('\n', ' ').strip())
        if selfie_camera_match:
            camera_parts.append(
                "Front: " + selfie_camera_match.group(1).strip())
        if camera_parts:
            feature_data['camera'] = ' | '.join(camera_parts)

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
            self.extract_scrape_content(url, '#product-grid'))
        n_page = int(soup_product_list.find_all(
            'a', class_="pagination__nav-item")[-1].get_text())

        for i in range(1, n_page + 1):
            page_url = f"https://compasia.com.ph/collections/smartphones?page={i}"
            product_list_soup = asyncio.run(
                self.extract_scrape_content(page_url, '#product-grid'))

            urls.extend([self.URL + product.find('a').get('href')
                        for product in product_list_soup.find_all('div', attrs={'class': ["product-item", "product-item--vertical"]})])

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
