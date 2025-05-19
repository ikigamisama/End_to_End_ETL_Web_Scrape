import asyncio
import nest_asyncio
import random
import time
import pandas as pd
from bs4 import BeautifulSoup
from .products_etl import ProductsETL
from playwright.async_api import async_playwright
from fake_useragent import UserAgent
nest_asyncio.apply()

headers = {
    "Accept": 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    "User-Agent": UserAgent().random,
    'Priority': "u=0, i",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Ch-Ua": "\"Not(A:Brand\";v=\"99\", \"Opera GX\";v=\"118\", \"Chromium\";v=\"133\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"Windows\"",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1"
}


class AbensonETL(ProductsETL):
    def __init__(self, shop, url, extract_url_link):
        super().__init__()
        self.SHOP = shop
        self.URL = url
        self.EXTRACT_URL_LINK = extract_url_link

    async def _scroll_products(self, url):
        soup = None
        browser = None
        try:
            async with async_playwright() as p:
                browser_args = {
                    "headless": True,
                    "args": ["--disable-blink-features=AutomationControlled"]
                }

                browser = await p.chromium.launch(**browser_args)
                context = await browser.new_context(
                    user_agent=UserAgent().random,
                    viewport={"width": random.randint(
                        1200, 1600), "height": random.randint(800, 1200)},
                    locale="en-US"
                )

                page = await context.new_page()
                await page.set_extra_http_headers(headers)

                await page.goto(url, wait_until="domcontentloaded")
                await page.wait_for_selector('#root-product-list', timeout=30000)

                print(
                    "Starting to scrape the product list (Infinite scroll scrape)...")

                scroll_step = 1500
                scroll_delay = 5

                previous_count = 0
                same_count_retries = 0
                max_retries = 3

                while True:
                    # Scroll to the bottom
                    await page.evaluate(f'window.scrollBy(0, {scroll_step})')
                    await asyncio.sleep(scroll_delay)

                    # Check if the spinner exists
                    current_count = await page.evaluate("""
                        () => document.querySelectorAll('div.item-siminia-product-grid-item-3do').length
                    """)

                    print(f"Current item count: {current_count}")

                    if current_count > previous_count:
                        previous_count = current_count
                        scroll_step += scroll_step
                        same_count_retries = 0
                    else:
                        same_count_retries += 1
                        print(
                            f"No new items loaded. Retry {same_count_retries}/{max_retries}")

                        if same_count_retries >= max_retries:
                            print("No more items being loaded. Done scrolling.")
                            break

                print("Scraping complete. Extracting content...")

                rendered_html = await page.content()
                print(
                    f"Successfully extracted data from {url}"
                )
                soup = BeautifulSoup(rendered_html, "html.parser")
                return soup.find_all('div', class_="item-siminia-product-grid-item-3do")

        except Exception as e:
            print(f"An error occurred: {e}")

        finally:
            if browser:
                await browser.close()

    def transform(self, soup, url) -> pd.DataFrame:
        product_shop = self.SHOP
        product_name = soup.find(
            'h1', class_="productFullDetail-productName-2jb").get_text()
        product_brand = soup.find(
            'meta', attrs={'itemprop': 'brand'}).get('content')
        product_rating = soup.find(
            'span', class_="productReview-averageReview-qT6").get_text()
        product_description = soup.find(
            'meta', attrs={'itemprop': 'description'}).get('content')
        product_url = url
        product_variant = soup.find(
            'section', class_="productFullDetail-shortDesc-1L9").get_text()
        product_image_url = soup.find(
            'meta', attrs={'itemprop': 'image'}).get('content')

        discount_price_soup = soup.find(
            'span', class_="productFullDetail-specialPrice-1wb")
        if discount_price_soup:
            price_html = [s.get_text() for s in soup.find(
                'span', class_="productFullDetail-specialPrice-1wb").find_all('span')]
            price_str = ''.join(price_html[1:])
            price = float(price_str.replace(',', ''))

            discounted_price_html = [s.get_text() for s in soup.find(
                'span', class_="productFullDetail-regularPrice-188").find_all('span')]
            discounted_price_str = ''.join(discounted_price_html[1:])
            discounted_price = float(discounted_price_str.replace(',', ''))

            discount_percentage = float(soup.find(
                'span', class_="productFullDetail-saleOff-a4h").get_text().replace('-', '').replace('%', '')) / 100
        else:
            price_html = [s.get_text() for s in soup.find(
                'span', class_="productFullDetail-regularPrice-188").find_all('span')]
            price_str = ''.join(price_html[1:])
            price = float(price_str.replace(',', ''))

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
        feature_blocks = soup.find_all('div', class_='features-block-2mF')
        for block in feature_blocks:
            title = block.find('div', class_='features-blockTitle-hWK')
            if title and title.get_text(strip=True).lower() == 'highlights':
                highlights = block.find_all(
                    'div', class_='features-highlight-1V0')
                for item in highlights:
                    spans = item.find_all('span')
                    if len(spans) >= 3:
                        label = spans[0].get_text(strip=True).lower()
                        value = spans[2].get_text(strip=True)

                        if 'height' in label:
                            feature_data['height'] = float(value)
                        elif 'width' in label:
                            feature_data['width'] = float(value)
                        elif 'length' in label:
                            feature_data['length'] = float(value)
                        elif 'gross weight' in label:
                            feature_data['gross_weight'] = float(value)
                        elif 'net weight' in label:
                            feature_data['net_weight'] = float(value)
                        elif 'screen size' in label:
                            feature_data['screen_size'] = value
                        elif 'sim slot' in label:
                            feature_data['sim_slot'] = value
                        elif 'processor' in label:
                            feature_data['processor'] = value
                        elif 'memory' in label:
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
        soup_product_list = asyncio.run(self._scroll_products(url))
        urls = [self.URL +
                product_html.find('a').get('href') for product_html in soup_product_list]

        df = pd.DataFrame({"url": urls})
        df.insert(0, "shop", self.SHOP)
        return df
