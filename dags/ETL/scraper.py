import os
import datetime as dt
from dotenv import load_dotenv
from ETL.libs.utils import get_db_conn, execute_query, get_sql_from_file
from ETL.products import (
    AbensonETL,
    AnsonsETL,
    CompAsiaETL,
    EmcorETL,
    KimStoreETL,
    MxmemoxpressETL,
    MyPhoneETL,
    PcxETL,
    SavenearnETL,
    VivoGlobalETL,
    WesternETL
)


load_dotenv(dotenv_path="/opt/airflow/.env")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASS")
DB_NAME = os.getenv("DB_NAME")
DB_DRIVER = os.getenv("DB_DRIVER")

engine = get_db_conn(
    DB_DRIVER,
    DB_USER,
    DB_PASSWORD,
    DB_HOST,
    DB_PORT,
    DB_NAME,
)


def launch_etl(shop: str, selector: str):
    start_time = dt.datetime.now()
    factory = {
        "Abenson": AbensonETL("Abenson", 'https://www.abenson.com', '/mobile/smartphone.html'),
        'Ansons': AnsonsETL("Ansons", 'https://ansons.ph', '/product-category/smartphones/'),
        "CompAsia": CompAsiaETL('CompAsia', 'https://compasia.com.ph', '/collections/smartphones'),
        'Emcor': EmcorETL("Emcor", 'https://emcor.com.ph', '/product-category/it-products/smartphone/'),
        "KimStore": KimStoreETL('KimStore', 'https://www.kimstore.com', '/collections/smartphones'),
        "MxMemoXpress": MxmemoxpressETL("MxMemoXpress", 'https://mxmemoxpress.com', '/all-mobiles/'),
        'MyPhone': MyPhoneETL("MyPhone", 'https://www.myphone.com.ph', '/smartphone/'),
        "PCX": PcxETL('PCX', 'https://pcx.com.ph', '/collections/smartphones'),
        "SavenEarn": SavenearnETL("SavenEarn", 'https://savenearn.com.ph', '/collections/smartphone'),
        'VivoGlobal': VivoGlobalETL("VivoGlobal", 'https://shop.vivoglobal.ph', '/collections/all-phones'),
        'Western': WesternETL("Western", 'https://western.com.ph', '/shop/gadgets/smartphones/'),
    }

    if shop in factory:
        execute_query(engine, "TRUNCATE TABLE stg_urls;")
        factory[shop].refresh_links(engine, "stg_urls")

        sql = get_sql_from_file("insert_into_urls.sql")
        execute_query(engine, sql)

        execute_query(engine, "TRUNCATE TABLE stg_pet_products;")
        factory[shop].run(engine, "stg_pet_products", selector)

        sql = get_sql_from_file("insert_into_pet_products.sql")
        execute_query(engine, sql)

        sql = get_sql_from_file("insert_into_pet_product_variants.sql")
        execute_query(engine, sql)

        sql = get_sql_from_file(
            "insert_into_pet_product_variant_prices.sql")
        execute_query(engine, sql)

        end_time = dt.datetime.now()
        duration = end_time - start_time
        print(
            f"{shop} (shop={shop}) has ended. Elapsed: {duration}")
    else:
        raise ValueError(
            f"Shop {shop} is not supported. Please pass a valid shop.")


if __name__ == "__main__":
    pass
