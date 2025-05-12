CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.stg_shops (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    base_url VARCHAR(100)
);

INSERT INTO etl.stg_shops (name, base_url)
VALUES 
    ('Ansons','https://ansons.ph/'),
    ('Abenson','https://www.abenson.com/l'),
    ('SavenEarn','https://savenearn.com.ph/'),
    ('CompAsia','https://compasia.com.ph/'),
    ('VivoGlobal','https://shop.vivoglobal.ph/'),
    ('Western','https://western.com.ph/shop/gadgets/smartphones/'),
    ('KimStore','https://www.kimstore.com/collections/smartphones'),
    ('PCX','https://pcx.com.ph/collections/smartphones'),
    ('MyPhone','https://www.myphone.com.ph/smartphone/'),
    ('Emcor','https://emcor.com.ph/product-category/it-products/smartphone/'),
    ('MxMemoXpress','https://mxmemoxpress.com/all-mobiles/');

CREATE TABLE IF NOT EXISTS etl.stg_urls (
    inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shop_id INT REFERENCES etl.stg_shops(id),
    url VARCHAR(255),
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl.urls (
    id SERIAL PRIMARY KEY,
    inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shop_id INT REFERENCES etl.stg_shops(id),
    url VARCHAR(255),
    scrape_status VARCHAR(25) DEFAULT 'NOT STARTED',
    updated_date TIMESTAMP
);

CREATE TABLE IF NOT EXISTS etl.stg_products (
    inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shop VARCHAR(50),
    name VARCHAR(255),
    brand VARCHAR(255),
    rating VARCHAR(50),
    description VARCHAR(1000),
    url VARCHAR(255),
    variant VARCHAR(255),
    price DECIMAL(10, 2),
    discounted_price DECIMAL(10, 2),
    discount_percentage DECIMAL(10, 2),
    image_url VARCHAR(255),
    height DECIMAL(10, 2),
    width DECIMAL(10, 2),
    length DECIMAL(10, 2),
    gross_weight DECIMAL(10, 2),
    net_weight DECIMAL(10, 2),
    screen_size VARCHAR(255),
    sim_slot VARCHAR(255),
    processor VARCHAR(255),
    memory VARCHAR(255),
    camera VARCHAR(1000),
    battery VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS etl.products (
    id SERIAL PRIMARY KEY,
    inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    shop_id INT REFERENCES etl.stg_shops(id),
    name VARCHAR(255),
    brand VARCHAR(255),
    rating VARCHAR(50),
    description VARCHAR(1000),
    url VARCHAR(255),
    image_url VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS etl.product_feature (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES etl.products(id),
    height DECIMAL(10, 2),
    width DECIMAL(10, 2),
    length DECIMAL(10, 2),
    gross_weight DECIMAL(10, 2),
    net_weight DECIMAL(10, 2),
    screen_size VARCHAR(255),
    sim_slot VARCHAR(255),
    processor VARCHAR(255),
    memory VARCHAR(255),
    camera VARCHAR(1000),
    battery VARCHAR(255),
    inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS etl.product_prices (
    id SERIAL PRIMARY KEY,
    product_id INT NOT NULL REFERENCES etl.products(id),
    shop_id INT NOT NULL REFERENCES etl.stg_shops(id),
    inserted_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    price DECIMAL(10, 2),
    discounted_price DECIMAL(10, 2),
    discount_percentage DECIMAL(10, 2)
);
