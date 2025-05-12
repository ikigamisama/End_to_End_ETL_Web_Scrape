INSERT INTO etl.product_prices (
    product_id,
    shop_id,
    price,
    discounted_price,
    discount_percentage
)
SELECT DISTINCT
    p.id AS product_id,
    s.id AS shop_id,
    sp.price,
    sp.discounted_price,
    sp.discount_percentage
FROM etl.stg_products sp
JOIN etl.shops s ON s.name = sp.shop
JOIN etl.products p ON p.shop_id = s.id AND p.url = sp.url;
