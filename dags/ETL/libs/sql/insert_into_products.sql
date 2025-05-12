INSERT INTO etl.products (
    shop_id,
    name,
    brand,
    rating,
    description,
    url,
    image_url
)
SELECT DISTINCT
    s.id AS shop_id,
    a.name,
    a.brand,
    a.rating,
    a.description,
    a.url,
    a.image_url
FROM etl.stg_products a
JOIN etl.shops s ON s.name = a.shop
LEFT JOIN etl.products p ON p.url = a.url AND p.shop_id = s.id
WHERE p.id IS NULL;
