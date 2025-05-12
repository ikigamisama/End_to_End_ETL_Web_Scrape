INSERT INTO etl.urls (
    shop_id
    ,url
    ,updated_date
)
SELECT DISTINCT
    s.id AS shop_id,
    a.url,
    a.updated_date
FROM etl.stg_urls a
JOIN etl.shops s ON s.name = a.shop
LEFT JOIN etl.urls b ON b.url = a.url
WHERE b.id IS NULL;