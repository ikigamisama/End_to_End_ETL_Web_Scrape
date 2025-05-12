INSERT INTO etl.product_feature (
    product_id,
    height,
    width,
    length,
    gross_weight,
    net_weight,
    screen_size,
    sim_slot,
    processor,
    memory,
    camera,
    battery
)
SELECT DISTINCT
    p.id AS product_id,
    sp.height,
    sp.width,
    sp.length,
    sp.gross_weight,
    sp.net_weight,
    sp.screen_size,
    sp.sim_slot,
    sp.processor,
    sp.memory,
    sp.camera,
    sp.battery
FROM etl.stg_products sp
JOIN etl.shops s ON s.name = sp.shop
JOIN etl.products p ON p.shop_id = s.id AND p.url = sp.url;
