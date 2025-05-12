UPDATE etl.urls 
SET scrape_status='{status}'
    ,updated_date='{timestamp}'
WHERE id={pkey}