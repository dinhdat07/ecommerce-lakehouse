-- Legacy demo bootstrap for the earlier Trino-view flow.
-- Phase 1 Bronze to Silver to Gold materialization now runs in Spark and writes
-- physical Iceberg tables directly, so this file is no longer part of the
-- active end-to-end path.

CREATE SCHEMA IF NOT EXISTS iceberg.demo;

CREATE OR REPLACE VIEW iceberg.demo.silver_events AS
WITH normalized AS (
    SELECT
        CAST(from_iso8601_timestamp(event_time) AS timestamp(6) with time zone) AS event_time_utc,
        CAST(CAST(from_iso8601_timestamp(event_time) AS timestamp(6) with time zone) AS date) AS event_date,
        lower(trim(event_type)) AS event_type,
        CAST(product_id AS bigint) AS product_id,
        CAST(category_id AS bigint) AS category_id,
        NULLIF(trim(category_code), '') AS category_code,
        NULLIF(trim(brand), '') AS brand,
        CAST(price AS double) AS price,
        CAST(user_id AS bigint) AS user_id,
        user_session,
        ingested_at,
        source_file,
        record_hash,
        row_number() OVER (
            PARTITION BY record_hash, user_session
            ORDER BY from_iso8601_timestamp(event_time)
        ) AS row_num
    FROM iceberg.demo.bronze_events
)
SELECT
    event_time_utc,
    event_date,
    event_type,
    product_id,
    category_id,
    category_code,
    brand,
    price,
    user_id,
    user_session,
    ingested_at,
    source_file,
    record_hash
FROM normalized
WHERE row_num = 1;

CREATE OR REPLACE VIEW iceberg.demo.gold_revenue_by_category AS
SELECT
    event_date,
    category_code,
    count_if(event_type = 'purchase') AS purchase_count,
    round(sum(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END), 2) AS purchase_revenue,
    count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS unique_buyers
FROM iceberg.demo.silver_events
GROUP BY 1, 2;

CREATE OR REPLACE VIEW iceberg.demo.gold_conversion_funnel AS
WITH daily AS (
    SELECT
        event_date,
        count_if(event_type = 'view') AS views,
        count_if(event_type = 'cart') AS carts,
        count_if(event_type = 'purchase') AS purchases
    FROM iceberg.demo.silver_events
    GROUP BY 1
)
SELECT
    event_date,
    views,
    carts,
    purchases,
    CASE WHEN views = 0 THEN NULL ELSE round(carts / CAST(views AS double), 4) END AS view_to_cart_rate,
    CASE WHEN carts = 0 THEN NULL ELSE round(purchases / CAST(carts AS double), 4) END AS cart_to_purchase_rate,
    CASE WHEN views = 0 THEN NULL ELSE round(purchases / CAST(views AS double), 4) END AS view_to_purchase_rate
FROM daily;
