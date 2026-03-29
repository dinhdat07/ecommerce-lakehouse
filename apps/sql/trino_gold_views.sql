-- Trino-facing serving views for the future Iceberg catalog.
-- These views mirror the locally materialized Gold contracts.

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.v_user_activity_summary AS
SELECT
    event_date,
    user_id,
    total_events,
    views,
    carts,
    purchases,
    purchase_revenue,
    distinct_products,
    first_event_time,
    last_event_time
FROM iceberg.analytics.user_activity_summary;

CREATE OR REPLACE VIEW analytics.v_product_popularity AS
SELECT
    event_date,
    product_id,
    category_id,
    category_code,
    brand,
    views,
    carts,
    purchases,
    purchase_revenue,
    unique_users,
    view_to_purchase_rate
FROM iceberg.analytics.product_popularity;

CREATE OR REPLACE VIEW analytics.v_conversion_funnel AS
SELECT
    event_date,
    views,
    carts,
    purchases,
    view_to_cart_rate,
    cart_to_purchase_rate,
    view_to_purchase_rate
FROM iceberg.analytics.conversion_funnel;

CREATE OR REPLACE VIEW analytics.v_revenue_by_category AS
SELECT
    event_date,
    category_id,
    category_code,
    purchase_count,
    purchase_revenue,
    unique_buyers,
    average_order_value
FROM iceberg.analytics.revenue_by_category;
