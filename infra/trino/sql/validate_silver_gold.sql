-- Silver validation
SHOW TABLES FROM iceberg.demo;

SELECT count(*) AS silver_rows FROM iceberg.demo.silver_events;

SELECT count(*) AS duplicate_dedupe_keys
FROM (
    SELECT dedupe_key
    FROM iceberg.demo.silver_events
    GROUP BY 1
    HAVING count(*) > 1
);

SELECT
    count_if(event_time IS NULL) AS null_event_time_rows,
    count_if(event_date IS NULL) AS null_event_date_rows,
    count_if(event_type IS NULL) AS null_event_type_rows,
    count_if(event_type NOT IN ('view', 'cart', 'purchase')) AS unexpected_event_type_rows
FROM iceberg.demo.silver_events;

SELECT event_type, count(*) AS rows
FROM iceberg.demo.silver_events
GROUP BY 1
ORDER BY 2 DESC;

SELECT event_date, min(event_time) AS first_event_time, max(event_time) AS last_event_time, count(*) AS rows
FROM iceberg.demo.silver_events
GROUP BY 1
ORDER BY 1;

-- Gold row counts
SELECT
    (SELECT count(*) FROM iceberg.demo.daily_revenue) AS daily_revenue_rows,
    (SELECT count(*) FROM iceberg.demo.top_products) AS top_products_rows,
    (SELECT count(*) FROM iceberg.demo.conversion_funnel_daily) AS conversion_funnel_daily_rows,
    (SELECT count(*) FROM iceberg.demo.category_performance_daily) AS category_performance_daily_rows,
    (SELECT count(*) FROM iceberg.demo.session_funnel) AS session_funnel_rows,
    (SELECT count(*) FROM iceberg.demo.user_conversion_path) AS user_conversion_path_rows,
    (SELECT count(*) FROM iceberg.demo.cohort_retention) AS cohort_retention_rows,
    (SELECT count(*) FROM iceberg.demo.repeat_purchase) AS repeat_purchase_rows,
    (SELECT count(*) FROM iceberg.demo.product_affinity) AS product_affinity_rows,
    (SELECT count(*) FROM iceberg.demo.time_to_conversion_distribution) AS time_to_conversion_distribution_rows,
    (SELECT count(*) FROM iceberg.demo.rfm_segmentation) AS rfm_segmentation_rows;

-- daily_revenue should match Silver purchase aggregates
SELECT
    s.event_date,
    s.purchase_count AS silver_purchase_count,
    g.purchase_count AS gold_purchase_count,
    s.purchase_revenue AS silver_purchase_revenue,
    g.purchase_revenue AS gold_purchase_revenue
FROM (
    SELECT
        event_date,
        count(*) AS purchase_count,
        round(sum(coalesce(price, 0.0)), 2) AS purchase_revenue
    FROM iceberg.demo.silver_events
    WHERE event_type = 'purchase'
    GROUP BY 1
) s
JOIN iceberg.demo.daily_revenue g ON s.event_date = g.event_date
ORDER BY 1;

-- top_products sanity: no duplicate ranks within a day and max 10 products per day
SELECT event_date, count(*) AS ranked_products, max(product_rank) AS max_rank
FROM iceberg.demo.top_products
GROUP BY 1
ORDER BY 1;

SELECT count(*) AS duplicate_product_ranks
FROM (
    SELECT event_date, product_rank
    FROM iceberg.demo.top_products
    GROUP BY 1, 2
    HAVING count(*) > 1
);

-- conversion funnel should reconcile with Silver event counts
SELECT
    f.event_date,
    f.views,
    f.carts,
    f.purchases,
    round(CASE WHEN f.views = 0 THEN 0 ELSE CAST(f.carts AS double) / f.views END, 4) AS recomputed_view_to_cart,
    f.view_to_cart_rate,
    round(CASE WHEN f.carts = 0 THEN 0 ELSE CAST(f.purchases AS double) / f.carts END, 4) AS recomputed_cart_to_purchase,
    f.cart_to_purchase_rate
FROM iceberg.demo.conversion_funnel_daily f
ORDER BY 1;

-- category performance should reconcile with Silver purchase revenue by day/category
SELECT
    s.event_date,
    s.category_id,
    s.category_code,
    s.purchase_revenue AS silver_purchase_revenue,
    g.purchase_revenue AS gold_purchase_revenue
FROM (
    SELECT
        event_date,
        category_id,
        category_code,
        round(sum(CASE WHEN event_type = 'purchase' THEN coalesce(price, 0.0) ELSE 0.0 END), 2) AS purchase_revenue
    FROM iceberg.demo.silver_events
    GROUP BY 1, 2, 3
) s
JOIN iceberg.demo.category_performance_daily g
  ON s.event_date = g.event_date
 AND s.category_id = g.category_id
 AND s.category_code IS NOT DISTINCT FROM g.category_code
ORDER BY 1, 2;

-- session funnel sanity
SELECT
    count(*) AS sessions,
    count_if(converted) AS converted_sessions,
    count_if(session_duration_seconds < 0) AS negative_duration_sessions
FROM iceberg.demo.session_funnel;

SELECT event_date, path_label, count(*) AS sessions
FROM iceberg.demo.session_funnel
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- user conversion path sanity
SELECT
    event_date,
    count(*) AS users,
    count_if(converted) AS converted_users,
    sum(session_count) AS total_sessions
FROM iceberg.demo.user_conversion_path
GROUP BY 1
ORDER BY 1;

SELECT event_date, path_label, count(*) AS users
FROM iceberg.demo.user_conversion_path
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- cohort retention sanity
SELECT
    count_if(period_offset < 0) AS negative_period_offsets,
    count_if(active_users < 0 OR cohort_users < 0) AS negative_cohort_metrics,
    count_if(retention_rate < 0 OR retention_rate > 1) AS invalid_retention_rates
FROM iceberg.demo.cohort_retention;

-- repeat purchase sanity
SELECT
    count_if(repeat_purchasers > purchasers) AS repeat_gt_purchasers,
    count_if(repeat_purchase_rate < 0 OR repeat_purchase_rate > 1) AS invalid_repeat_rates
FROM iceberg.demo.repeat_purchase;

-- product affinity sanity
SELECT
    count_if(product_a >= product_b) AS unordered_pairs,
    count_if(co_purchase_sessions < 0) AS negative_pair_sessions
FROM iceberg.demo.product_affinity;

-- time-to-conversion sanity
SELECT
    event_date,
    sum(conversions) AS conversions
FROM iceberg.demo.time_to_conversion_distribution
GROUP BY 1
ORDER BY 1;

-- RFM sanity
SELECT
    count_if(recency_days < 0 OR frequency_90d < 0 OR monetary_90d < 0) AS negative_rfm_metrics,
    count_if(r_score NOT BETWEEN 1 AND 5 OR f_score NOT BETWEEN 1 AND 5 OR m_score NOT BETWEEN 1 AND 5) AS invalid_rfm_scores
FROM iceberg.demo.rfm_segmentation;
