-- Bronze rows ingested by the streaming replay path
SELECT source_type, source_month, count(*) AS rows
FROM iceberg.demo.bronze_events
WHERE source_type = 'kafka_replay'
GROUP BY 1, 2
ORDER BY 2, 1;

-- Silver rows derived from the streaming replay months
SELECT event_date, count(*) AS silver_rows
FROM iceberg.demo.silver_events
WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30'
GROUP BY 1
ORDER BY 1;

-- Silver quality checks scoped to the streaming replay window
SELECT count(*) AS duplicate_dedupe_keys
FROM (
    SELECT dedupe_key
    FROM iceberg.demo.silver_events
    WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30'
    GROUP BY 1
    HAVING count(*) > 1
);

SELECT
    count_if(event_time IS NULL) AS null_event_time_rows,
    count_if(event_date IS NULL) AS null_event_date_rows,
    count_if(event_type NOT IN ('view', 'cart', 'purchase')) AS unexpected_event_type_rows
FROM iceberg.demo.silver_events
WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30';

-- Gold table row counts in the streaming window
SELECT
    (SELECT count(*) FROM iceberg.demo.daily_revenue WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30') AS daily_revenue_rows,
    (SELECT count(*) FROM iceberg.demo.top_products WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30') AS top_products_rows,
    (SELECT count(*) FROM iceberg.demo.conversion_funnel_daily WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30') AS conversion_funnel_rows,
    (SELECT count(*) FROM iceberg.demo.category_performance_daily WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30') AS category_performance_rows,
    (SELECT count(*) FROM iceberg.demo.session_funnel WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30') AS session_funnel_rows,
    (SELECT count(*) FROM iceberg.demo.user_conversion_path WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30') AS user_conversion_path_rows,
    (SELECT count(*) FROM iceberg.demo.time_to_conversion_distribution WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30') AS time_to_conversion_distribution_rows,
    (SELECT count(*) FROM iceberg.demo.cohort_retention) AS cohort_retention_rows,
    (SELECT count(*) FROM iceberg.demo.repeat_purchase) AS repeat_purchase_rows,
    (SELECT count(*) FROM iceberg.demo.product_affinity) AS product_affinity_rows,
    (SELECT count(*) FROM iceberg.demo.rfm_segmentation) AS rfm_segmentation_rows;

-- Gold revenue must reconcile with Silver purchases
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
    WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30'
      AND event_type = 'purchase'
    GROUP BY 1
) s
JOIN iceberg.demo.daily_revenue g ON s.event_date = g.event_date
ORDER BY 1;

-- Funnel rates sanity
SELECT
    event_date,
    views,
    carts,
    purchases,
    view_to_cart_rate,
    cart_to_purchase_rate,
    view_to_purchase_rate
FROM iceberg.demo.conversion_funnel_daily
WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30'
ORDER BY 1;

-- Session and user-path sanity
SELECT
    count_if(session_duration_seconds < 0) AS negative_session_durations,
    count_if(converted) AS converted_sessions
FROM iceberg.demo.session_funnel
WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30';

SELECT event_date, path_label, count(*) AS users
FROM iceberg.demo.user_conversion_path
WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30'
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- Advanced Gold table sanity in the streaming window
SELECT
    event_date,
    sum(conversions) AS conversions
FROM iceberg.demo.time_to_conversion_distribution
WHERE event_date BETWEEN DATE '2020-03-01' AND DATE '2020-04-30'
GROUP BY 1
ORDER BY 1;

SELECT
    count_if(period_offset < 0) AS negative_period_offsets
FROM iceberg.demo.cohort_retention;

SELECT
    count_if(repeat_purchasers > purchasers) AS repeat_gt_purchasers
FROM iceberg.demo.repeat_purchase;

SELECT
    count_if(product_a >= product_b) AS unordered_pairs
FROM iceberg.demo.product_affinity;

SELECT
    count_if(r_score NOT BETWEEN 1 AND 5 OR f_score NOT BETWEEN 1 AND 5 OR m_score NOT BETWEEN 1 AND 5) AS invalid_rfm_scores
FROM iceberg.demo.rfm_segmentation;
