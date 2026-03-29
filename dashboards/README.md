# Dashboard Blueprint

These dashboard definitions are the visible serving layer mapped onto the current Gold contracts and Trino views.

## User Behavior Dashboard

- Dataset: `analytics.v_user_activity_summary`
- Charts:
  - daily event volume
  - daily views, carts, and purchases
  - active users and distinct products by day

## Conversion Funnel Dashboard

- Dataset: `analytics.v_conversion_funnel`
- Charts:
  - daily funnel counts
  - view-to-cart, cart-to-purchase, and view-to-purchase rates

## Product Performance Dashboard

- Dataset: `analytics.v_product_popularity`
- Charts:
  - top viewed products
  - top purchased products
  - product conversion rate leaderboard

## Revenue Dashboard

- Dataset: `analytics.v_revenue_by_category`
- Charts:
  - revenue by category over time
  - purchase count by category
  - average order value by category
