# Pipeline Design

## Bronze

- Inputs: raw monthly CSV or CSV.GZ files for Phase 1, plus bounded Kafka replay messages for Phase 2.
- Output contract: physical Iceberg table `bronze_events` with raw event columns plus `batch_run_id`, `source_file`, `source_month`, `source_type`, `record_hash`, and `ingested_at`.
- Purpose: rerun-safe historical landing zone and stable backfill boundary.
- Streaming note: Kafka replay rows use `source_type='kafka_replay'` and preserve replay metadata through `source_file` and `source_month`.

## Silver

- Input: Bronze Iceberg rows.
- Processing rules:
  - parse both ISO sample timestamps and historical `YYYY-MM-DD HH:MM:SS UTC` timestamps
  - normalize `event_type`, `brand`, and empty-string nullable fields
  - cast numeric identifiers and prices
  - deduplicate on a deterministic event identity hash
- Outputs:
  - physical Iceberg table `silver_events`
  - incremental inserts only; no full-table overwrite
- Refresh model:
  - Phase 1 batch loads process historical files directly.
  - Phase 2 streaming micro-batches reuse the same normalization and deduplication logic through Spark `foreachBatch`.

## Gold

- Input: Silver canonical Iceberg events.
- Outputs:
  - `daily_revenue`
  - `top_products`
  - `conversion_funnel_daily`
  - `category_performance_daily`
  - `session_funnel`
  - `user_conversion_path`
  - `cohort_retention`
  - `repeat_purchase`
  - `product_affinity`
  - `time_to_conversion_distribution`
  - `rfm_segmentation`

## Operational Layer

- Gold refresh is incremental by `event_date`: only partitions touched by newly inserted Silver rows are recomputed.
- Advanced Gold tables that depend on broader history or full-session context are refreshed from the full Silver state:
  - `cohort_retention`
  - `repeat_purchase`
  - `product_affinity`
  - `time_to_conversion_distribution`
  - `rfm_segmentation`
- Trino is query-only in Phase 1; transformation logic runs only in Spark.
- Trino remains query-only in Phase 2; no Trino views or CTAS transformations are used.
- Phase 2 replay is intentionally bounded by sample size, replay sleep interval, `maxOffsetsPerTrigger`, and streaming timeout.
- Session analytics use the dataset's `user_session` as the primary session key and derive fallback sessions with a 30-minute inactivity rule only when `user_session` is missing.
- `user_conversion_path` is one row per `(event_date, user_id)`, built from the first observed `view`, `cart`, and `purchase` timestamps for that user-day.
