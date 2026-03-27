# Pipeline Design (Skeleton)

## Bronze

- Input: CSV replay to Kafka and/or direct raw historical files
- Output: append-only raw events with minimal mutation
- Purpose: traceability and replay

## Silver

- Input: Bronze events
- Processing:
  - parse timestamps
  - cast numeric/string fields
  - enforce required columns
  - basic deduplication strategy (TBD)
- Output: canonical typed events, partitioned by event date

## Gold

- Input: Silver canonical events
- Processing:
  - aggregate behavior metrics (e.g., views, carts, purchases)
  - compute user/product/category summaries
- Output: BI-friendly tables for Trino/Superset

## Workloads

- Batch jobs:
  - `backfill_bronze.py`
  - `bronze_to_silver.py`
  - `silver_to_gold.py`
- Streaming jobs:
  - `kafka_to_bronze.py`
  - `bronze_to_silver_stream.py`
  - `silver_to_gold_stream.py`
