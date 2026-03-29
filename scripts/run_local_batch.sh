#!/usr/bin/env bash
set -euo pipefail

python3 apps/batch/backfill_bronze.py --input-dir data/sample --bronze-path ./data/lakehouse/bronze/events
python3 apps/batch/bronze_to_silver.py --bronze-path ./data/lakehouse/bronze/events --silver-path ./data/lakehouse/silver/events
python3 apps/batch/silver_to_gold.py --silver-path ./data/lakehouse/silver/events --gold-path ./data/lakehouse/gold/analytics
