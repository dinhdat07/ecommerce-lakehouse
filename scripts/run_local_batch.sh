#!/usr/bin/env bash
set -euo pipefail

python apps/batch/backfill_bronze.py
python apps/batch/bronze_to_silver.py
python apps/batch/silver_to_gold.py
