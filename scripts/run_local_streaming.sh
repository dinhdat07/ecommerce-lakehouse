#!/usr/bin/env bash
set -euo pipefail

python3 apps/producer/replay_csv_to_kafka.py --input-csv data/sample/events_sample_100k.csv --topic ecom.events
python3 apps/streaming/kafka_to_bronze.py --topic ecom.events --bronze-path ./data/lakehouse/bronze/events
python3 apps/streaming/bronze_to_silver_stream.py --bronze-path ./data/lakehouse/bronze/events --silver-path ./data/lakehouse/silver/events
python3 apps/streaming/silver_to_gold_stream.py --silver-path ./data/lakehouse/silver/events --gold-path ./data/lakehouse/gold/analytics
