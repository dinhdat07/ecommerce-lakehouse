# Local Setup Guide

## Prerequisites

- Ubuntu or WSL with `python3`, `python3-venv`, and `python3-pip`
- Optional for future platform mode: Java 17, Docker, Kafka, Spark

On Ubuntu, install the Python prerequisites with:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip
```

## Quick Setup

1. Copy `.env.example` to `.env`.
2. Create the local virtual environment and install test tooling:
   ```bash
   bash scripts/bootstrap_local.sh
   ```
3. Activate the environment:
   ```bash
   source .venv/bin/activate
   ```
4. Verify the zero-dependency local round trip:
   ```bash
   python3 scripts/verify_local.py
   ```
5. Run the batch sample pipeline:
   ```bash
   bash scripts/clean_local_state.sh
   bash scripts/run_local_batch.sh
   ```
6. Run the streaming-friendly local pipeline:
   ```bash
   bash scripts/clean_local_state.sh
   bash scripts/run_local_streaming.sh
   ```

## Sample Data

- Checked-in sample: `data/sample/events_sample_100k.csv`
- Default Docker demo input: generated or reused sample CSV at about `100k` rows
- Synthetic sample generator:
  ```bash
  python3 scripts/create_sample.py --rows 1000 --output data/sample/events_sample.csv
  ```

## Automated Tests

- Pytest suite:
  ```bash
  python3 -m pytest -q
  ```
- Make targets:
  ```bash
  make verify-local
  make test
  make run-batch
  make run-streaming
  ```
