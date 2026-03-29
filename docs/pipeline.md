# Pipeline Design

## Bronze

- Inputs: raw monthly CSV or CSV.GZ files, plus replayed streaming events.
- Output contract: append-only JSONL with `payload`, `record_hash`, `source_file`, `source_row_number`, `source_type`, `run_id`, and `ingested_at`.
- Purpose: replay, auditability, and a stable ingestion boundary.

## Silver

- Input: Bronze JSONL records.
- Processing rules:
  - parse both ISO sample timestamps and historical `YYYY-MM-DD HH:MM:SS UTC` timestamps
  - normalize `event_type`, `brand`, and empty-string nullable fields
  - cast numeric identifiers and prices
  - validate required canonical columns
  - deduplicate on a deterministic event identity hash
- Outputs:
  - canonical event partitions under `event_date=YYYY-MM-DD/`
  - `_quarantine/` for invalid rows

## Gold

- Input: Silver canonical events.
- Outputs:
  - `user_activity_summary`
  - `product_popularity`
  - `conversion_funnel`
  - `revenue_by_category`
  - `session_summary`

## Operational Layer

- Every stage writes a JSON manifest with row counts, inputs, outputs, and run metadata.
- Batch and streaming-friendly entrypoints share the same business logic so the contracts stay aligned.
- Local runs materialize logical `s3a://...` stage URIs under `data/lakehouse/`, keeping the codebase cloud-portable.
