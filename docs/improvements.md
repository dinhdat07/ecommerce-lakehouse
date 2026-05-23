# Improvement Opportunities

## Visible Layer

- The repository now exposes working batch and streaming-friendly CLIs, Gold outputs, and serving SQL, but dashboards are still represented as contracts rather than exported Superset objects.
- Benchmark outputs are JSON and CSV files today. A production follow-up could push benchmark history into a queryable audit table or time-series store.

## Hidden Layer

- Add a dedicated invalid-record table separate from Silver quarantine so data-quality metrics can be queried historically.
- Introduce compaction and retention policies for Iceberg data and metadata files.
- Persist streaming watermarks and replay offsets in a queryable audit layer so reruns can be incremental and easier to debug.
- Add reconciliation checks between Bronze row counts, Silver valid/invalid counts, and Gold aggregate totals to catch silent data drift.
- Promote run manifests into a queryable audit table for lineage, SLA tracking, and incident response.

## Scalability and Maintainability

- Move the current filesystem-backed contracts behind repository interfaces so Spark and Iceberg writers can replace the local adapters without changing business logic.
- Add schema-version metadata to Silver and Gold outputs before introducing backward-incompatible changes.
- Expand tests to compare batch and streaming outputs on the same input slice and enforce semantic parity.
