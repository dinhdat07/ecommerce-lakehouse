# Improvement Opportunities

## Visible Layer

- The repository now exposes working batch and streaming-friendly CLIs, Gold outputs, and serving SQL, but dashboards are still represented as contracts rather than exported Superset objects.
- Gold outputs are local CSV files for portability. A production follow-up should materialize these as Iceberg tables and expose them through Trino directly.

## Hidden Layer

- Add a dedicated invalid-record table separate from Silver quarantine so data-quality metrics can be queried historically.
- Introduce compaction and retention policies. JSONL and CSV are practical for local validation, but object-store deployments need small-file control and lifecycle rules.
- Persist watermarks and replay offsets for the local streaming bus so reruns can be incremental instead of full micro-batch recomputations.
- Add reconciliation checks between Bronze row counts, Silver valid/invalid counts, and Gold aggregate totals to catch silent data drift.
- Promote run manifests into a queryable audit table for lineage, SLA tracking, and incident response.

## Scalability and Maintainability

- Move the current filesystem-backed contracts behind repository interfaces so Spark and Iceberg writers can replace the local adapters without changing business logic.
- Add schema-version metadata to Silver and Gold outputs before introducing backward-incompatible changes.
- Expand tests to compare batch and streaming outputs on the same input slice and enforce semantic parity.
