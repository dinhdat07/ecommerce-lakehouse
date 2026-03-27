# Problem Statement

The project builds an analytics pipeline for E-commerce user behavior events with two workloads:

- Historical backfill from monthly compressed CSV files (Oct 2019 to Feb 2020)
- Near real-time processing from Kafka for newer simulated events

Key goals:
- Preserve raw events in Bronze for replay/audit
- Standardize typed canonical events in Silver
- Publish business-ready aggregates in Gold for BI tools

Non-goals for current phase:
- Full production deployment
- Complete business metric definitions
- Advanced data quality framework
