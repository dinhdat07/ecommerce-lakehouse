# References and Source-Quality Review

Last reviewed: 2026-05-21.

This project uses a Spark -> Iceberg -> Trino -> Superset lakehouse with Kafka streaming, distributed MinIO object storage, and a separate FastAPI/React analytics chatbot. The references below prioritize primary and high-quality sources: peer-reviewed/academic material, official Apache/project documentation, vendor documentation for vendor-owned concepts, and the original dataset publisher pages.

## Source-quality policy

| Rule | Applied decision |
|---|---|
| Prefer primary sources | Use official Apache Spark, Iceberg, Kafka, Trino, Superset, MinIO, Google Cloud, FastAPI, Vite, and Tailwind documentation. |
| Use academic sources for architecture | Use the CIDR lakehouse paper to justify the lakehouse concept. |
| Cite dataset provenance directly | Cite both Kaggle and REES46 because the dataset page and publisher mirror are the strongest provenance sources. |
| Separate internal methodology from external standards | Gold metrics such as repeat purchase, RFM, cohort retention, product affinity, and time-to-conversion are project-defined contracts documented in this repo, not copied from a single external standard. |
| Avoid weak citations | Do not rely on random blogs, Medium posts, StackOverflow answers, or AI-generated explanations in the final report unless clearly marked as non-authoritative supplementary reading. |

## Core architecture and design pattern

| Topic | Reference | How it is used in this project | Quality note |
|---|---|---|---|
| Data lakehouse concept | Armbrust, Ghodsi, Xin, and Zaharia, *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*, CIDR 2021. https://vldb.org/cidrdb/2021/lakehouse-a-new-generation-of-open-platforms-that-unify-data-warehousing-and-advanced-analytics.html | Justifies combining low-cost object storage, open table formats, Spark processing, SQL serving, BI, and analytics workloads in one architecture. | Academic conference paper; strongest conceptual source for the lakehouse architecture. |
| Bronze / Silver / Gold medallion layering | Databricks, *What is the medallion lakehouse architecture?* https://docs.databricks.com/gcp/en/lakehouse/medallion | Defines the raw, validated, and business-ready layers used by `bronze_events`, `silver_events`, and Gold marts. | Vendor documentation, but it is the canonical source for the medallion terminology and aligns directly with the implemented pattern. |

## Source dataset

| Topic | Reference | How it is used in this project | Quality note |
|---|---|---|---|
| E-commerce behavior dataset | Kaggle, *eCommerce behavior data from multi category store*. https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data | Source of monthly event files such as `2019-Oct.csv.gz`, `2019-Nov.csv.gz`, `2019-Dec.csv.gz`, `2020-Jan.csv.gz`, `2020-Feb.csv.gz`, `2020-Mar.csv.gz`, and `2020-Apr.csv.gz`. | Original public dataset page; cite in the report/slides for dataset source and fields. |
| Dataset publisher mirror | REES46 datasets page. https://data.rees46.com/ | Confirms monthly archive names, public provenance, and the 7-month marketplace behavior dataset source. | Publisher-side source; useful to defend where the data came from if Kaggle is not accessible during grading. |

## Processing, storage, and serving stack

| Component | Reference | How it is used in this project | Quality note |
|---|---|---|---|
| Apache Spark Structured Streaming | Spark Structured Streaming Programming Guide. https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html | Supports the Kafka streaming path, microbatch processing, checkpointing model, and `foreachBatch` pattern used to reuse Bronze/Silver/Gold logic per microbatch. | Official Apache Spark documentation; versioned to Spark 3.5.x. |
| Apache Iceberg Spark writes | Iceberg Spark Writes documentation. https://iceberg.apache.org/docs/1.9.1/spark-writes/ | Supports Spark writing physical Iceberg Bronze, Silver, and Gold tables. | Official Apache Iceberg documentation; versioned source. |
| Apache Iceberg JDBC catalog | Iceberg JDBC Catalog documentation. https://iceberg.apache.org/docs/latest/jdbc/ | Supports the Postgres-backed JDBC catalog metadata used by Iceberg tables. | Official Apache Iceberg documentation. |
| Trino Iceberg connector | Trino Iceberg connector documentation. https://trino.io/docs/current/connector/iceberg.html | Supports querying Iceberg tables from Trino for BI dashboards and chatbot SQL execution. | Official Trino documentation. |
| Trino JDBC catalog/metastore configuration | Trino object-storage metastore documentation. https://trino.io/docs/current/object-storage/metastores.html#jdbc-catalog | Supports Trino configuration for the Iceberg JDBC catalog. | Official Trino documentation. |
| Apache Kafka KRaft | Kafka KRaft operations documentation. https://kafka.apache.org/42/operations/kraft/ | Supports the three-node Kafka broker/controller setup without ZooKeeper. | Official Apache Kafka documentation. |
| Kafka topic durability | Kafka topic configs, especially `min.insync.replicas`. https://kafka.apache.org/42/configuration/topic-configs/ | Supports topic replication factor 3 and min ISR 2 choices for the demo cluster. | Official Apache Kafka documentation. |
| MinIO distributed object storage | MinIO multi-node multi-drive deployment. https://min.io/docs/minio/linux/operations/install-deploy-manage/deploy-minio-multi-node-multi-drive.html | Supports distributed object storage for the Iceberg warehouse. | Official MinIO documentation. Current MinIO docs may redirect to MinIO AIStor pages, but the source is still MinIO-owned documentation. |
| MinIO erasure coding | MinIO erasure coding concepts. https://min.io/docs/minio/linux/operations/concepts/erasure-coding.html | Supports explaining erasure coding, parity, read/write quorum, and the distributed storage proof in the demo. | Official MinIO documentation. |
| Apache Superset | Apache Superset documentation. https://superset.apache.org/ | Supports the BI dashboard/UI layer over Gold tables. | Official Apache Superset documentation. |
| Superset Datasets API | Superset Datasets API. https://superset.apache.org/docs/api/datasets/ | Supports automated Superset dataset/bootstrap logic. | Official Apache Superset API documentation. |

## Chatbot and application layer

| Component | Reference | How it is used in this project | Quality note |
|---|---|---|---|
| Vertex AI Gemini 2.5 Flash | Google Cloud, *Gemini 2.5 Flash*. https://docs.cloud.google.com/vertex-ai/generative-ai/docs/models/gemini/2-5-flash | Supports chatbot planning, SQL generation, and answer summarization. | Official Google Cloud documentation; model availability can change, so verify again before final submission. |
| Google Gen AI Python SDK | Google Gen AI SDK documentation. https://googleapis.github.io/python-genai/ | Supports the Python client library used by the chatbot backend to call Gemini/Vertex AI. | Official generated SDK documentation. |
| FastAPI StreamingResponse | FastAPI custom responses / StreamingResponse documentation. https://fastapi.tiangolo.com/advanced/custom-response/#streamingresponse | Supports streaming chatbot responses from backend to frontend using `StreamingResponse` with `text/event-stream`. | Official FastAPI documentation. |
| SQLGlot SQL parser | SQLGlot API documentation. https://sqlglot.com/sqlglot.html | Supports SQL parsing/validation guardrails before executing generated SQL in Trino. | Project documentation for a widely used SQL parser; appropriate because the repo depends directly on `sqlglot`. |
| Trino Python client | Trino Python client repository. https://github.com/trinodb/trino-python-client | Supports backend connectivity from FastAPI to Trino. | Official TrinoDB-maintained client repository. |
| Vite React tooling | Vite official plugins documentation. https://vite.dev/plugins/ | Supports React + Vite frontend build/dev tooling. | Official Vite documentation. |
| Tailwind CSS with Vite | Tailwind CSS Vite installation documentation. https://tailwindcss.com/docs/installation/using-vite | Supports frontend styling setup. | Official Tailwind CSS documentation. |

## BI and analytics methodology

| Topic | Reference | How it is used in this project | Quality note |
|---|---|---|---|
| Sessionization | Dataset `user_session` field from Kaggle/REES46 dataset semantics. https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data | Uses provided `user_session` as the primary session key and falls back to derived sessions only when missing. | Original dataset semantics; explain this explicitly in the report. |
| Gold table contracts | `docs/pipeline.md` and `infra/trino/sql/validate_silver_gold.sql` | Defines project-specific analytics outputs: revenue, category performance, conversion funnel, session funnel, retention, repeat purchase, product affinity, time-to-conversion, and RFM. | Internal methodology; high quality if presented as project-defined metrics with validation SQL rather than as an external standard. |
| Demo/operational proof | `docs/demo_guide.md`, `docs/operations.md`, and server validation scripts | Shows how to prove distributed components, row counts, BI dashboards, streaming, and chatbot behavior during the demo. | Internal operational evidence; useful as an appendix, not as an external academic citation. |

## Recommended bibliography for report/slides

1. Armbrust, M., Ghodsi, A., Xin, R., and Zaharia, M. (2021). *Lakehouse: A New Generation of Open Platforms that Unify Data Warehousing and Advanced Analytics*. CIDR.
2. Databricks. *What is the medallion lakehouse architecture?*
3. Kaggle / REES46. *eCommerce behavior data from multi category store*.
4. Apache Spark. *Structured Streaming Programming Guide*.
5. Apache Iceberg. *Spark Writes* and *JDBC Catalog* documentation.
6. Trino. *Iceberg connector* and *JDBC catalog metastore* documentation.
7. Apache Kafka. *KRaft* and *Topic Configs* documentation.
8. MinIO. *Multi-node Multi-drive Deployment* and *Erasure Coding* documentation.
9. Apache Superset. *Superset documentation* and *Datasets API*.
10. Google Cloud. *Gemini 2.5 Flash on Vertex AI* and Google Gen AI Python SDK documentation.
11. FastAPI. *StreamingResponse* documentation.
12. SQLGlot and Trino Python client documentation for chatbot SQL validation and query execution.
13. Vite and Tailwind CSS official documentation for the chatbot frontend.

## Notes for grading discussion

- The strongest academic citation is the CIDR lakehouse paper; use it to justify the overall architecture.
- The strongest pattern citation is Databricks medallion architecture; use it to explain Bronze/Silver/Gold.
- The strongest dataset citations are Kaggle and REES46; use both because REES46 confirms file names and provenance.
- The strongest implementation citations are official project docs from Apache Spark, Apache Iceberg, Trino, Kafka, MinIO, Superset, Google Cloud, FastAPI, Vite, and Tailwind.
- For metrics, state clearly that definitions are implemented and validated by this project. Do not pretend all metrics come from one external standard.
