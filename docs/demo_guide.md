# Demo Showcase Runbook

This runbook is designed for a high-impact internal demo of the distributed lakehouse, BI dashboard, and analytics chatbot.

## 1. Start The Full Demo Stack

Run from node1:

```bash
cd /root/ecommerce-lakehouse
bash infra/server/scripts/start-full-demo-stack.sh
```

This does four things:

1. starts the 3-node Kafka cluster
2. validates Kafka quorum and topic replication
3. starts Trino and Superset on node1
4. starts the chatbot frontend and backend on node1

## 2. Show Cluster Topology And Data Scale

Run:

```bash
bash infra/server/scripts/demo-complete-showcase.sh all
```

This prints:

- Spark workers across the 3 nodes
- MinIO distributed-cluster health
- Kafka quorum status and replicated topic details
- row counts from Iceberg Silver and Gold tables
- raw/object-storage evidence
- Superset and chatbot health

## 3. Open The UI Windows

Open these URLs:

- Spark UI: `http://100.123.190.84:8080`
- Trino UI: `http://100.123.190.84:8085/ui/`
- Superset: `http://100.123.190.84:8088`
- Chatbot: `http://100.123.190.84:8089`

Recommended Superset login:

- username: `admin`
- password: `admin`

## 4. Demo Narrative

### Part A - Architecture And Distribution

Use the output of `demo-showcase.sh status` and explain:

- 3-node Spark cluster
- 3-node MinIO erasure-coded object storage
- 3-node Kafka quorum for ingestion
- Trino serving Iceberg tables
- Superset using Gold marts
- Chatbot using Trino + semantic layer + Vertex AI

### Part B - Data Scale

Highlight:

- `silver_events` row count
- top category revenue query output
- Gold marts such as `daily_revenue`, `category_performance_daily`, and `rfm_segmentation`

### Part C - Dashboard

In Superset, open the analytics dashboard and explain that it reads physical Gold Iceberg tables through Trino.

### Part D - Live Distributed Proof

Run:

```bash
bash infra/server/scripts/demo-realtime-proof.sh run
```

During this step:

- keep Spark UI open
- keep Trino UI open
- keep Superset open

What to say:

- a bounded replay sample is pushed into Kafka
- Spark Structured Streaming consumes it
- Bronze and Silver row counts increase
- downstream BI stays on the same lakehouse

## 5. Chatbot Demo Questions

Use these prompts in the chatbot UI:

1. `Show the daily revenue trend and purchase volume.`
2. `Which categories generate the most revenue?`
3. `How is the conversion funnel trending over time?`
4. `Summarize cohort retention performance.`
5. `What does the RFM segment mix look like?`

What to emphasize:

- SQL transparency
- Trino-backed answers
- chart + table rendering
- English business summary
- internal copilot UX separate from Superset

## 6. Useful Validation Commands

Kafka:

```bash
bash infra/server/scripts/validate-kafka-cluster-3node.sh
```

Realtime proof status:

```bash
bash infra/server/scripts/demo-realtime-proof.sh status
```

Chatbot readiness:

```bash
curl http://100.123.190.84:8090/api/ready
```

Trino sample count:

```bash
docker exec server-trino trino --server http://127.0.0.1:8085 --execute "SELECT count(*) FROM iceberg.demo.silver_events"
```

## 7. Recommended Demo Order

1. `start-full-demo-stack.sh`
2. `demo-complete-showcase.sh all`
3. open Spark UI / Trino UI / Superset / Chatbot
4. show dashboard first
5. run `demo-realtime-proof.sh run`
6. finish with chatbot Q&A

## 8. One-Command Sections

Use these if you want to reveal the demo step by step:

```bash
bash infra/server/scripts/demo-complete-showcase.sh urls
bash infra/server/scripts/demo-complete-showcase.sh health
bash infra/server/scripts/demo-complete-showcase.sh distributed
bash infra/server/scripts/demo-complete-showcase.sh storage
bash infra/server/scripts/demo-complete-showcase.sh data
bash infra/server/scripts/demo-complete-showcase.sh realtime
bash infra/server/scripts/demo-complete-showcase.sh chatbot
```
