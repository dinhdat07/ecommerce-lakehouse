# Chatbot Architecture Proposal

## Recommendation

Build a dedicated chatbot product as a separate frontend and backend, then add an MCP facade later only if internal agents need the same capabilities.

Do not start by embedding the chatbot inside Superset.

## Why

- The current BI stack already shares a small machine with Trino, Superset, Spark, Postgres, and MinIO.
- LLM orchestration adds session state, retries, prompt logic, query validation, and audit needs that do not belong inside Superset.
- A separate service boundary makes it easier to control latency, quotas, and failures.

## Recommended Technology

### Backend

- FastAPI
- Python 3.11+
- `google-genai` for Vertex AI API usage
- `trino` Python client
- `sqlglot` or equivalent for SQL parsing and allow-list validation

Why FastAPI:

- It matches the repo's Python-first operational model.
- It is lightweight enough for the current hardware.
- It supports streaming responses well.
- It is straightforward to run behind the existing infrastructure later.

### Frontend

- React
- Vite
- TypeScript
- Tailwind CSS

Why not Next.js first:

- The current host does not have much spare CPU or RAM.
- A static SPA is simpler to build and cheaper to run.
- This product does not need SEO or server-side rendering.

### LLM access

- Use Vertex AI through the Google Gen AI SDK.
- For quick implementation, an API key can work.
- For production, move to application default credentials or another server-side credential flow.

## Vertex AI Notes

According to Google's Vertex AI documentation:

- Google recommends API keys for testing and application default credentials for production.
- Vertex AI express mode supports API-key authentication.
- Express mode is a preview offering and has lower request-rate limits than the full Vertex AI setup.

Implication:

- If you want to move quickly, use a server-side Vertex AI API key first.
- If this chatbot is expected to serve real users beyond low traffic, plan an early migration to standard Vertex AI authentication.

## Product Behavior

The chatbot should not behave like a general-purpose assistant. It should behave like a constrained analytics copilot.

### User flow

1. User asks an analytics question in English.
2. Backend classifies the question:
   - answer from semantic metadata
   - generate SQL
   - ask a clarifying question
3. Backend sends the model a curated schema context, metric glossary, and rules.
4. Model returns:
   - intent
   - SQL
   - confidence
   - explanation
5. Backend validates the SQL:
   - `SELECT` only
   - only approved schemas and Gold tables
   - row limit enforced
   - no DDL, DML, or external functions
6. Backend runs the query on Trino.
7. Backend optionally asks the model for an English explanation of the results.
8. Frontend renders:
   - assistant answer
   - generated SQL
   - data table
   - chart suggestion when appropriate

## Data Scope

Start with curated Gold outputs only, not Bronze or raw Silver:

- `daily_revenue`
- `top_products`
- `conversion_funnel_daily`
- `category_performance_daily`
- `session_funnel`
- `user_conversion_path`
- `cohort_retention`
- `repeat_purchase`
- `product_affinity`
- `time_to_conversion_distribution`
- `rfm_segmentation`

Why:

- These are already business-facing tables.
- They reduce schema ambiguity.
- They make text-to-SQL more reliable.

## Prompting Strategy

Use a two-stage backend flow:

1. Intent and planning step
   - determine the business question
   - select candidate tables and metrics
   - decide whether clarification is needed
2. SQL generation step
   - generate Trino-compatible SQL only
   - include required limit and approved-table constraints

This is safer than one-shot prompting.

## Response Contract

The backend should return structured payloads like:

- `message`
- `sql`
- `columns`
- `rows`
- `row_count`
- `chart_suggestion`
- `confidence`
- `warnings`

This keeps the frontend simple and auditable.

## Safety Rules

- Backend stores the Vertex AI API key only on the server.
- Browser never talks directly to Vertex AI or Trino.
- SQL must be parsed and validated before execution.
- Limit result size and execution time.
- Log prompts, generated SQL, and query metadata for review.
- Add a deny-by-default allow-list for schemas, tables, and functions.

## Deployment Shape

Short term:

- frontend: static build served by Nginx or a small Node process
- backend: single FastAPI process

Later:

- frontend remains static
- backend can move behind Cloud Run, Docker Compose, or another dedicated service host
- MCP adapter can call the same backend endpoints

## Suggested Folder Layout

The layout below reflects the original design. In the current implementation, LLM
prompts are inlined in `chat_service.py` rather than stored in a separate
`prompts/` directory.

```text
chatbot/
  backend/
    app/
      api/
      core/
      llm/
      services/
      sql/
    tests/
    README.md
  frontend/
    src/
      components/
      features/chat/
      features/results/
      lib/
    README.md
  shared/
    semantic/
```

## Suggested MVP

Version 1 should support:

1. English-only chat
2. 5 to 10 curated business questions
3. Gold-table-only SQL generation
4. Visible generated SQL
5. CSV export of result tables
6. Basic chart rendering for time series and category breakdowns

Do not start with:

- multi-turn agent workflows
- self-healing autonomous SQL retries
- write-back actions
- dashboard authoring inside chat

## Decision

Use:

- FastAPI backend
- React + Vite frontend
- Vertex AI via server-side API key first
- curated semantic layer over Gold tables

Avoid:

- custom chatbot inside Superset as the primary implementation
- local model hosting on this machine
- exposing raw Trino access to the browser
