# Chatbot Workspace

This workspace is the starting point for a dedicated English-language analytics chatbot
for the ecommerce lakehouse.

Recommended shape:

- `backend/`: FastAPI service for chat orchestration, text-to-SQL, guardrails, and Trino access
- `frontend/`: React + Vite single-page application for the chat experience
- `shared/`: optional shared prompt, schema, and contract files

Demo ports:

- frontend: `8089`
- backend API: `8090`

Why this lives outside Superset:

- it keeps LLM orchestration, prompt logic, and chat state out of the BI process
- it can scale and deploy independently from Trino and Superset
- it is easier to add API-key, auth, logging, and evaluation controls

Planned capabilities:

1. Natural-language analytics chat in English
2. Safe SQL generation against curated Gold tables
3. Query execution through a backend-controlled Trino client
4. Result summarization and chart-friendly response payloads
5. Optional MCP wrapper later for internal agent workflows
