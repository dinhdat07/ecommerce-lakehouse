# Chatbot Backend

Run locally:

```bash
cd chatbot/backend
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
PYTHONPATH=. uvicorn app.main:app --reload --host 0.0.0.0 --port 8090
```

Main features:

- FastAPI API with session persistence in SQLite
- strict SQL validation before Trino execution
- Vertex AI orchestration with a deterministic mock fallback
- SSE streaming response endpoint for the frontend

Important environment variables:

- `CHATBOT_SESSION_SECRET`
- `CHATBOT_TRINO_HOST`
- `CHATBOT_TRINO_PORT`
- `CHATBOT_VERTEX_API_KEY`
- `CHATBOT_ALLOW_MOCK_LLM`
