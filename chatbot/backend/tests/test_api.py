from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.core.settings import get_settings
from app.main import create_app


def test_session_flow(tmp_path: Path) -> None:
    app = create_app()
    app.dependency_overrides[get_settings] = lambda: get_settings().model_copy(
        update={
            "data_dir": tmp_path,
            "session_secret": "test-secret",
            "allow_mock_llm": True,
            "trusted_hosts": ["testserver", "localhost", "127.0.0.1"],
            "cors_allowed_origins": ["http://testserver"],
        }
    )
    client = TestClient(app)

    created = client.post("/api/chat/sessions")
    assert created.status_code == 200
    session_id = created.json()["session"]["id"]

    listed = client.get("/api/chat/sessions")
    assert listed.status_code == 200
    assert listed.json()[0]["id"] == session_id

    loaded = client.get(f"/api/chat/sessions/{session_id}")
    assert loaded.status_code == 200
    assert loaded.json()["id"] == session_id
