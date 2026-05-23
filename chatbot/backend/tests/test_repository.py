from __future__ import annotations

from pathlib import Path

from app.storage.repository import ChatRepository, build_user_message


def test_repository_round_trip(tmp_path: Path) -> None:
    repo = ChatRepository(tmp_path / "chatbot.sqlite3")
    session = repo.create_session("browser-a", "Test")
    repo.add_message(session.id, build_user_message("Show revenue trend"))
    loaded = repo.get_session("browser-a", session.id)
    assert loaded is not None
    assert len(loaded.messages) == 1
