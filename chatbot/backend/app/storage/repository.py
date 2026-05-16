from __future__ import annotations

import sqlite3
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterator

from app.core.models import ChatMessage, SessionDetail, SessionSummary


def utc_now() -> datetime:
    return datetime.now(UTC)


class ChatRepository:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    @contextmanager
    def _connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    browser_id TEXT NOT NULL,
                    title TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    text TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    sql_text TEXT,
                    columns_json TEXT,
                    rows_json TEXT,
                    row_count INTEGER,
                    tables_json TEXT,
                    chart_json TEXT,
                    warnings_json TEXT,
                    confidence REAL,
                    elapsed_ms INTEGER,
                    FOREIGN KEY(session_id) REFERENCES sessions(id)
                );
                CREATE TABLE IF NOT EXISTS feedback (
                    id TEXT PRIMARY KEY,
                    message_id TEXT NOT NULL,
                    rating TEXT NOT NULL,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(message_id) REFERENCES messages(id)
                );
                """
            )

    def create_session(self, browser_id: str, title: str) -> SessionSummary:
        session_id = uuid.uuid4().hex
        now = utc_now()
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO sessions (id, browser_id, title, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
                (session_id, browser_id, title, now.isoformat(), now.isoformat()),
            )
        return SessionSummary(id=session_id, title=title, created_at=now, updated_at=now)

    def list_sessions(self, browser_id: str) -> list[SessionSummary]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, title, created_at, updated_at FROM sessions WHERE browser_id = ? ORDER BY updated_at DESC",
                (browser_id,),
            ).fetchall()
        return [
            SessionSummary(
                id=row["id"],
                title=row["title"],
                created_at=datetime.fromisoformat(row["created_at"]),
                updated_at=datetime.fromisoformat(row["updated_at"]),
            )
            for row in rows
        ]

    def get_session(self, browser_id: str, session_id: str) -> SessionDetail | None:
        with self._connect() as conn:
            session_row = conn.execute(
                "SELECT id, title, created_at, updated_at FROM sessions WHERE id = ? AND browser_id = ?",
                (session_id, browser_id),
            ).fetchone()
            if session_row is None:
                return None
            message_rows = conn.execute(
                """
                SELECT id, role, text, created_at, sql_text, columns_json, rows_json, row_count,
                       tables_json, chart_json, warnings_json, confidence, elapsed_ms
                FROM messages
                WHERE session_id = ?
                ORDER BY created_at ASC
                """,
                (session_id,),
            ).fetchall()
        return SessionDetail(
            id=session_row["id"],
            title=session_row["title"],
            created_at=datetime.fromisoformat(session_row["created_at"]),
            updated_at=datetime.fromisoformat(session_row["updated_at"]),
            messages=[self._row_to_message(row) for row in message_rows],
        )

    def add_message(self, session_id: str, message: ChatMessage) -> None:
        import json

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO messages (
                    id, session_id, role, text, created_at, sql_text, columns_json, rows_json,
                    row_count, tables_json, chart_json, warnings_json, confidence, elapsed_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message.id,
                    session_id,
                    message.role,
                    message.text,
                    message.created_at.isoformat(),
                    message.sql,
                    json.dumps(message.columns),
                    json.dumps(message.rows),
                    message.row_count,
                    json.dumps(message.tables_used),
                    json.dumps(message.chart_suggestion),
                    json.dumps(message.warnings),
                    message.confidence,
                    message.elapsed_ms,
                ),
            )
            conn.execute(
                """
                UPDATE sessions
                SET updated_at = ?,
                    title = CASE
                        WHEN title = 'New conversation' AND ? = 'user' THEN ?
                        ELSE title
                    END
                WHERE id = ?
                """,
                (utc_now().isoformat(), message.role, self._derive_title(message.text), session_id),
            )

    def add_feedback(self, message_id: str, rating: str, note: str | None) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO feedback (id, message_id, rating, note, created_at) VALUES (?, ?, ?, ?, ?)",
                (uuid.uuid4().hex, message_id, rating, note, utc_now().isoformat()),
            )

    @staticmethod
    def _derive_title(message: str) -> str:
        normalized = " ".join(message.split())
        return normalized[:72] if normalized else "New conversation"

    @staticmethod
    def _row_to_message(row: sqlite3.Row) -> ChatMessage:
        import json

        return ChatMessage(
            id=row["id"],
            role=row["role"],
            text=row["text"],
            created_at=datetime.fromisoformat(row["created_at"]),
            sql=row["sql_text"],
            columns=json.loads(row["columns_json"] or "[]"),
            rows=json.loads(row["rows_json"] or "[]"),
            row_count=row["row_count"],
            tables_used=json.loads(row["tables_json"] or "[]"),
            chart_suggestion=json.loads(row["chart_json"]) if row["chart_json"] else None,
            warnings=json.loads(row["warnings_json"] or "[]"),
            confidence=row["confidence"],
            elapsed_ms=row["elapsed_ms"],
        )


def build_assistant_message(
    text: str,
    *,
    sql: str | None = None,
    columns: list[str] | None = None,
    rows: list[dict[str, Any]] | None = None,
    row_count: int | None = None,
    tables_used: list[str] | None = None,
    chart_suggestion: dict[str, Any] | None = None,
    warnings: list[str] | None = None,
    confidence: float | None = None,
    elapsed_ms: int | None = None,
) -> ChatMessage:
    return ChatMessage(
        id=uuid.uuid4().hex,
        role="assistant",
        text=text,
        created_at=utc_now(),
        sql=sql,
        columns=columns or [],
        rows=rows or [],
        row_count=row_count,
        tables_used=tables_used or [],
        chart_suggestion=chart_suggestion,
        warnings=warnings or [],
        confidence=confidence,
        elapsed_ms=elapsed_ms,
    )


def build_user_message(text: str) -> ChatMessage:
    return ChatMessage(id=uuid.uuid4().hex, role="user", text=text, created_at=utc_now())
