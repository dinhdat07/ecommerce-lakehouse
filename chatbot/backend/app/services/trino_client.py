from __future__ import annotations

from typing import Any

import trino

from app.core.settings import Settings


class TrinoQueryService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def healthcheck(self) -> bool:
        try:
            self.execute("SELECT 1 AS ok LIMIT 1")
            return True
        except Exception:
            return False

    def execute(self, sql: str) -> tuple[list[str], list[dict[str, Any]]]:
        connection = trino.dbapi.connect(
            host=self._settings.trino_host,
            port=self._settings.trino_port,
            user=self._settings.trino_user,
            catalog=self._settings.trino_catalog,
            schema=self._settings.trino_schema,
            http_scheme=self._settings.trino_http_scheme,
            session_properties={"query_max_execution_time": f"{self._settings.trino_query_timeout_seconds}s"},
        )
        cursor = connection.cursor()
        try:
            cursor.execute(sql)
            columns = [item[0] for item in (cursor.description or [])]
            raw_rows = cursor.fetchall()
            rows = [self._normalize_row(columns, row) for row in raw_rows[: self._settings.trino_max_rows]]
            return columns, rows
        finally:
            cursor.close()
            connection.close()

    @staticmethod
    def _normalize_row(columns: list[str], row: tuple[Any, ...]) -> dict[str, Any]:
        normalized: dict[str, Any] = {}
        for index, column in enumerate(columns):
            value = row[index]
            if hasattr(value, "isoformat"):
                value = value.isoformat()
            normalized[column] = value
        return normalized
