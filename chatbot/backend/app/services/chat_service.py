from __future__ import annotations

import json
import time

from app.core.settings import Settings
from app.llm.client import LLMUnavailableError, VertexLLMClient
from app.services.charts import suggest_chart
from app.services.semantic import SemanticCatalog
from app.services.trino_client import TrinoQueryService
from app.sql.validator import SQLValidationError, SQLValidator
from app.storage.repository import build_assistant_message, build_user_message, ChatRepository


class ChatService:
    def __init__(
        self,
        settings: Settings,
        repository: ChatRepository,
        semantic_catalog: SemanticCatalog,
        llm_client: VertexLLMClient,
        sql_validator: SQLValidator,
        trino_service: TrinoQueryService,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._semantic_catalog = semantic_catalog
        self._llm_client = llm_client
        self._sql_validator = sql_validator
        self._trino_service = trino_service

    def run_message(self, browser_id: str, session_id: str, message: str) -> tuple[list[dict[str, str]], dict]:
        session = self._repository.get_session(browser_id, session_id)
        if session is None:
            raise LookupError("Session not found.")

        user_message = build_user_message(message)
        self._repository.add_message(session_id, user_message)
        events: list[dict[str, str]] = [
            {"event": "message_saved", "data": json.dumps({"messageId": user_message.id, "role": "user"})},
            {"event": "status", "data": json.dumps({"step": "planning"})},
        ]

        plan = self._llm_client.plan(message)
        if plan.action == "refuse":
            assistant = build_assistant_message(
                "That request is outside this demo's supported scope. "
                + self._semantic_catalog.supported_question_hint(),
                confidence=plan.confidence,
                warnings=["Out of scope"],
            )
            self._repository.add_message(session_id, assistant)
            events.append({"event": "completed", "data": assistant.model_dump_json()})
            return events, assistant.model_dump()

        if plan.action == "clarify" and plan.clarification:
            assistant = build_assistant_message(
                plan.clarification,
                confidence=plan.confidence,
                warnings=["Clarification required"],
            )
            self._repository.add_message(session_id, assistant)
            events.append({"event": "completed", "data": assistant.model_dump_json()})
            return events, assistant.model_dump()

        events.append({"event": "status", "data": json.dumps({"step": "generating_sql"})})
        sql_plan = self._llm_client.generate_sql(plan.question)
        try:
            validated = self._sql_validator.validate(sql_plan.sql)
        except SQLValidationError as exc:
            assistant = build_assistant_message(
                (
                    "I could not produce a safe query for that request. "
                    "Try asking about revenue, categories, products, funnels, retention, or RFM."
                ),
                warnings=[str(exc)],
                confidence=sql_plan.confidence,
            )
            self._repository.add_message(session_id, assistant)
            events.append({"event": "completed", "data": assistant.model_dump_json()})
            return events, assistant.model_dump()

        events.append({"event": "sql", "data": json.dumps({"sql": validated.sql})})
        start = time.perf_counter()
        try:
            columns, rows = self._trino_service.execute(validated.sql)
        except Exception as exc:
            assistant = build_assistant_message(
                "The data service is currently unavailable or the query could not complete.",
                sql=validated.sql,
                tables_used=validated.tables_used,
                warnings=[str(exc)],
                confidence=sql_plan.confidence,
            )
            self._repository.add_message(session_id, assistant)
            events.append({"event": "completed", "data": assistant.model_dump_json()})
            return events, assistant.model_dump()
        elapsed_ms = int((time.perf_counter() - start) * 1000)

        events.append({"event": "status", "data": json.dumps({"step": "summarizing"})})
        try:
            answer_text = self._llm_client.summarize(message, rows)
        except LLMUnavailableError:
            answer_text = "I ran the query successfully. Review the returned data and chart for the main pattern."

        chart = suggest_chart(columns, rows)
        warnings: list[str] = []
        if len(rows) >= self._settings.trino_max_rows:
            warnings.append("Result rows were truncated to the demo limit.")
        assistant = build_assistant_message(
            answer_text,
            sql=validated.sql,
            columns=columns,
            rows=rows,
            row_count=len(rows),
            tables_used=validated.tables_used,
            chart_suggestion=chart,
            warnings=warnings,
            confidence=sql_plan.confidence,
            elapsed_ms=elapsed_ms,
        )
        self._repository.add_message(session_id, assistant)
        events.append({"event": "completed", "data": assistant.model_dump_json()})
        return events, assistant.model_dump()
