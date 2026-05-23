from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from app.core.settings import Settings
from app.services.semantic import SemanticCatalog


class LLMUnavailableError(RuntimeError):
    pass


@dataclass
class PlanResult:
    action: str
    question: str
    clarification: str | None
    confidence: float


@dataclass
class SQLResult:
    sql: str
    explanation: str
    confidence: float


class VertexLLMClient:
    def __init__(self, settings: Settings, semantic_catalog: SemanticCatalog) -> None:
        self._settings = settings
        self._semantic_catalog = semantic_catalog

    def plan(self, question: str) -> PlanResult:
        if not self._settings.vertex_api_key:
            return self._mock_plan(question)
        prompt = (
            "You are an analytics intent planner. Return compact JSON with keys "
            "`action`, `question`, `clarification`, `confidence`. "
            "Actions: `answer`, `clarify`, `refuse`. "
            "Refuse if the question is outside curated ecommerce analytics scope.\n\n"
            f"Supported scope:\n{self._semantic_catalog.supported_question_hint()}\n\n"
            f"User question: {question}"
        )
        payload = self._generate_text(prompt)
        data = self._parse_json(payload)
        return PlanResult(
            action=str(data.get("action", "clarify")),
            question=str(data.get("question", question)),
            clarification=data.get("clarification"),
            confidence=self._coerce_confidence(data.get("confidence", 0.5)),
        )

    def generate_sql(self, question: str) -> SQLResult:
        if not self._settings.vertex_api_key:
            return self._mock_sql(question)
        prompt = (
            "You are a Trino SQL generator for a safe analytics chatbot.\n"
            "Rules:\n"
            "- Return JSON with keys `sql`, `explanation`, `confidence`\n"
            "- Produce exactly one SELECT statement\n"
            "- Use only tables from catalog iceberg schema demo\n"
            "- Use only the tables described below\n"
            f"- Always include LIMIT {self._settings.sql_default_limit} or lower\n"
            "- Never reference Bronze or Silver\n"
            "- Never invent columns\n\n"
            f"{self._semantic_catalog.prompt_context()}\n\n"
            f"Question: {question}"
        )
        payload = self._generate_text(prompt)
        data = self._parse_json(payload)
        return SQLResult(
            sql=str(data["sql"]),
            explanation=str(data.get("explanation", "")),
            confidence=self._coerce_confidence(data.get("confidence", 0.7)),
        )

    def summarize(self, question: str, rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "No rows were returned for this question."
        if not self._settings.vertex_api_key:
            return self._mock_summary(question, rows)
        sample = rows[:5]
        prompt = (
            "You are an analytics assistant. Summarize the result in English for a business user.\n"
            "- Use only the provided rows.\n"
            "- Be concise and avoid speculation.\n"
            "- Mention if the result is truncated.\n\n"
            f"Question: {question}\nRows: {json.dumps(sample, ensure_ascii=True)}"
        )
        return self._generate_text(prompt).strip()

    def _generate_text(self, prompt: str) -> str:
        try:
            from google import genai
        except ImportError as exc:
            raise LLMUnavailableError("google-genai is not installed") from exc

        try:
            client = genai.Client(vertexai=True, api_key=self._settings.vertex_api_key)
            response = client.models.generate_content(model=self._settings.vertex_model, contents=prompt)
            return str(getattr(response, "text", "")).strip()
        except Exception as exc:  # pragma: no cover - network/runtime errors
            if self._settings.allow_mock_llm:
                return ""
            raise LLMUnavailableError("Vertex AI request failed") from exc

    @staticmethod
    def _parse_json(payload: str) -> dict[str, Any]:
        if not payload:
            return {}
        payload = payload.strip()
        fenced = re.search(r"```json\s*(\{.*?\})\s*```", payload, re.DOTALL)
        if fenced:
            payload = fenced.group(1)
        return json.loads(payload)

    @staticmethod
    def _coerce_confidence(value: Any) -> float:
        if isinstance(value, (int, float)):
            return max(0.0, min(float(value), 1.0))
        if isinstance(value, str):
            normalized = value.strip().lower()
            mapping = {
                "very high": 0.95,
                "high": 0.85,
                "medium": 0.65,
                "moderate": 0.65,
                "low": 0.35,
                "very low": 0.15,
            }
            if normalized in mapping:
                return mapping[normalized]
            try:
                return max(0.0, min(float(normalized), 1.0))
            except ValueError:
                return 0.5
        return 0.5

    @staticmethod
    def _mock_plan(question: str) -> PlanResult:
        lowered = question.lower()
        vague = {"performance", "how is it going", "insights", "what happened"}
        if any(token in lowered for token in vague) and "revenue" not in lowered and "conversion" not in lowered:
            return PlanResult(
                action="clarify",
                question=question,
                clarification="Do you want revenue, conversion, retention, product, or category performance?",
                confidence=0.55,
            )
        return PlanResult(action="answer", question=question, clarification=None, confidence=0.72)

    def _mock_sql(self, question: str) -> SQLResult:
        lowered = question.lower()
        if "category" in lowered:
            return SQLResult(
                sql=(
                    "SELECT category_code, SUM(purchase_revenue) AS revenue "
                    "FROM iceberg.demo.category_performance_daily "
                    "GROUP BY 1 ORDER BY revenue DESC LIMIT 12"
                ),
                explanation="Category revenue leaderboard.",
                confidence=0.65,
            )
        if "retention" in lowered or "cohort" in lowered:
            return SQLResult(
                sql=(
                    "SELECT cohort_month, period_offset, retention_rate "
                    "FROM iceberg.demo.cohort_retention "
                    "ORDER BY cohort_month, period_offset LIMIT 120"
                ),
                explanation="Cohort retention matrix source.",
                confidence=0.64,
            )
        if "product" in lowered:
            return SQLResult(
                sql=(
                    "SELECT product_id, SUM(purchase_revenue) AS revenue "
                    "FROM iceberg.demo.top_products GROUP BY 1 ORDER BY revenue DESC LIMIT 10"
                ),
                explanation="Top products by revenue.",
                confidence=0.66,
            )
        return SQLResult(
            sql=(
                "SELECT event_date, SUM(purchase_revenue) AS revenue, SUM(purchase_count) AS purchases "
                "FROM iceberg.demo.daily_revenue "
                "GROUP BY 1 ORDER BY event_date ASC LIMIT 90"
            ),
            explanation="Daily revenue trend.",
            confidence=0.68,
        )

    @staticmethod
    def _mock_summary(question: str, rows: list[dict[str, Any]]) -> str:
        first = rows[0]
        keys = ", ".join(first.keys())
        return (
            f"I answered the question using {len(rows)} rows. "
            f"The result includes these fields: {keys}. "
            "Use the chart and SQL panels to inspect the trend or ranking in more detail."
        )
