from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class SessionSummary(BaseModel):
    id: str
    title: str
    created_at: datetime
    updated_at: datetime


class SessionDetail(SessionSummary):
    messages: list["ChatMessage"]


class ChatMessage(BaseModel):
    id: str
    role: Literal["user", "assistant", "system"]
    text: str
    created_at: datetime
    sql: str | None = None
    columns: list[str] = Field(default_factory=list)
    rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int | None = None
    tables_used: list[str] = Field(default_factory=list)
    chart_suggestion: dict[str, Any] | None = None
    warnings: list[str] = Field(default_factory=list)
    confidence: float | None = None
    elapsed_ms: int | None = None


class CreateSessionResponse(BaseModel):
    session: SessionSummary


class FeedbackRequest(BaseModel):
    rating: Literal["up", "down"]
    note: str | None = None


class UserMessageRequest(BaseModel):
    message: str = Field(min_length=1, max_length=5000)


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    services: dict[str, str]


class SemanticExample(BaseModel):
    id: str
    question: str
    description: str


ChatMessage.model_rebuild()
