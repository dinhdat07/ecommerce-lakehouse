from __future__ import annotations

import json
from typing import Iterator

from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import JSONResponse, StreamingResponse

from app.api.dependencies import get_browser_id, get_chat_service, get_repository, get_semantic_catalog
from app.core.models import CreateSessionResponse, FeedbackRequest, HealthResponse, UserMessageRequest
from app.core.settings import Settings, get_settings
from app.services.chat_service import ChatService
from app.services.semantic import SemanticCatalog
from app.services.trino_client import TrinoQueryService
from app.storage.repository import ChatRepository

router = APIRouter()


@router.get("/health", response_model=HealthResponse)
def health(
    settings: Settings = Depends(get_settings),
    repository: ChatRepository = Depends(get_repository),
) -> HealthResponse:
    _ = repository
    return HealthResponse(status="ok", services={"api": "ok", "storage": "ok", "environment": settings.environment})


@router.get("/ready", response_model=HealthResponse)
def ready(
    settings: Settings = Depends(get_settings),
) -> HealthResponse:
    trino_ready = TrinoQueryService(settings).healthcheck()
    llm_state = "ok" if settings.vertex_api_key or settings.allow_mock_llm else "missing_api_key"
    degraded = not trino_ready or llm_state != "ok"
    return HealthResponse(
        status="degraded" if degraded else "ok",
        services={"trino": "ok" if trino_ready else "down", "vertex": llm_state},
    )


@router.get("/semantic/examples")
def semantic_examples(semantic_catalog: SemanticCatalog = Depends(get_semantic_catalog)) -> list[dict]:
    return [item.model_dump() for item in semantic_catalog.examples]


@router.post("/chat/sessions", response_model=CreateSessionResponse)
def create_session(
    response: Response,
    browser_id: str = Depends(get_browser_id),
    repository: ChatRepository = Depends(get_repository),
) -> CreateSessionResponse:
    session = repository.create_session(browser_id, "New conversation")
    return CreateSessionResponse(session=session)


@router.get("/chat/sessions")
def list_sessions(
    response: Response,
    browser_id: str = Depends(get_browser_id),
    repository: ChatRepository = Depends(get_repository),
) -> list[dict]:
    return [item.model_dump() for item in repository.list_sessions(browser_id)]


@router.get("/chat/sessions/{session_id}")
def get_session(
    session_id: str,
    response: Response,
    browser_id: str = Depends(get_browser_id),
    repository: ChatRepository = Depends(get_repository),
) -> dict:
    session = repository.get_session(browser_id, session_id)
    if session is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")
    return session.model_dump()


@router.post("/chat/sessions/{session_id}/messages")
def post_message(
    session_id: str,
    payload: UserMessageRequest,
    response: Response,
    browser_id: str = Depends(get_browser_id),
    chat_service: ChatService = Depends(get_chat_service),
) -> StreamingResponse:
    try:
        events, _ = chat_service.run_message(browser_id, session_id, payload.message)
    except LookupError:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Session not found")

    def event_stream() -> Iterator[str]:
        for item in events:
            yield f"event: {item['event']}\ndata: {item['data']}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@router.post("/chat/messages/{message_id}/feedback")
def submit_feedback(
    message_id: str,
    payload: FeedbackRequest,
    repository: ChatRepository = Depends(get_repository),
    settings: Settings = Depends(get_settings),
) -> JSONResponse:
    note = (payload.note or "").strip() or None
    if note and len(note) > settings.feedback_note_max_length:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Feedback note is too long")
    repository.add_feedback(message_id, payload.rating, note)
    return JSONResponse({"status": "ok"})
