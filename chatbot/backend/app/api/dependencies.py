from __future__ import annotations

from fastapi import Depends, Request, Response

from app.core.security import SessionSigner
from app.core.settings import Settings, get_settings
from app.llm.client import VertexLLMClient
from app.services.chat_service import ChatService
from app.services.semantic import SemanticCatalog
from app.services.trino_client import TrinoQueryService
from app.sql.validator import SQLValidator
from app.storage.repository import ChatRepository


def get_semantic_catalog(settings: Settings = Depends(get_settings)) -> SemanticCatalog:
    return SemanticCatalog(settings.data_dir.parent.parent / "shared" / "semantic" / "gold_catalog.json")


def get_repository(settings: Settings = Depends(get_settings)) -> ChatRepository:
    return ChatRepository(settings.data_dir / "chatbot.sqlite3")


def get_signer(settings: Settings = Depends(get_settings)) -> SessionSigner:
    return SessionSigner(settings)


def get_browser_id(
    request: Request,
    response: Response,
    signer: SessionSigner = Depends(get_signer),
    settings: Settings = Depends(get_settings),
) -> str:
    token = request.cookies.get(settings.cookie_name)
    browser_id = signer.loads(token) if token else None
    if browser_id:
        return browser_id
    import uuid

    browser_id = uuid.uuid4().hex
    response.set_cookie(
        settings.cookie_name,
        signer.dumps(browser_id),
        max_age=settings.session_ttl_hours * 3600,
        httponly=True,
        secure=False,
        samesite="lax",
    )
    return browser_id


def get_chat_service(
    settings: Settings = Depends(get_settings),
    repository: ChatRepository = Depends(get_repository),
    semantic_catalog: SemanticCatalog = Depends(get_semantic_catalog),
) -> ChatService:
    return ChatService(
        settings=settings,
        repository=repository,
        semantic_catalog=semantic_catalog,
        llm_client=VertexLLMClient(settings, semantic_catalog),
        sql_validator=SQLValidator(settings, semantic_catalog.allowed_tables()),
        trino_service=TrinoQueryService(settings),
    )
