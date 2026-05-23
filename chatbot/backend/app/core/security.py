from __future__ import annotations

from itsdangerous import BadSignature, URLSafeTimedSerializer

from app.core.settings import Settings


class SessionSigner:
    def __init__(self, settings: Settings) -> None:
        self._serializer = URLSafeTimedSerializer(settings.session_secret, salt=settings.cookie_name)
        self._max_age = settings.session_ttl_hours * 3600

    def dumps(self, session_id: str) -> str:
        return self._serializer.dumps(session_id)

    def loads(self, token: str) -> str | None:
        try:
            return str(self._serializer.loads(token, max_age=self._max_age))
        except BadSignature:
            return None
