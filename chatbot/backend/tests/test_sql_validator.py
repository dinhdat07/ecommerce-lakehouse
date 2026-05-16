from __future__ import annotations

import pytest

from app.core.settings import Settings
from app.sql.validator import SQLValidationError, SQLValidator


@pytest.fixture
def validator() -> SQLValidator:
    settings = Settings(session_secret="test-secret")
    return SQLValidator(settings, {"daily_revenue", "top_products"})


def test_allows_single_select(validator: SQLValidator) -> None:
    result = validator.validate(
        "SELECT event_date, SUM(purchase_revenue) AS revenue FROM iceberg.demo.daily_revenue GROUP BY 1"
    )
    assert "LIMIT" in result.sql
    assert result.tables_used == ["daily_revenue"]


def test_rejects_wrong_schema(validator: SQLValidator) -> None:
    with pytest.raises(SQLValidationError):
        validator.validate("SELECT * FROM system.runtime.nodes")


def test_rejects_multiple_statements(validator: SQLValidator) -> None:
    with pytest.raises(SQLValidationError):
        validator.validate("SELECT * FROM iceberg.demo.daily_revenue; SELECT 2")
