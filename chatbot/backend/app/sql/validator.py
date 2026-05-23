from __future__ import annotations

from dataclasses import dataclass

from sqlglot import exp, parse

from app.core.settings import Settings


class SQLValidationError(ValueError):
    pass


@dataclass
class ValidatedQuery:
    sql: str
    tables_used: list[str]


class SQLValidator:
    def __init__(self, settings: Settings, allowed_tables: set[str]) -> None:
        self._settings = settings
        self._allowed_tables = allowed_tables

    def validate(self, sql: str) -> ValidatedQuery:
        statements = parse(sql, read="trino")
        if len(statements) != 1:
            raise SQLValidationError("Only one SQL statement is allowed.")
        statement = statements[0]
        if statement.find(exp.Select) is None:
            raise SQLValidationError("Only SELECT queries are allowed.")
        if any(isinstance(node, (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Alter)) for node in statement.walk()):
            raise SQLValidationError("Only read-only SQL is allowed.")
        tables_used: list[str] = []
        for table in statement.find_all(exp.Table):
            catalog = table.catalog or self._settings.trino_catalog
            schema = table.db or self._settings.trino_schema
            name = table.name
            if catalog != self._settings.trino_catalog or schema != self._settings.trino_schema:
                raise SQLValidationError("Query references an out-of-scope catalog or schema.")
            if name not in self._allowed_tables:
                raise SQLValidationError(f"Table `{name}` is not allowed.")
            tables_used.append(name)

        if not tables_used:
            raise SQLValidationError("At least one approved Gold table must be referenced.")

        self._reject_dangerous_functions(statement)
        self._enforce_limit(statement)
        return ValidatedQuery(sql=statement.sql(dialect="trino"), tables_used=sorted(set(tables_used)))

    def _enforce_limit(self, statement: exp.Expression) -> None:
        limit = statement.args.get("limit")
        if limit is None or not isinstance(limit.expression, exp.Literal):
            statement.set("limit", exp.Limit(expression=exp.Literal.number(self._settings.sql_default_limit)))
            return
        value = int(limit.expression.this)
        capped = min(value, self._settings.sql_hard_limit)
        statement.set("limit", exp.Limit(expression=exp.Literal.number(capped)))

    @staticmethod
    def _reject_dangerous_functions(statement: exp.Expression) -> None:
        banned = {"read_csv", "http_get", "system", "query"}
        for func in statement.find_all(exp.Func):
            if func.name.lower() in banned:
                raise SQLValidationError("Query uses a disallowed function.")
