from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from app.core.models import SemanticExample


@dataclass(frozen=True)
class SemanticTable:
    name: str
    description: str
    grain: str
    key_columns: list[str]
    dimensions: list[str]
    metrics: list[str]
    examples: list[str]


class SemanticCatalog:
    def __init__(self, source_path: Path) -> None:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        self.tables = {
            item["name"]: SemanticTable(
                name=item["name"],
                description=item["description"],
                grain=item["grain"],
                key_columns=item["key_columns"],
                dimensions=item["dimensions"],
                metrics=item["metrics"],
                examples=item["examples"],
            )
            for item in payload["tables"]
        }
        self.examples = [
            SemanticExample(id=item["id"], question=item["question"], description=item["description"])
            for item in payload["starter_questions"]
        ]

    def prompt_context(self) -> str:
        lines: list[str] = []
        for table in self.tables.values():
            lines.append(f"Table: {table.name}")
            lines.append(f"Description: {table.description}")
            lines.append(f"Grain: {table.grain}")
            lines.append(f"Dimensions: {', '.join(table.dimensions)}")
            lines.append(f"Metrics: {', '.join(table.metrics)}")
            lines.append(f"Examples: {', '.join(table.examples)}")
            lines.append("")
        return "\n".join(lines).strip()

    def allowed_tables(self) -> set[str]:
        return set(self.tables)

    def supported_question_hint(self) -> str:
        return (
            "I can answer curated analytics questions about revenue, products, categories, funnels, "
            "retention, repeat purchase, conversion latency, and RFM segmentation using the Gold layer."
        )
