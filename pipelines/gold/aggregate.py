"""Silver-to-Gold aggregation logic.

This module computes BI-facing Gold outputs from canonical Silver events. The
aggregations are intentionally deterministic and filesystem-backed so they can
be validated locally today and later migrated to Iceberg-managed tables.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from common.config import AppConfig
from common.manifests import RunManifest, write_manifest
from common.runtime import build_run_id
from common.storage import iter_jsonl_files, read_jsonl, write_csv


@dataclass(frozen=True)
class GoldAggregationResult:
    """Summary of one Silver-to-Gold run."""

    run_id: str
    rows_read: int
    output_paths: list[Path]
    manifest_path: Path


def _safe_divide(numerator: float, denominator: float) -> float:
    """Return a rounded division result or zero when the denominator is zero."""

    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def build_gold_tables(silver_rows: list[dict]) -> dict[str, list[dict]]:
    """Build the Gold analytics tables from canonical Silver rows."""

    user_summary: dict[tuple[str, int], dict] = {}
    product_summary: dict[tuple[str, int], dict] = {}
    revenue_by_category: dict[tuple[str, int | None, str | None], dict] = {}
    funnel_by_date: dict[str, dict] = {}
    session_summary: dict[tuple[str, int | None, str | None], dict] = {}
    product_users: dict[tuple[str, int], set[int]] = defaultdict(set)
    category_buyers: dict[tuple[str, int | None, str | None], set[int]] = defaultdict(set)

    for row in sorted(silver_rows, key=lambda item: (item["event_time"], item["event_id"])):
        rows_key = (row["event_date"], row["user_id"])
        user_row = user_summary.setdefault(
            rows_key,
            {
                "event_date": row["event_date"],
                "user_id": row["user_id"],
                "total_events": 0,
                "views": 0,
                "carts": 0,
                "purchases": 0,
                "purchase_revenue": 0.0,
                "distinct_products": set(),
                "first_event_time": row["event_time"],
                "last_event_time": row["event_time"],
            },
        )
        user_row["total_events"] += 1
        user_row["distinct_products"].add(row["product_id"])
        user_row["first_event_time"] = min(user_row["first_event_time"], row["event_time"])
        user_row["last_event_time"] = max(user_row["last_event_time"], row["event_time"])

        product_key = (row["event_date"], row["product_id"])
        product_row = product_summary.setdefault(
            product_key,
            {
                "event_date": row["event_date"],
                "product_id": row["product_id"],
                "category_id": row["category_id"],
                "category_code": row["category_code"],
                "brand": row["brand"],
                "views": 0,
                "carts": 0,
                "purchases": 0,
                "purchase_revenue": 0.0,
            },
        )

        category_key = (row["event_date"], row["category_id"], row["category_code"])
        category_row = revenue_by_category.setdefault(
            category_key,
            {
                "event_date": row["event_date"],
                "category_id": row["category_id"],
                "category_code": row["category_code"],
                "purchase_count": 0,
                "purchase_revenue": 0.0,
                "unique_buyers": 0,
                "average_order_value": 0.0,
            },
        )

        funnel_row = funnel_by_date.setdefault(
            row["event_date"],
            {
                "event_date": row["event_date"],
                "views": 0,
                "carts": 0,
                "purchases": 0,
                "view_to_cart_rate": 0.0,
                "cart_to_purchase_rate": 0.0,
                "view_to_purchase_rate": 0.0,
            },
        )

        session_key = (row["user_id"], row["user_session"], row["event_date"])
        session_row = session_summary.setdefault(
            session_key,
            {
                "event_date": row["event_date"],
                "user_id": row["user_id"],
                "user_session": row["user_session"],
                "first_event_time": row["event_time"],
                "last_event_time": row["event_time"],
                "total_events": 0,
                "views": 0,
                "carts": 0,
                "purchases": 0,
                "purchase_revenue": 0.0,
            },
        )

        event_type = row["event_type"]
        if event_type == "view":
            user_row["views"] += 1
            product_row["views"] += 1
            funnel_row["views"] += 1
            session_row["views"] += 1
        elif event_type == "cart":
            user_row["carts"] += 1
            product_row["carts"] += 1
            funnel_row["carts"] += 1
            session_row["carts"] += 1
        elif event_type == "purchase":
            revenue = float(row["price"] or 0.0)
            user_row["purchases"] += 1
            user_row["purchase_revenue"] += revenue
            product_row["purchases"] += 1
            product_row["purchase_revenue"] += revenue
            category_row["purchase_count"] += 1
            category_row["purchase_revenue"] += revenue
            funnel_row["purchases"] += 1
            session_row["purchases"] += 1
            session_row["purchase_revenue"] += revenue
            if row["user_id"] is not None:
                category_buyers[category_key].add(row["user_id"])

        session_row["total_events"] += 1
        session_row["first_event_time"] = min(session_row["first_event_time"], row["event_time"])
        session_row["last_event_time"] = max(session_row["last_event_time"], row["event_time"])
        if row["user_id"] is not None:
            product_users[product_key].add(row["user_id"])

    user_rows: list[dict] = []
    for row in user_summary.values():
        row["distinct_products"] = len(row["distinct_products"])
        row["purchase_revenue"] = round(row["purchase_revenue"], 2)
        user_rows.append(row)

    product_rows: list[dict] = []
    for key, row in product_summary.items():
        row["unique_users"] = len(product_users[key])
        row["view_to_purchase_rate"] = _safe_divide(row["purchases"], row["views"])
        row["purchase_revenue"] = round(row["purchase_revenue"], 2)
        product_rows.append(row)

    category_rows: list[dict] = []
    for key, row in revenue_by_category.items():
        row["unique_buyers"] = len(category_buyers[key])
        row["purchase_revenue"] = round(row["purchase_revenue"], 2)
        row["average_order_value"] = round(
            _safe_divide(row["purchase_revenue"], row["purchase_count"]),
            2,
        )
        category_rows.append(row)

    funnel_rows: list[dict] = []
    for row in funnel_by_date.values():
        row["view_to_cart_rate"] = _safe_divide(row["carts"], row["views"])
        row["cart_to_purchase_rate"] = _safe_divide(row["purchases"], row["carts"])
        row["view_to_purchase_rate"] = _safe_divide(row["purchases"], row["views"])
        funnel_rows.append(row)

    session_rows: list[dict] = []
    for row in session_summary.values():
        row["purchase_revenue"] = round(row["purchase_revenue"], 2)
        session_rows.append(row)

    return {
        "user_activity_summary": sorted(user_rows, key=lambda item: (item["event_date"], item["user_id"])),
        "product_popularity": sorted(product_rows, key=lambda item: (item["event_date"], item["product_id"])),
        "conversion_funnel": sorted(funnel_rows, key=lambda item: item["event_date"]),
        "revenue_by_category": sorted(
            category_rows,
            key=lambda item: (item["event_date"], item["category_id"] or -1, item["category_code"] or ""),
        ),
        "session_summary": sorted(
            session_rows,
            key=lambda item: (item["event_date"], item["user_id"] or -1, item["user_session"] or ""),
        ),
    }


def run_silver_to_gold(
    config: AppConfig,
    *,
    silver_root: Path | None = None,
    gold_root: Path | None = None,
) -> GoldAggregationResult:
    """Aggregate Silver rows into Gold analytics tables."""

    stage_name = "silver_to_gold"
    run_id = build_run_id("gold")
    silver_root = silver_root or config.silver_local_path
    gold_root = gold_root or config.gold_local_path

    silver_rows: list[dict] = []
    for jsonl_file in iter_jsonl_files(silver_root):
        if "_quarantine" in jsonl_file.parts:
            continue
        silver_rows.extend(read_jsonl(jsonl_file))

    tables = build_gold_tables(silver_rows)
    output_paths: list[Path] = []
    for table_name, rows in tables.items():
        if not rows:
            continue
        output_path = gold_root / table_name / f"run_id={run_id}" / "records.csv"
        write_csv(output_path, rows, fieldnames=list(rows[0].keys()))
        output_paths.append(output_path)

    manifest = RunManifest(
        stage=stage_name,
        run_id=run_id,
        status="succeeded",
        inputs=[str(silver_root)],
        outputs=[str(path) for path in output_paths],
        metrics={"rows_read": len(silver_rows), "tables_written": len(output_paths)},
    )
    manifest_path = write_manifest(config.manifest_root, manifest)

    return GoldAggregationResult(
        run_id=run_id,
        rows_read=len(silver_rows),
        output_paths=output_paths,
        manifest_path=manifest_path,
    )
