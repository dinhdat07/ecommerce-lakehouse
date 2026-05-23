"""Product affinity Gold helper for the filesystem-backed local pipeline."""

from __future__ import annotations

from collections import defaultdict
from itertools import combinations


def build_product_affinity_table(silver_rows: list[dict], *, top_pairs: int = 200) -> list[dict]:
    """Build product-pair affinity from products purchased in the same session."""

    session_products: dict[tuple[int | None, str | None, str], set[int]] = defaultdict(set)
    for row in silver_rows:
        if row.get("event_type") != "purchase" or row.get("product_id") is None:
            continue
        session_key = (row.get("user_id"), row.get("user_session"), row["event_date"])
        session_products[session_key].add(int(row["product_id"]))

    pair_counts: dict[tuple[int, int], int] = defaultdict(int)
    product_session_counts: dict[int, int] = defaultdict(int)
    total_sessions = 0
    for products in session_products.values():
        if not products:
            continue
        total_sessions += 1
        for product_id in products:
            product_session_counts[product_id] += 1
        for product_a, product_b in combinations(sorted(products), 2):
            pair_counts[(product_a, product_b)] += 1

    if total_sessions == 0:
        return []

    results: list[dict] = []
    for (product_a, product_b), co_purchase_sessions in sorted(pair_counts.items(), key=lambda item: (-item[1], item[0])):
        session_count_a = product_session_counts[product_a]
        session_count_b = product_session_counts[product_b]
        lift_denominator = session_count_a * session_count_b
        affinity_lift = round((co_purchase_sessions * total_sessions) / lift_denominator, 4) if lift_denominator else 0.0
        results.append(
            {
                "product_a": product_a,
                "product_b": product_b,
                "co_purchase_sessions": co_purchase_sessions,
                "affinity_lift": affinity_lift,
            }
        )
        if len(results) >= top_pairs:
            break
    return results
