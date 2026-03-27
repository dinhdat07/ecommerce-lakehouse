"""Schema definitions for E-commerce events."""

from __future__ import annotations

from typing import Dict, List

EVENT_COLUMNS: List[str] = [
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
]

EVENT_SCHEMA_SIMPLE: Dict[str, str] = {
    "event_time": "timestamp",
    "event_type": "string",
    "product_id": "long",
    "category_id": "long",
    "category_code": "string",
    "brand": "string",
    "price": "double",
    "user_id": "long",
    "user_session": "string",
}


def get_required_columns() -> List[str]:
    """Return required canonical columns for all events."""
    return list(EVENT_COLUMNS)


def get_spark_struct_type():
    """Return Spark StructType for canonical events.

    Spark is optional in local unit tests. If pyspark is unavailable,
    this function raises ImportError naturally.
    """
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

    return StructType(
        [
            StructField("event_time", TimestampType(), True),
            StructField("event_type", StringType(), True),
            StructField("product_id", LongType(), True),
            StructField("category_id", LongType(), True),
            StructField("category_code", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("user_id", LongType(), True),
            StructField("user_session", StringType(), True),
        ]
    )
