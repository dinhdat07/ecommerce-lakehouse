"""Bootstrap Superset assets for the laptop stack.

This script creates a Trino database connection, datasets for the Gold Iceberg
 tables, richer saved charts, and multiple dashboards so the user can open
 Superset and explore the sample data from several business angles.
"""

from __future__ import annotations

import json
import os

from flask import current_app
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError


CHART_SPECS: list[dict[str, object]] = [
    {
        "table_name": "daily_revenue",
        "slice_name": "Executive KPI - Total Revenue",
        "viz_type": "big_number_total",
        "metric": {"column": "purchase_revenue", "aggregate": "SUM", "label": "Revenue"},
        "y_axis_format": "$,.2f",
    },
    {
        "table_name": "daily_revenue",
        "slice_name": "Executive KPI - Total Purchases",
        "viz_type": "big_number_total",
        "metric": {"column": "purchase_count", "aggregate": "SUM", "label": "Purchases"},
        "y_axis_format": "SMART_NUMBER",
    },
    {
        "table_name": "daily_revenue",
        "slice_name": "Executive KPI - Average Purchase Value",
        "viz_type": "big_number_total",
        "metric": {
            "label": "Average purchase value",
            "sqlExpression": "SUM(purchase_revenue) / NULLIF(SUM(purchase_count), 0)",
        },
        "y_axis_format": "$,.2f",
    },
    {
        "table_name": "repeat_purchase",
        "slice_name": "Executive KPI - Weighted Repeat Purchase Rate",
        "viz_type": "big_number_total",
        "metric": {
            "label": "Weighted repeat rate",
            "sqlExpression": "CAST(SUM(repeat_purchasers) AS DOUBLE) / NULLIF(SUM(purchasers), 0)",
        },
        "y_axis_format": ".1%",
    },
    {
        "table_name": "session_funnel",
        "slice_name": "Executive KPI - Session Conversion Rate",
        "viz_type": "big_number_total",
        "metric": {
            "label": "Session conversion rate",
            "sqlExpression": "CAST(SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(session_id), 0)",
        },
        "y_axis_format": ".1%",
    },
    {
        "table_name": "daily_revenue",
        "slice_name": "Revenue and Purchases by Day",
        "viz_type": "mixed_timeseries",
        "x_axis": "event_date",
        "time_grain_sqla": "P1D",
        "metric": {"column": "purchase_revenue", "aggregate": "SUM", "label": "Revenue"},
        "metric_b": {"column": "purchase_count", "aggregate": "SUM", "label": "Purchases"},
        "series_type": "line",
        "series_type_b": "bar",
        "groupby": [],
        "groupby_b": [],
        "row_limit": 400,
        "y_axis_format": "$,.2f",
        "y_axis_format_secondary": "SMART_NUMBER",
    },
    {
        "table_name": "conversion_funnel_daily",
        "slice_name": "Daily Funnel Activity",
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_date",
        "time_grain_sqla": "P1D",
        "metrics": [
            {"column": "views", "aggregate": "SUM", "label": "Views"},
            {"column": "carts", "aggregate": "SUM", "label": "Carts"},
            {"column": "purchases", "aggregate": "SUM", "label": "Purchases"},
        ],
        "groupby": [],
        "row_limit": 400,
        "y_axis_format": "SMART_NUMBER",
    },
    {
        "table_name": "conversion_funnel_daily",
        "slice_name": "Funnel Conversion Rates",
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_date",
        "time_grain_sqla": "P1D",
        "metrics": [
            {"column": "view_to_cart_rate", "aggregate": "AVG", "label": "View to Cart"},
            {"column": "cart_to_purchase_rate", "aggregate": "AVG", "label": "Cart to Purchase"},
            {"column": "view_to_purchase_rate", "aggregate": "AVG", "label": "View to Purchase"},
        ],
        "groupby": [],
        "row_limit": 400,
        "y_axis_format": ".1%",
    },
    {
        "table_name": "session_funnel",
        "slice_name": "Session Conversion Rate by Day",
        "viz_type": "echarts_timeseries_line",
        "x_axis": "event_date",
        "time_grain_sqla": "P1D",
        "metrics": [
            {
                "label": "Session conversion rate",
                "sqlExpression": "CAST(SUM(CASE WHEN converted THEN 1 ELSE 0 END) AS DOUBLE) / NULLIF(COUNT(session_id), 0)",
            }
        ],
        "groupby": [],
        "row_limit": 400,
        "y_axis_format": ".1%",
    },
    {
        "table_name": "top_products",
        "slice_name": "Top-10 Product Revenue by Day",
        "viz_type": "dist_bar",
        "groupby": ["product_id"],
        "metrics": [{"column": "purchase_revenue", "aggregate": "SUM", "label": "Revenue"}],
        "row_limit": 10,
        "order_bars": True,
        "show_bar_value": False,
        "x_axis_label": "Product ID",
        "y_axis_format": "$,.2f",
    },
    {
        "table_name": "category_performance_daily",
        "slice_name": "Revenue Mix by Category",
        "viz_type": "treemap_v2",
        "groupby": ["category_code"],
        "metric": {"column": "purchase_revenue", "aggregate": "SUM", "label": "Revenue"},
        "row_limit": 25,
        "show_labels": True,
        "show_upper_labels": True,
        "label_type": "key_value",
    },
    {
        "table_name": "category_performance_daily",
        "slice_name": "Category Revenue Leaderboard",
        "viz_type": "dist_bar",
        "groupby": ["category_code"],
        "metrics": [{"column": "purchase_revenue", "aggregate": "SUM", "label": "Revenue"}],
        "row_limit": 12,
        "order_bars": True,
        "show_bar_value": True,
        "x_axis_label": "Category",
        "y_axis_format": "$,.2f",
    },
    {
        "table_name": "time_to_conversion_distribution",
        "slice_name": "Conversion Latency Distribution",
        "viz_type": "dist_bar",
        "groupby": ["time_bucket"],
        "metrics": [{"column": "conversions", "aggregate": "SUM", "label": "Conversions"}],
        "row_limit": 12,
        "order_bars": True,
        "show_bar_value": True,
        "show_legend": False,
        "x_axis_label": "Time bucket",
        "y_axis_format": "SMART_NUMBER",
    },
    {
        "table_name": "repeat_purchase",
        "slice_name": "Repeat Purchase Rate by Month",
        "viz_type": "dist_bar",
        "groupby": ["activity_month"],
        "metrics": [{"column": "repeat_purchase_rate", "aggregate": "AVG", "label": "Repeat rate"}],
        "row_limit": 24,
        "order_bars": False,
        "show_bar_value": True,
        "y_axis_format": ".1%",
        "x_axis_label": "Month",
    },
    {
        "table_name": "cohort_retention",
        "slice_name": "Cohort Retention Heatmap",
        "viz_type": "heatmap_v2",
        "x_axis": "cohort_month",
        "groupby": "period_offset",
        "metric": {"column": "retention_rate", "aggregate": "AVG", "label": "Retention rate"},
        "row_limit": 120,
        "show_legend": True,
        "show_percentage": True,
        "show_values": True,
        "legend_type": "continuous",
        "linear_color_scheme": "superset_seq_1",
        "y_axis_format": ".1%",
    },
    {
        "table_name": "session_funnel",
        "slice_name": "Session Path Distribution",
        "viz_type": "dist_bar",
        "groupby": ["path_label"],
        "metrics": [{"column": "session_id", "aggregate": "COUNT", "label": "Sessions"}],
        "row_limit": 10,
        "order_bars": True,
        "show_bar_value": True,
        "show_legend": False,
        "x_axis_label": "Session path",
        "y_axis_format": "SMART_NUMBER",
    },
    {
        "table_name": "rfm_segmentation",
        "slice_name": "RFM Segment Mix",
        "viz_type": "pie",
        "groupby": ["rfm_segment"],
        "metric": {"column": "user_id", "aggregate": "COUNT", "label": "Users"},
        "row_limit": 10,
        "show_labels": True,
        "labels_outside": True,
        "show_legend": True,
        "sort_by_metric": True,
    },
    {
        "table_name": "product_affinity",
        "slice_name": "Cross-sell Affinity Heatmap",
        "viz_type": "heatmap_v2",
        "x_axis": "product_a",
        "groupby": "product_b",
        "metric": {"column": "affinity_lift", "aggregate": "AVG", "label": "Affinity lift"},
        "row_limit": 200,
        "show_legend": True,
        "show_percentage": False,
        "show_values": False,
        "legend_type": "continuous",
        "linear_color_scheme": "superset_seq_1",
        "y_axis_format": ".2f",
    },
]

DASHBOARD_SPECS: list[dict[str, object]] = [
    {
        "slug": "lakehouse-sample-dashboard",
        "title": "Lakehouse Executive BI Dashboard",
        "description": (
            "Executive view across the ecommerce gold layer with purchase-event KPIs, "
            "session conversion, funnel health, category mix, and customer quality. "
            "October-November are full historical loads; December is the benchmark streaming sample "
            "spread across the month, so December volumes are directional rather than full-month totals."
        ),
        "charts": [
            {"slice_name": "Executive KPI - Total Revenue", "row": 1, "width": 3, "height": 18},
            {"slice_name": "Executive KPI - Total Purchases", "row": 1, "width": 3, "height": 18},
            {"slice_name": "Executive KPI - Average Purchase Value", "row": 1, "width": 3, "height": 18},
            {"slice_name": "Executive KPI - Session Conversion Rate", "row": 1, "width": 3, "height": 18},
            {"slice_name": "Revenue and Purchases by Day", "row": 2, "width": 8, "height": 42},
            {"slice_name": "RFM Segment Mix", "row": 2, "width": 4, "height": 42},
            {"slice_name": "Daily Funnel Activity", "row": 3, "width": 7, "height": 40},
            {"slice_name": "Session Path Distribution", "row": 3, "width": 5, "height": 40},
            {"slice_name": "Revenue Mix by Category", "row": 4, "width": 7, "height": 40},
            {"slice_name": "Conversion Latency Distribution", "row": 4, "width": 5, "height": 40},
            {"slice_name": "Repeat Purchase Rate by Month", "row": 5, "width": 5, "height": 40},
            {"slice_name": "Cohort Retention Heatmap", "row": 5, "width": 7, "height": 40},
        ],
    },
    {
        "slug": "lakehouse-retention-dashboard",
        "title": "Lakehouse Customer and Retention Dashboard",
        "description": (
            "Customer quality dashboard focused on repeat behaviour, cohort stickiness, "
            "session conversion, RFM mix, and the speed from interest to purchase. "
            "December remains a sampled replay month."
        ),
        "charts": [
            {"slice_name": "Executive KPI - Weighted Repeat Purchase Rate", "row": 1, "width": 4, "height": 18},
            {"slice_name": "Executive KPI - Session Conversion Rate", "row": 1, "width": 4, "height": 18},
            {"slice_name": "Executive KPI - Average Purchase Value", "row": 1, "width": 4, "height": 18},
            {"slice_name": "Funnel Conversion Rates", "row": 2, "width": 7, "height": 40},
            {"slice_name": "RFM Segment Mix", "row": 2, "width": 5, "height": 40},
            {"slice_name": "Repeat Purchase Rate by Month", "row": 3, "width": 5, "height": 40},
            {"slice_name": "Cohort Retention Heatmap", "row": 3, "width": 7, "height": 40},
            {"slice_name": "Session Conversion Rate by Day", "row": 4, "width": 7, "height": 40},
            {"slice_name": "Conversion Latency Distribution", "row": 4, "width": 5, "height": 40},
            {"slice_name": "Session Path Distribution", "row": 5, "width": 12, "height": 40},
        ],
    },
    {
        "slug": "lakehouse-merchandising-dashboard",
        "title": "Lakehouse Product and Merchandising Dashboard",
        "description": (
            "Merchandising view of product demand, category concentration, cross-sell affinity, "
            "and purchase momentum. Product charts are sourced from Gold tables only; "
            "the top-product view reflects products that enter the daily top-10 revenue cut."
        ),
        "charts": [
            {"slice_name": "Executive KPI - Total Revenue", "row": 1, "width": 4, "height": 18},
            {"slice_name": "Executive KPI - Total Purchases", "row": 1, "width": 4, "height": 18},
            {"slice_name": "Executive KPI - Average Purchase Value", "row": 1, "width": 4, "height": 18},
            {"slice_name": "Top-10 Product Revenue by Day", "row": 2, "width": 5, "height": 40},
            {"slice_name": "Category Revenue Leaderboard", "row": 2, "width": 7, "height": 40},
            {"slice_name": "Revenue Mix by Category", "row": 3, "width": 6, "height": 40},
            {"slice_name": "Cross-sell Affinity Heatmap", "row": 3, "width": 6, "height": 40},
            {"slice_name": "Conversion Latency Distribution", "row": 4, "width": 5, "height": 40},
            {"slice_name": "Session Path Distribution", "row": 4, "width": 7, "height": 40},
            {"slice_name": "Revenue and Purchases by Day", "row": 5, "width": 12, "height": 42},
        ],
    },
]

DASHBOARD_CSS = """
.dashboard-content {
  background:
    radial-gradient(circle at top left, rgba(16, 185, 129, 0.10), transparent 28%),
    radial-gradient(circle at top right, rgba(14, 165, 233, 0.12), transparent 26%),
    linear-gradient(180deg, #f4f7fb 0%, #ebf1f8 100%);
}

.dashboard-header {
  background: linear-gradient(135deg, #0f172a 0%, #16324f 52%, #0f766e 100%);
  color: #ffffff;
  border-radius: 20px;
  padding: 18px 24px;
  margin-bottom: 18px;
  box-shadow: 0 18px 40px rgba(15, 23, 42, 0.18);
}

.dashboard-header h1,
.dashboard-header h2,
.dashboard-header .editable-title,
.dashboard-header .dashboard-title,
.dashboard-header .dashboard-description {
  color: #ffffff;
}

.dashboard-grid .grid-content {
  gap: 14px;
}

.dashboard-component-chart-holder {
  background: rgba(255, 255, 255, 0.94);
  border: 1px solid #d7e3ef;
  border-radius: 18px;
  box-shadow: 0 14px 32px rgba(15, 23, 42, 0.07);
  padding: 8px;
  backdrop-filter: blur(4px);
}

.dashboard-component-chart-holder .chart-header {
  border-bottom: 1px solid #edf2f7;
  padding: 6px 8px 10px;
}

.dashboard-component-chart-holder .header-title,
.dashboard-component-chart-holder .header-title > span {
  color: #0f172a;
  font-weight: 700;
  letter-spacing: 0.01em;
}

.dashboard-component-chart-holder table th {
  background: #f8fafc;
}
"""


def get_dataset_column(dataset, column_name: str):
    """Return a Superset dataset column by name."""

    for column in dataset.columns:
        if column.column_name == column_name:
            return column
    raise KeyError(f"Missing dataset column: {column_name}")


def serialize_column(column) -> dict[str, object]:
    """Serialize a dataset column for Superset ad hoc metric payloads."""

    return {
        "advanced_data_type": getattr(column, "advanced_data_type", None),
        "certification_details": getattr(column, "certification_details", None),
        "certified_by": getattr(column, "certified_by", None),
        "column_name": column.column_name,
        "description": getattr(column, "description", None),
        "expression": getattr(column, "expression", None),
        "filterable": getattr(column, "filterable", True),
        "groupby": getattr(column, "groupby", True),
        "id": column.id,
        "is_certified": getattr(column, "is_certified", False),
        "is_dttm": getattr(column, "is_dttm", False),
        "python_date_format": getattr(column, "python_date_format", None),
        "type": getattr(column, "type", None),
        "type_generic": getattr(column, "type_generic", None),
        "verbose_name": getattr(column, "verbose_name", None),
        "warning_markdown": getattr(column, "warning_markdown", None),
    }


def build_simple_metric(dataset, column_name: str, aggregate: str, label: str | None = None) -> dict[str, object]:
    """Build a simple Superset metric payload from an actual dataset column."""

    column = get_dataset_column(dataset, column_name)
    metric_label = label or f"{aggregate}({column_name})"
    return {
        "expressionType": "SIMPLE",
        "column": serialize_column(column),
        "aggregate": aggregate,
        "sqlExpression": None,
        "datasourceWarning": False,
        "hasCustomLabel": label is not None,
        "isNew": False,
        "label": metric_label,
        "optionName": f"metric_{column_name}_{aggregate.lower()}",
    }


def build_sql_metric(label: str, sql_expression: str) -> dict[str, object]:
    """Build a SQL ad hoc metric for weighted KPIs and ratios."""

    option_name = "".join(char.lower() if char.isalnum() else "_" for char in label).strip("_") or "metric_sql"
    return {
        "expressionType": "SQL",
        "column": None,
        "aggregate": None,
        "sqlExpression": sql_expression,
        "datasourceWarning": False,
        "hasCustomLabel": True,
        "isNew": False,
        "label": label,
        "optionName": f"metric_{option_name}",
    }


def build_metric(dataset, metric_spec: dict[str, object]) -> dict[str, object]:
    """Build either a simple column aggregate or SQL metric from a spec."""

    if "sqlExpression" in metric_spec:
        return build_sql_metric(str(metric_spec["label"]), str(metric_spec["sqlExpression"]))
    return build_simple_metric(
        dataset,
        str(metric_spec["column"]),
        str(metric_spec["aggregate"]),
        str(metric_spec.get("label")) if metric_spec.get("label") is not None else None,
    )


def build_time_filter(column_name: str) -> list[dict[str, object]]:
    """Return a no-op temporal filter so time-series charts keep the time picker."""

    return [
        {
            "clause": "WHERE",
            "subject": column_name,
            "operator": "TEMPORAL_RANGE",
            "comparator": "No filter",
            "expressionType": "SIMPLE",
        }
    ]


def build_chart_form_data(dataset, spec: dict[str, object]) -> dict[str, object]:
    """Return a stable Superset chart payload for a spec."""

    datasource = f"{dataset.id}__table"
    viz_type = str(spec["viz_type"])
    form_data: dict[str, object] = {
        "datasource": datasource,
        "viz_type": viz_type,
        "extra_form_data": {},
    }

    if viz_type == "big_number_total":
        metric_spec = spec["metric"]
        form_data.update(
            {
                "metric": build_metric(dataset, metric_spec),
                "adhoc_filters": [],
                "header_font_size": 0.38,
                "subheader_font_size": 0.14,
                "y_axis_format": str(spec.get("y_axis_format", "SMART_NUMBER")),
                "time_format": "smart_date",
            }
        )
        return form_data

    if viz_type == "mixed_timeseries":
        metric_spec = spec["metric"]
        metric_b_spec = spec["metric_b"]
        form_data.update(
            {
                "x_axis": spec["x_axis"],
                "time_grain_sqla": spec["time_grain_sqla"],
                "metrics": [
                    build_metric(dataset, metric_spec),
                ],
                "groupby": list(spec.get("groupby", [])),
                "adhoc_filters": build_time_filter(str(spec["x_axis"])),
                "order_desc": True,
                "row_limit": int(spec["row_limit"]),
                "truncate_metric": True,
                "show_empty_columns": True,
                "comparison_type": "values",
                "annotation_layers": [],
                "forecastPeriods": 10,
                "forecastInterval": 0.8,
                "x_axis_title_margin": 15,
                "y_axis_title_margin": 15,
                "y_axis_title_position": "Left",
                "sort_series_type": "sum",
                "color_scheme": "supersetColors",
                "seriesType": str(spec.get("series_type", "line")),
                "only_total": True,
                "opacity": 0.2,
                "markerSize": 6,
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "x_axis_time_format": "smart_date",
                "y_axis_format": str(spec.get("y_axis_format", "SMART_NUMBER")),
                "truncateXAxis": True,
                "rich_tooltip": True,
                "tooltipTimeFormat": "smart_date",
            }
        )
        form_data["metrics_b"] = [
            build_metric(dataset, metric_b_spec),
        ]
        form_data["groupby_b"] = list(spec.get("groupby_b", []))
        form_data["adhoc_filters_b"] = build_time_filter(str(spec["x_axis"]))
        form_data["order_desc_b"] = True
        form_data["row_limit_b"] = int(spec["row_limit"])
        form_data["truncate_metric_b"] = True
        form_data["comparison_type_b"] = "values"
        form_data["seriesTypeB"] = str(spec.get("series_type_b", "bar"))
        form_data["opacityB"] = 0.2
        form_data["markerSizeB"] = 6
        form_data["yAxisIndex"] = 1
        form_data["yAxisIndexB"] = 0
        form_data["y_axis_bounds"] = [None, None]
        form_data["y_axis_bounds_secondary"] = [None, None]
        form_data["y_axis_format_secondary"] = str(spec.get("y_axis_format_secondary", "SMART_NUMBER"))
        return form_data

    if viz_type == "echarts_timeseries_line":
        form_data.update(
            {
                "x_axis": spec["x_axis"],
                "time_grain_sqla": spec["time_grain_sqla"],
                "x_axis_sort_asc": True,
                "x_axis_sort_series": "name",
                "x_axis_sort_series_ascending": True,
                "metrics": [
                    build_metric(dataset, metric)
                    for metric in spec["metrics"]
                ],
                "groupby": list(spec.get("groupby", [])),
                "adhoc_filters": build_time_filter(str(spec["x_axis"])),
                "order_desc": True,
                "row_limit": int(spec["row_limit"]),
                "truncate_metric": True,
                "show_empty_columns": True,
                "comparison_type": "values",
                "annotation_layers": [],
                "forecastPeriods": 10,
                "forecastInterval": 0.8,
                "x_axis_title_margin": 15,
                "y_axis_title_margin": 15,
                "y_axis_title_position": "Left",
                "sort_series_type": "sum",
                "color_scheme": "supersetColors",
                "seriesType": "line",
                "only_total": True,
                "opacity": 0.2,
                "markerSize": 6,
                "show_legend": True,
                "legendType": "scroll",
                "legendOrientation": "top",
                "x_axis_time_format": "smart_date",
                "rich_tooltip": True,
                "tooltipTimeFormat": "smart_date",
                "y_axis_format": str(spec.get("y_axis_format", "SMART_NUMBER")),
                "truncateXAxis": True,
                "y_axis_bounds": [None, None],
            }
        )
        return form_data

    if viz_type == "dist_bar":
        form_data.update(
            {
                "groupby": list(spec["groupby"]),
                "metrics": [
                    build_metric(dataset, metric)
                    for metric in spec["metrics"]
                ],
                "columns": [],
                "row_limit": int(spec["row_limit"]),
                "order_bars": bool(spec.get("order_bars", True)),
                "show_bar_value": bool(spec.get("show_bar_value", False)),
                "show_controls": True,
                "show_legend": bool(spec.get("show_legend", True)),
                "bar_stacked": bool(spec.get("bar_stacked", False)),
                "contribution": False,
                "color_scheme": "supersetColors",
                "x_axis_label": spec.get("x_axis_label"),
                "x_ticks_layout": "flat",
                "y_axis_format": spec.get("y_axis_format", "SMART_NUMBER"),
                "time_range": "No filter",
                "url_params": {},
            }
        )
        return form_data

    if viz_type == "treemap_v2":
        metric_spec = spec["metric"]
        form_data.update(
            {
                "groupby": list(spec["groupby"]),
                "metric": build_metric(dataset, metric_spec),
                "row_limit": int(spec["row_limit"]),
                "color_scheme": "supersetColors",
                "show_labels": bool(spec.get("show_labels", True)),
                "show_upper_labels": bool(spec.get("show_upper_labels", True)),
                "label_type": str(spec.get("label_type", "key_value")),
                "number_format": "SMART_NUMBER",
                "date_format": "smart_date",
            }
        )
        return form_data

    if viz_type == "pie":
        metric_spec = spec["metric"]
        form_data.update(
            {
                "groupby": list(spec["groupby"]),
                "metric": build_metric(dataset, metric_spec),
                "row_limit": int(spec["row_limit"]),
                "sort_by_metric": bool(spec.get("sort_by_metric", True)),
                "color_scheme": "supersetColors",
                "show_labels_threshold": 5,
                "show_legend": bool(spec.get("show_legend", True)),
                "legendType": "scroll",
                "legendOrientation": "top",
                "label_type": "key",
                "number_format": "SMART_NUMBER",
                "date_format": "smart_date",
                "show_labels": bool(spec.get("show_labels", True)),
                "labels_outside": bool(spec.get("labels_outside", True)),
                "outerRadius": 70,
                "innerRadius": 30,
            }
        )
        return form_data

    if viz_type == "heatmap_v2":
        metric_spec = spec["metric"]
        form_data.update(
            {
                "x_axis": spec["x_axis"],
                "groupby": spec["groupby"],
                "metric": build_metric(dataset, metric_spec),
                "row_limit": int(spec["row_limit"]),
                "sort_x_axis": "alpha_asc",
                "sort_y_axis": "alpha_asc",
                "normalize_across": "heatmap",
                "legend_type": str(spec.get("legend_type", "continuous")),
                "linear_color_scheme": str(spec.get("linear_color_scheme", "superset_seq_1")),
                "xscale_interval": -1,
                "yscale_interval": -1,
                "left_margin": "auto",
                "bottom_margin": "auto",
                "value_bounds": [None, None],
                "y_axis_format": str(spec.get("y_axis_format", "SMART_NUMBER")),
                "x_axis_time_format": "smart_date",
                "show_legend": bool(spec.get("show_legend", True)),
                "show_percentage": bool(spec.get("show_percentage", True)),
                "show_values": bool(spec.get("show_values", True)),
            }
        )
        return form_data

    if viz_type == "pivot_table_v2":
        form_data.update(
            {
                "groupbyColumns": list(spec["groupbyColumns"]),
                "groupbyRows": list(spec["groupbyRows"]),
                "time_grain_sqla": "P1D",
                "temporal_columns_lookup": {},
                "metrics": [
                    build_metric(dataset, metric)
                    for metric in spec["metrics"]
                ],
                "metricsLayout": str(spec.get("metricsLayout", "COLUMNS")),
                "adhoc_filters": [],
                "row_limit": int(spec["row_limit"]),
                "order_desc": bool(spec.get("order_desc", True)),
                "aggregateFunction": str(spec.get("aggregateFunction", "Sum")),
                "valueFormat": str(spec.get("valueFormat", "SMART_NUMBER")),
                "date_format": "smart_date",
                "rowOrder": str(spec.get("rowOrder", "key_a_to_z")),
                "colOrder": str(spec.get("colOrder", "key_a_to_z")),
            }
        )
        return form_data

    raise ValueError(f"Unsupported viz_type: {viz_type}")


def build_dashboard_layout(layout_specs: list[dict[str, object]], charts_by_name: dict[str, object]) -> dict[str, object]:
    """Return a multi-row layout for a Superset dashboard."""

    layout: dict[str, object] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "children": [],
            "parents": ["ROOT_ID"],
            "meta": {},
        },
    }

    rows: dict[int, list[dict[str, object]]] = {}
    for entry in layout_specs:
        rows.setdefault(int(entry["row"]), []).append(entry)

    for row_number in sorted(rows):
        row_id = f"ROW_{row_number}"
        row_entries = rows[row_number]
        row_children: list[str] = []
        layout["GRID_ID"]["children"].append(row_id)
        layout[row_id] = {
            "id": row_id,
            "type": "ROW",
            "children": row_children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }
        for column_index, entry in enumerate(row_entries, start=1):
            chart = charts_by_name[str(entry["slice_name"])]
            chart_component_id = f"CHART_{row_number}_{column_index}"
            row_children.append(chart_component_id)
            layout[chart_component_id] = {
                "id": chart_component_id,
                "type": "CHART",
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {
                    "chartId": chart.id,
                    "height": int(entry["height"]),
                    "sliceName": chart.slice_name,
                    "uuid": str(chart.uuid),
                    "width": int(entry["width"]),
                },
            }
    return layout


def build_dashboard_metadata() -> dict[str, object]:
    """Return dashboard metadata without advanced features that the demo does not need."""

    return {
        "chart_configuration": {},
        "global_chart_configuration": {},
        "map_label_colors": {},
        "timed_refresh_immune_slices": [],
        "expanded_slices": {},
        "refresh_frequency": 0,
        "color_scheme": "",
        "label_colors": {},
        "shared_label_colors": [],
        "color_scheme_domain": [],
        "cross_filters_enabled": False,
        "native_filter_configuration": [],
        "default_filters": "{}",
    }


def ensure_dataset(db, sqla_table_cls, database, table_name: str):
    """Create the Superset dataset if missing and return it."""

    dataset = (
        db.session.query(sqla_table_cls)
        .filter_by(table_name=table_name, schema="demo", database_id=database.id)
        .one_or_none()
    )
    if dataset is None:
        dataset = sqla_table_cls(table_name=table_name, schema="demo", database=database)
        db.session.add(dataset)
        db.session.flush()
        dataset.fetch_metadata()
        db.session.flush()
    return dataset


def upsert_chart(db, slice_cls, dataset, spec: dict[str, object]):
    """Create or update a saved Superset chart for a gold dataset."""

    slice_name = str(spec["slice_name"])
    form_data = build_chart_form_data(dataset, spec)

    chart = db.session.query(slice_cls).filter_by(slice_name=slice_name).one_or_none()
    if chart is None:
        chart = slice_cls(
            slice_name=slice_name,
            datasource_type="table",
            datasource_id=dataset.id,
            viz_type=str(spec["viz_type"]),
            params=json.dumps(form_data),
        )
        db.session.add(chart)
        db.session.flush()
    else:
        chart.datasource_type = "table"
        chart.datasource_id = dataset.id
        chart.viz_type = str(spec["viz_type"])
        chart.params = json.dumps(form_data)

    chart.description = f"Gold table: demo.{spec['table_name']}"
    chart.query_context = None
    db.session.flush()
    return chart


def upsert_dashboard(db, dashboard_cls, charts_by_name: dict[str, object], spec: dict[str, object]) -> None:
    """Create or update a Superset dashboard from a layout spec."""

    dashboard = db.session.query(dashboard_cls).filter_by(slug=str(spec["slug"])).one_or_none()
    if dashboard is None:
        dashboard = dashboard_cls(
            dashboard_title=str(spec["title"]),
            slug=str(spec["slug"]),
            position_json="{}",
            json_metadata="{}",
            published=True,
        )
        db.session.add(dashboard)
        db.session.flush()

    charts = [charts_by_name[str(entry["slice_name"])] for entry in spec["charts"]]
    dashboard.dashboard_title = str(spec["title"])
    dashboard.description = str(spec["description"])
    dashboard.css = DASHBOARD_CSS
    dashboard.slices = charts
    dashboard.position_json = json.dumps(build_dashboard_layout(list(spec["charts"]), charts_by_name))
    dashboard.json_metadata = json.dumps(build_dashboard_metadata())
    dashboard.published = True


def main() -> None:
    """Create the Superset database, datasets, charts, and dashboards if missing."""

    trino_uri = os.getenv("SUPERSET_TRINO_SQLALCHEMY_URI", "trino://trino@trino:8080/iceberg/demo")

    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database
        from superset.models.dashboard import Dashboard
        from superset.models.slice import Slice

        database = db.session.query(Database).filter_by(database_name="lakehouse_trino").one_or_none()
        if database is None:
            database = Database(
                database_name="lakehouse_trino",
                sqlalchemy_uri=trino_uri,
                expose_in_sqllab=True,
                allow_ctas=False,
                allow_cvas=False,
                allow_dml=False,
                extra=json.dumps({"metadata_params": {}, "engine_params": {}, "schemas_allowed_for_file_upload": []}),
            )
            db.session.add(database)
            db.session.commit()
        elif database.sqlalchemy_uri != trino_uri:
            database.sqlalchemy_uri = trino_uri
            db.session.commit()

        required_objects = {str(spec["table_name"]) for spec in CHART_SPECS}
        available_objects: set[str] = set()
        with database.get_sqla_engine() as engine:
            inspector = inspect(engine)
            for object_name in sorted(required_objects):
                try:
                    inspector.get_columns(object_name, schema="demo")
                except SQLAlchemyError:
                    continue
                available_objects.add(object_name)
        if not required_objects.issubset(available_objects):
            current_app.logger.warning(
                "Superset bootstrap skipped dataset/chart creation because required Trino objects are missing: %s",
                ", ".join(sorted(required_objects - available_objects)),
            )
            return

        datasets = {
            table_name: ensure_dataset(db, SqlaTable, database, table_name)
            for table_name in sorted(required_objects)
        }
        charts_by_name = {
            str(spec["slice_name"]): upsert_chart(db, Slice, datasets[str(spec["table_name"])], spec)
            for spec in CHART_SPECS
        }

        for dashboard_spec in DASHBOARD_SPECS:
            upsert_dashboard(db, Dashboard, charts_by_name, dashboard_spec)

        db.session.commit()
        current_app.logger.info("Superset assets ready.")


if __name__ == "__main__":
    main()
