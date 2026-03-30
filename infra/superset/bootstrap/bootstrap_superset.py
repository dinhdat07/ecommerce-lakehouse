"""Bootstrap Superset assets for the laptop stack.

This script creates a Trino database connection, datasets for the Gold Iceberg
tables, a simple saved chart, and a dashboard so the user can open Superset and
see a working visualization from the sample data.
"""

from __future__ import annotations

import json

from flask import current_app
from sqlalchemy import inspect


def build_table_form_data(dataset_id: int) -> dict[str, object]:
    """Return a stable Superset table chart payload for the revenue dataset."""

    return {
        "datasource": f"{dataset_id}__table",
        "viz_type": "table",
        "slice_id": None,
        "query_mode": "raw",
        "all_columns": [
            "event_date",
            "category_code",
            "purchase_count",
            "purchase_revenue",
            "unique_buyers",
        ],
        "order_by_cols": ['["purchase_revenue", false]'],
        "order_desc": True,
        "row_limit": 1000,
        "server_pagination": False,
        "include_search": True,
        "adhoc_filters": [],
    }


def build_query_context(dataset_id: int, chart_id: int, form_data: dict[str, object]) -> dict[str, object]:
    """Build the query context expected by Superset chart APIs."""

    return {
        "datasource": {"id": dataset_id, "type": "table"},
        "force": False,
        "queries": [
            {
                "filters": [],
                "extras": {"having": "", "where": ""},
                "applied_time_extras": {},
                "columns": form_data["all_columns"],
                "metrics": [],
                "orderby": [["purchase_revenue", False]],
                "annotation_layers": [],
                "row_limit": form_data["row_limit"],
                "series_limit": 0,
                "order_desc": True,
                "url_params": {},
                "custom_params": {},
                "custom_form_data": {},
                "post_processing": [],
                "time_offsets": [],
            }
        ],
        "form_data": {
            **form_data,
            "slice_id": chart_id,
            "force": False,
            "result_format": "json",
            "result_type": "full",
            "include_time": False,
        },
        "result_format": "json",
        "result_type": "full",
    }


def build_dashboard_layout(chart_id: int, chart_uuid: str, slice_name: str) -> dict[str, object]:
    """Return a minimal but complete dashboard layout for a single chart."""

    return {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"id": "ROOT_ID", "type": "ROOT", "children": ["GRID_ID"]},
        "GRID_ID": {
            "id": "GRID_ID",
            "type": "GRID",
            "children": ["ROW_ID"],
            "parents": ["ROOT_ID"],
            "meta": {},
        },
        "ROW_ID": {
            "id": "ROW_ID",
            "type": "ROW",
            "children": ["CHART_ID"],
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        },
        "CHART_ID": {
            "id": "CHART_ID",
            "type": "CHART",
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", "ROW_ID"],
            "meta": {
                "chartId": chart_id,
                "height": 50,
                "sliceName": slice_name,
                "uuid": chart_uuid,
                "width": 12,
            },
        },
    }


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


def main() -> None:
    """Create the Superset database, datasets, chart, and dashboard if missing."""

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
                sqlalchemy_uri="trino://trino@trino:8080/iceberg/demo",
                expose_in_sqllab=True,
                allow_ctas=False,
                allow_cvas=False,
                allow_dml=False,
                extra=json.dumps({"metadata_params": {}, "engine_params": {}, "schemas_allowed_for_file_upload": []}),
            )
            db.session.add(database)
            db.session.commit()

        with database.get_sqla_engine() as engine:
            inspector = inspect(engine)
            available_objects = set(inspector.get_table_names(schema="demo")) | set(
                inspector.get_view_names(schema="demo")
            )
        required_objects = {"gold_revenue_by_category", "gold_conversion_funnel"}
        if not required_objects.issubset(available_objects):
            current_app.logger.warning(
                "Superset bootstrap skipped dataset/chart creation because required Trino objects are missing: %s",
                ", ".join(sorted(required_objects - available_objects)),
            )
            return

        for table_name in ["gold_revenue_by_category", "gold_conversion_funnel"]:
            dataset = (
                db.session.query(SqlaTable)
                .filter_by(table_name=table_name, schema="demo", database_id=database.id)
                .one_or_none()
            )
            if dataset is None:
                dataset = SqlaTable(table_name=table_name, schema="demo", database=database)
                db.session.add(dataset)
                db.session.commit()
                dataset.fetch_metadata()
                db.session.commit()

        revenue_dataset = (
            db.session.query(SqlaTable)
            .filter_by(table_name="gold_revenue_by_category", schema="demo", database_id=database.id)
            .one()
        )

        chart = db.session.query(Slice).filter_by(slice_name="Revenue by Category").one_or_none()
        form_data = build_table_form_data(revenue_dataset.id)
        if chart is None:
            chart = Slice(
                slice_name="Revenue by Category",
                datasource_type="table",
                datasource_id=revenue_dataset.id,
                viz_type="table",
                params=json.dumps(form_data),
            )
            db.session.add(chart)
            db.session.commit()
        else:
            chart.datasource_type = "table"
            chart.datasource_id = revenue_dataset.id
            chart.viz_type = "table"
            chart.params = json.dumps(form_data)

        chart.query_context = json.dumps(build_query_context(revenue_dataset.id, chart.id, form_data))
        db.session.commit()

        dashboard = db.session.query(Dashboard).filter_by(slug="lakehouse-sample-dashboard").one_or_none()
        if dashboard is None:
            dashboard = Dashboard(
                dashboard_title="Lakehouse Sample Dashboard",
                slug="lakehouse-sample-dashboard",
                position_json="{}",
                json_metadata="{}",
                published=True,
            )
            db.session.add(dashboard)

        if chart not in dashboard.slices:
            dashboard.slices.append(chart)

        dashboard.position_json = json.dumps(build_dashboard_layout(chart.id, str(chart.uuid), chart.slice_name))
        dashboard.json_metadata = json.dumps(build_dashboard_metadata())
        dashboard.published = True
        db.session.commit()

        current_app.logger.info("Superset assets ready.")


if __name__ == "__main__":
    main()
