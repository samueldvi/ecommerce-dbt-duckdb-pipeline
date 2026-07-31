"""Portable, read-only data and fallback tests for the Streamlit demo."""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
from streamlit.testing.v1 import AppTest

from dashboard.config import (
    DATABASE_PATH_ENV,
    DBT_SCHEMA_ENV,
    DashboardConfigurationError,
    load_dashboard_config,
    resolve_database_path,
    resolve_dbt_schema,
)
from dashboard.data import (
    DashboardQueryError,
    DatabaseIncompleteError,
    DatabaseUnavailableError,
    database_fingerprint,
    load_dashboard_snapshot,
    open_read_only_database,
)
from dashboard.presentation import chart_records, format_currency, format_integer
from dashboard.queries import QUERY_BY_NAME

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def create_complete_database(
    path: Path,
    *,
    schema: str = "main",
    with_rows: bool = True,
) -> None:
    """Create the smallest database satisfying the dashboard read contract."""

    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    try:
        connection.execute("CREATE SCHEMA raw")
        connection.execute(
            """
            CREATE TABLE raw.ingestion_manifest (
                source_file VARCHAR,
                row_count BIGINT,
                ingested_at TIMESTAMPTZ
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE raw.order_lines (
                order_line_id VARCHAR,
                source_file VARCHAR
            )
            """
        )
        if schema != "main":
            connection.execute(f'CREATE SCHEMA "{schema}"')
        connection.execute(
            f"""
            CREATE TABLE "{schema}".dim_customers (
                customer_sk VARCHAR,
                customer_id VARCHAR,
                customer_name VARCHAR,
                country_code VARCHAR
            )
            """
        )
        connection.execute(
            f"""
            CREATE TABLE "{schema}".dim_products (
                product_sk VARCHAR,
                category VARCHAR
            )
            """
        )
        connection.execute(
            f"""
            CREATE TABLE "{schema}".dim_date (
                date_sk INTEGER,
                date_day DATE,
                year_number INTEGER,
                iso_week_number INTEGER
            )
            """
        )
        connection.execute(
            f"""
            CREATE TABLE "{schema}".fct_order_lines (
                order_line_id VARCHAR,
                order_id VARCHAR,
                customer_sk VARCHAR,
                product_sk VARCHAR,
                date_sk INTEGER,
                quantity INTEGER,
                unit_price DECIMAL(12, 2),
                order_status VARCHAR,
                sales_channel VARCHAR,
                is_economically_valid BOOLEAN,
                revenue DECIMAL(18, 2)
            )
            """
        )
        if not with_rows:
            return
        connection.execute(
            f"""
            INSERT INTO "{schema}".dim_customers VALUES
                ('customer|1', 'CUS_1', 'Synthetic Customer', 'IT')
            """
        )
        connection.execute(
            f"""
            INSERT INTO "{schema}".dim_products VALUES
                ('product|1', 'Hardware')
            """
        )
        connection.execute(
            f"""
            INSERT INTO "{schema}".dim_date VALUES
                (20250101, DATE '2025-01-01', 2025, 1)
            """
        )
        connection.execute(
            f"""
            INSERT INTO "{schema}".fct_order_lines VALUES
                (
                    'OL_1', 'ORD_1', 'customer|1', 'product|1', 20250101,
                    2, 10.00, 'completed', 'web', true, 20.00
                ),
                (
                    'OL_2', 'ORD_2', 'customer|1', 'product|1', 20250101,
                    1, null, 'cancelled', null, false, null
                )
            """
        )
        connection.execute(
            """
            INSERT INTO raw.order_lines VALUES
                ('OL_1', 'run_1/orders.csv'),
                ('OL_2', 'run_1/orders.csv')
            """
        )
        connection.execute(
            """
            INSERT INTO raw.ingestion_manifest VALUES
                (
                    'run_1/orders.csv',
                    2,
                    TIMESTAMPTZ '2025-01-01 12:00:00+00'
                )
            """
        )
    finally:
        connection.close()


class StreamlitDemoTests(unittest.TestCase):
    """Verify dashboard behavior without coupling to visual layout details."""

    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = Path(self.temporary_directory.name)
        self.database_path = self.root / "warehouse" / "demo.duckdb"

    def config(self, *, schema: str = "main"):
        return load_dashboard_config(
            project_root=self.root,
            environ={
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: schema,
            },
        )

    def test_default_path_is_project_relative_not_cwd_relative(self) -> None:
        other_directory = self.root / "another working directory"
        other_directory.mkdir()
        previous_directory = Path.cwd()
        try:
            os.chdir(other_directory)
            resolved = resolve_database_path(
                project_root=REPOSITORY_ROOT, environ={}
            )
        finally:
            os.chdir(previous_directory)
        self.assertEqual(
            resolved,
            REPOSITORY_ROOT / "data/warehouse/ecommerce.duckdb",
        )

    def test_environment_overrides_path_and_schema_safely(self) -> None:
        relative = resolve_database_path(
            project_root=self.root,
            environ={DATABASE_PATH_ENV: "custom/demo.duckdb"},
        )
        self.assertEqual(
            relative,
            (self.root / "custom/demo.duckdb").resolve(strict=False),
        )
        self.assertEqual(
            resolve_dbt_schema(environ={DBT_SCHEMA_ENV: "analytics_dev"}),
            "analytics_dev",
        )
        with self.assertRaises(DashboardConfigurationError):
            resolve_dbt_schema(environ={DBT_SCHEMA_ENV: "main; drop schema raw"})

    def test_database_fingerprint_changes_with_database_file(self) -> None:
        create_complete_database(self.database_path, with_rows=False)
        before = database_fingerprint(self.database_path)
        connection = duckdb.connect(str(self.database_path))
        try:
            connection.execute(
                "INSERT INTO raw.order_lines VALUES ('OL_NEW', 'run_1/orders.csv')"
            )
        finally:
            connection.close()
        after = database_fingerprint(self.database_path)
        self.assertEqual(before.path, after.path)
        self.assertNotEqual(
            (before.modified_at_ns, before.size_bytes),
            (after.modified_at_ns, after.size_bytes),
        )

    def test_missing_and_corrupt_databases_raise_controlled_errors(self) -> None:
        with self.assertRaises(DatabaseUnavailableError):
            database_fingerprint(self.database_path)
        self.database_path.parent.mkdir(parents=True)
        self.database_path.write_text("not a DuckDB database", encoding="utf-8")
        with self.assertRaises(DatabaseUnavailableError):
            load_dashboard_snapshot(self.config())

    def test_incomplete_database_reports_required_resources(self) -> None:
        self.database_path.parent.mkdir(parents=True)
        connection = duckdb.connect(str(self.database_path))
        connection.close()
        with self.assertRaises(DatabaseIncompleteError) as context:
            load_dashboard_snapshot(self.config())
        self.assertIn("raw.order_lines", context.exception.missing_resources)
        self.assertIn("main.fct_order_lines", context.exception.missing_resources)

    def test_complete_database_loads_exact_aggregate_results(self) -> None:
        create_complete_database(self.database_path)
        snapshot = load_dashboard_snapshot(self.config())
        kpis = snapshot.kpis.records()[0]
        self.assertEqual(kpis["total_revenue"], Decimal("20.00"))
        self.assertEqual(kpis["distinct_orders"], 2)
        self.assertEqual(kpis["revenue_orders"], 1)
        self.assertEqual(kpis["order_lines"], 2)
        self.assertEqual(kpis["known_customers"], 1)
        self.assertEqual(kpis["average_revenue_order_value"], Decimal("20.00"))
        self.assertEqual(len(snapshot.revenue_by_category_month.rows), 1)
        self.assertEqual(len(snapshot.top_customers.rows), 1)
        self.assertEqual(len(snapshot.orders_by_region_week.rows), 1)
        self.assertEqual(len(snapshot.category_ranking.rows), 1)
        self.assertEqual(
            snapshot.incremental_loads.records()[0]["cumulative_keys"], 2
        )
        quality = snapshot.data_quality.records()[0]
        self.assertEqual(quality["duplicate_current_keys"], 0)
        self.assertEqual(quality["cancelled_lines"], 1)
        self.assertEqual(quality["economically_invalid_lines"], 1)

    def test_empty_complete_database_returns_safe_empty_results(self) -> None:
        create_complete_database(self.database_path, with_rows=False)
        snapshot = load_dashboard_snapshot(self.config())
        kpis = snapshot.kpis.records()[0]
        self.assertEqual(kpis["total_revenue"], Decimal("0.00"))
        self.assertEqual(kpis["order_lines"], 0)
        self.assertEqual(snapshot.revenue_by_category_month.rows, ())
        self.assertEqual(snapshot.top_customers.rows, ())
        self.assertEqual(snapshot.orders_by_region_week.rows, ())
        self.assertEqual(snapshot.category_ranking.rows, ())
        self.assertEqual(snapshot.incremental_loads.rows, ())

    def test_database_remains_byte_identical_after_dashboard_reads(self) -> None:
        create_complete_database(self.database_path)
        checksum_before = hashlib.sha256(self.database_path.read_bytes()).digest()
        load_dashboard_snapshot(self.config())
        checksum_after = hashlib.sha256(self.database_path.read_bytes()).digest()
        self.assertEqual(checksum_after, checksum_before)
        with open_read_only_database(self.database_path) as connection:
            with self.assertRaises(duckdb.Error):
                connection.execute("CREATE TABLE forbidden_write (id INTEGER)")

    def test_connection_closes_when_a_query_fails(self) -> None:
        self.database_path.parent.mkdir(parents=True)
        self.database_path.touch()
        connection = Mock()
        connection.execute.side_effect = RuntimeError("injected query failure")
        with patch("dashboard.data.duckdb.connect", return_value=connection):
            with self.assertRaises(DashboardQueryError):
                load_dashboard_snapshot(self.config())
        connection.close.assert_called_once_with()

    def test_queries_are_bounded_selects_without_jinja(self) -> None:
        self.assertEqual(
            set(QUERY_BY_NAME),
            {
                "kpis",
                "revenue_by_category_month",
                "top_customers",
                "orders_by_region_week",
                "category_ranking",
                "incremental_loads",
                "data_quality",
            },
        )
        for query in QUERY_BY_NAME.values():
            normalized = query.lower()
            self.assertNotIn("{{", query)
            self.assertNotIn("insert ", normalized)
            self.assertNotIn("update ", normalized)
            self.assertNotIn("delete ", normalized)
            self.assertNotIn("create ", normalized)

    def test_presentation_helpers_do_not_change_exact_query_values(self) -> None:
        records = [{"amount": Decimal("1234.50"), "count": 12}]
        chart = chart_records(records)
        self.assertEqual(records[0]["amount"], Decimal("1234.50"))
        self.assertEqual(chart, [{"amount": 1234.5, "count": 12}])
        self.assertEqual(format_currency(Decimal("1234.50")), "€ 1.234,50")
        self.assertEqual(format_integer(5010), "5.010")

    def test_app_shows_controlled_bootstrap_for_missing_database(self) -> None:
        with patch.dict(
            os.environ,
            {
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: "main",
            },
        ):
            app = AppTest.from_file(
                str(REPOSITORY_ROOT / "streamlit_app.py")
            ).run(timeout=20)
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(
            [element.value for element in app.error],
            ["La demo non può leggere un database analitico completo."],
        )
        self.assertIn("dbt build --profiles-dir .", app.code[0].value)

    def test_app_renders_all_sections_for_complete_database(self) -> None:
        create_complete_database(self.database_path)
        with patch.dict(
            os.environ,
            {
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: "main",
            },
        ):
            app = AppTest.from_file(
                str(REPOSITORY_ROOT / "streamlit_app.py")
            ).run(timeout=20)
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(
            [element.value for element in app.title],
            ["E-commerce Incremental Analytics"],
        )
        self.assertEqual(
            [element.value for element in app.header],
            [
                "KPI principali",
                "Analisi Gold",
                "Caricamenti incrementali",
                "Data Quality",
                "Limiti dichiarati",
            ],
        )
        self.assertEqual(len(app.error), 0)


if __name__ == "__main__":
    unittest.main()
