"""Portable, read-only data and UI tests for the Streamlit demo."""

from __future__ import annotations

import hashlib
import os
import tempfile
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import duckdb
from streamlit.testing.v1 import AppTest

from dashboard.charts import revenue_trend_spec
from dashboard.config import (
    DATABASE_PATH_ENV,
    DBT_SCHEMA_ENV,
    DashboardConfigurationError,
    load_dashboard_config,
    resolve_database_path,
    resolve_dbt_schema,
)
from dashboard.data import (
    DashboardFilters,
    DashboardQueryError,
    DatabaseIncompleteError,
    DatabaseUnavailableError,
    database_fingerprint,
    load_dashboard_metadata,
    load_dashboard_snapshot,
    open_read_only_database,
)
from dashboard.i18n import (
    SUPPORTED_LANGUAGES,
    TRANSLATIONS,
    Translator,
    translation_keys,
)
from dashboard.presentation import (
    chart_records,
    format_currency,
    format_date,
    format_integer,
    format_period,
)
from dashboard.queries import FILTERED_QUERY_NAMES, QUERY_BY_NAME
from dashboard.theme import THEMES, contrast_ratio

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
                ('customer|1', 'CUS_1', 'Customer One', 'IT'),
                ('customer|2', 'CUS_2', 'Customer Two', 'FR'),
                ('customer|unknown', '__UNKNOWN__', 'Unknown', null)
            """
        )
        connection.execute(
            f"""
            INSERT INTO "{schema}".dim_products VALUES
                ('product|1', 'Hardware'),
                ('product|2', 'Books'),
                ('product|unknown', 'Unknown')
            """
        )
        connection.execute(
            f"""
            INSERT INTO "{schema}".dim_date VALUES
                (20250101, DATE '2025-01-01', 2025, 1),
                (20250102, DATE '2025-01-02', 2025, 1)
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
                ),
                (
                    'OL_3', 'ORD_3', 'customer|2', 'product|2', 20250102,
                    1, 15.00, 'completed', 'mobile', true, 15.00
                )
            """
        )
        connection.execute(
            """
            INSERT INTO raw.order_lines VALUES
                ('OL_1', 'run_1/orders.csv'),
                ('OL_2', 'run_1/orders.csv'),
                ('OL_3', 'run_2/orders.csv')
            """
        )
        connection.execute(
            """
            INSERT INTO raw.ingestion_manifest VALUES
                (
                    'run_1/orders.csv', 2,
                    TIMESTAMPTZ '2025-02-01 12:00:00+00'
                ),
                (
                    'run_2/orders.csv', 1,
                    TIMESTAMPTZ '2025-02-02 12:00:00+00'
                )
            """
        )
    finally:
        connection.close()


class StreamlitDemoTests(unittest.TestCase):
    """Verify dashboard behavior without coupling to private Streamlit DOM."""

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

    def run_app(self, *, schema: str = "main") -> AppTest:
        with patch.dict(
            os.environ,
            {
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: schema,
            },
        ):
            return AppTest.from_file(
                str(REPOSITORY_ROOT / "streamlit_app.py")
            ).run(timeout=20)

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
        self.assertEqual(resolve_dbt_schema(environ={}), "analytics")
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

    def test_metadata_initializes_real_period_options_and_update(self) -> None:
        create_complete_database(self.database_path)
        metadata = load_dashboard_metadata(self.config())
        self.assertEqual(metadata.period_start, date(2025, 1, 1))
        self.assertEqual(metadata.period_end, date(2025, 1, 2))
        self.assertEqual(metadata.categories, ("Books", "Hardware"))
        self.assertEqual(metadata.regions, ("FR", "IT", "Unknown"))
        self.assertEqual(metadata.last_ingested_at.day, 2)

    def test_default_analytics_schema_loads_and_wrong_schema_is_controlled(
        self,
    ) -> None:
        create_complete_database(self.database_path, schema="analytics")
        default_config = load_dashboard_config(
            project_root=self.root,
            environ={DATABASE_PATH_ENV: str(self.database_path)},
        )
        snapshot = load_dashboard_snapshot(default_config)

        self.assertEqual(default_config.dbt_schema, "analytics")
        self.assertEqual(snapshot.kpis.records()[0]["order_lines"], 3)
        self.assertEqual(len(snapshot.revenue_trend.rows), 2)
        self.assertEqual(len(snapshot.revenue_by_category_month.rows), 2)
        self.assertEqual(len(snapshot.top_customers.rows), 2)
        self.assertEqual(len(snapshot.orders_by_region_week.rows), 2)
        self.assertEqual(len(snapshot.category_ranking.rows), 2)

        with self.assertRaises(DatabaseIncompleteError) as context:
            load_dashboard_snapshot(self.config(schema="wrong_schema"))
        self.assertIn(
            "wrong_schema.fct_order_lines",
            context.exception.missing_resources,
        )

    def test_complete_database_loads_exact_unfiltered_aggregates(self) -> None:
        create_complete_database(self.database_path)
        snapshot = load_dashboard_snapshot(self.config())
        kpis = snapshot.kpis.records()[0]
        self.assertEqual(kpis["total_revenue"], Decimal("35.00"))
        self.assertEqual(kpis["distinct_orders"], 3)
        self.assertEqual(kpis["revenue_orders"], 2)
        self.assertEqual(kpis["order_lines"], 3)
        self.assertEqual(kpis["known_customers"], 2)
        self.assertEqual(kpis["average_revenue_order_value"], Decimal("17.50"))
        self.assertEqual(snapshot.region_summary.records()[0]["customer_region"], "FR")
        self.assertEqual(
            snapshot.incremental_loads.records()[-1]["cumulative_keys"], 3
        )
        quality = snapshot.data_quality.records()[0]
        self.assertEqual(quality["duplicate_current_keys"], 0)
        self.assertEqual(quality["cancelled_lines"], 1)
        self.assertEqual(quality["economically_invalid_lines"], 1)

    def test_filters_apply_to_every_business_aggregate(self) -> None:
        create_complete_database(self.database_path)
        filtered = load_dashboard_snapshot(
            self.config(),
            DashboardFilters(
                date(2025, 1, 1),
                date(2025, 1, 1),
                category="Hardware",
                region="IT",
            ),
        )
        kpis = filtered.kpis.records()[0]
        self.assertEqual(kpis["total_revenue"], Decimal("20.00"))
        self.assertEqual(kpis["distinct_orders"], 2)
        self.assertEqual(kpis["known_customers"], 1)
        self.assertEqual(len(filtered.revenue_trend.rows), 1)
        self.assertEqual(len(filtered.revenue_by_category_month.rows), 1)
        self.assertEqual(len(filtered.top_customers.rows), 1)
        self.assertEqual(len(filtered.orders_by_region_week.rows), 1)
        self.assertEqual(len(filtered.category_ranking.rows), 1)
        self.assertEqual(len(filtered.region_summary.rows), 1)

    def test_filter_values_are_parameters_and_cannot_change_sql(self) -> None:
        create_complete_database(self.database_path)
        malicious = "Hardware' OR 1=1 --"
        snapshot = load_dashboard_snapshot(
            self.config(),
            DashboardFilters(
                date(2025, 1, 1),
                date(2025, 1, 2),
                category=malicious,
            ),
        )
        self.assertEqual(snapshot.kpis.records()[0]["order_lines"], 0)
        with open_read_only_database(self.database_path) as connection:
            self.assertEqual(
                connection.execute(
                    "select count(*) from main.fct_order_lines"
                ).fetchone()[0],
                3,
            )

    def test_filter_validation_rejects_reversed_dates(self) -> None:
        with self.assertRaises(ValueError):
            DashboardFilters(date(2025, 1, 2), date(2025, 1, 1))

    def test_empty_complete_database_returns_safe_empty_results(self) -> None:
        create_complete_database(self.database_path, with_rows=False)
        snapshot = load_dashboard_snapshot(self.config())
        kpis = snapshot.kpis.records()[0]
        self.assertEqual(kpis["total_revenue"], Decimal("0.00"))
        self.assertEqual(kpis["order_lines"], 0)
        self.assertEqual(snapshot.revenue_trend.rows, ())
        self.assertEqual(snapshot.revenue_by_category_month.rows, ())
        self.assertEqual(snapshot.top_customers.rows, ())
        self.assertEqual(snapshot.orders_by_region_week.rows, ())
        self.assertEqual(snapshot.category_ranking.rows, ())
        self.assertEqual(snapshot.region_summary.rows, ())
        self.assertEqual(snapshot.incremental_loads.rows, ())

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

    def test_missing_corrupt_and_incomplete_databases_are_controlled(self) -> None:
        with self.assertRaises(DatabaseUnavailableError):
            database_fingerprint(self.database_path)
        self.database_path.parent.mkdir(parents=True)
        self.database_path.write_text("not a DuckDB database", encoding="utf-8")
        with self.assertRaises(DatabaseUnavailableError):
            load_dashboard_snapshot(self.config())

        self.database_path.unlink()
        connection = duckdb.connect(str(self.database_path))
        connection.close()
        with self.assertRaises(DatabaseIncompleteError) as context:
            load_dashboard_snapshot(self.config())
        self.assertIn("raw.order_lines", context.exception.missing_resources)
        self.assertIn("main.fct_order_lines", context.exception.missing_resources)

    def test_database_remains_byte_identical_after_dashboard_reads(self) -> None:
        create_complete_database(self.database_path)
        checksum_before = hashlib.sha256(self.database_path.read_bytes()).digest()
        load_dashboard_metadata(self.config())
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

    def test_queries_are_bounded_parameterized_selects(self) -> None:
        self.assertEqual(
            set(QUERY_BY_NAME),
            {
                "kpis",
                "revenue_trend",
                "revenue_by_category_month",
                "top_customers",
                "orders_by_region_week",
                "category_ranking",
                "region_summary",
                "incremental_loads",
                "data_quality",
            },
        )
        for name, query in QUERY_BY_NAME.items():
            normalized = query.lower()
            self.assertNotIn("{{", query)
            for forbidden in ("insert ", "update ", "delete ", "create ", "drop "):
                self.assertNotIn(forbidden, normalized)
            self.assertEqual(query.count("?"), 6 if name in FILTERED_QUERY_NAMES else 0)

    def test_translation_catalogs_are_complete_and_strict(self) -> None:
        expected = translation_keys("it")
        self.assertGreater(len(expected), 70)
        for language in SUPPORTED_LANGUAGES:
            self.assertEqual(translation_keys(language), expected)
            translator = Translator(language)
            self.assertEqual(translator("title"), TRANSLATIONS[language]["title"])
        with self.assertRaises(ValueError):
            Translator("fr")
        with self.assertRaises(KeyError):
            Translator("it")("missing_key")

    def test_localized_formatters_preserve_exact_values(self) -> None:
        records = [{"amount": Decimal("1234.50"), "count": 12}]
        chart = chart_records(records)
        self.assertEqual(records[0]["amount"], Decimal("1234.50"))
        self.assertEqual(chart, [{"amount": 1234.5, "count": 12}])
        self.assertEqual(format_currency(Decimal("1234.50"), "it"), "€ 1.234,50")
        self.assertEqual(format_currency(Decimal("1234.50"), "en"), "€1,234.50")
        self.assertEqual(format_integer(5010, "it"), "5.010")
        self.assertEqual(format_integer(5010, "en"), "5,010")
        self.assertEqual(format_date(date(2025, 1, 2), "it"), "02/01/2025")
        self.assertEqual(format_date(date(2025, 1, 2), "en"), "Jan 02, 2025")
        self.assertEqual(
            format_period(date(2025, 1, 1), date(2025, 1, 2), "it"),
            "01/01/2025 – 02/01/2025",
        )

    def test_themes_meet_wcag_aa_for_primary_text(self) -> None:
        for tokens in THEMES.values():
            for foreground, background in (
                (tokens.text, tokens.background),
                (tokens.muted, tokens.background),
                (tokens.text, tokens.surface),
                (tokens.primary, tokens.surface),
            ):
                self.assertGreaterEqual(contrast_ratio(foreground, background), 4.5)

    def test_chart_specs_are_accessible_and_theme_aware(self) -> None:
        tokens = THEMES["dark"]
        spec = revenue_trend_spec(
            [{"order_date": date(2025, 1, 1), "total_revenue": Decimal("10.00")}],
            description="Revenue trend",
            date_label="Date",
            revenue_label="Revenue",
            tokens=tokens,
        )
        self.assertEqual(spec["description"], "Revenue trend")
        self.assertEqual(spec["background"], tokens.surface)
        self.assertEqual(spec["mark"]["color"], tokens.primary)
        self.assertTrue(spec["encoding"]["y"]["scale"]["zero"])
        self.assertIn("tooltip", spec["encoding"])

    def test_app_shows_localized_controlled_bootstrap(self) -> None:
        app = self.run_app()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(
            [element.value for element in app.error],
            ["La dashboard non è disponibile."],
        )
        self.assertIn("dbt build --profiles-dir .", app.code[0].value)

        with patch.dict(
            os.environ,
            {
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: "main",
            },
        ):
            app.selectbox[0].select("en").run(timeout=20)
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(
            [element.value for element in app.error],
            ["The dashboard is unavailable."],
        )

    def test_app_renders_business_hierarchy_and_four_kpis(self) -> None:
        create_complete_database(self.database_path)
        app = self.run_app()
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(
            [element.value for element in app.header],
            [
                "Andamento delle vendite",
                "Clienti e territorio",
                "Stato dei dati",
            ],
        )
        html = "\n".join(element.proto.body for element in app.get("html"))
        self.assertIn("Panoramica vendite e-commerce", html)
        self.assertEqual(html.count("class='kpi-card'"), 4)
        self.assertNotIn("Righe correnti", html)
        self.assertEqual(len(app.get("vega_lite_chart")), 5)
        self.assertEqual(len(app.error), 0)

    def test_app_handles_empty_data_and_wrong_schema_without_tracebacks(self) -> None:
        create_complete_database(self.database_path, with_rows=False)
        empty_app = self.run_app()
        self.assertEqual(len(empty_app.exception), 0)
        self.assertIn(
            "La struttura dati è disponibile, ma non contiene record analitici.",
            [element.value for element in empty_app.info],
        )
        self.assertEqual(len(empty_app.get("vega_lite_chart")), 0)

        wrong_schema_app = self.run_app(schema="not_the_mart")
        self.assertEqual(len(wrong_schema_app.exception), 0)
        self.assertEqual(
            [element.value for element in wrong_schema_app.error],
            ["La dashboard non è disponibile."],
        )

    def test_language_theme_and_filter_state_survive_reruns(self) -> None:
        create_complete_database(self.database_path)
        app = self.run_app()
        with patch.dict(
            os.environ,
            {
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: "main",
            },
        ):
            app.selectbox[0].select("en").run(timeout=20)
            app.selectbox[1].select("dark").run(timeout=20)
            app.selectbox[2].select("Hardware").run(timeout=20)
        self.assertEqual(len(app.exception), 0)
        self.assertEqual(app.selectbox[0].value, "en")
        self.assertEqual(app.selectbox[1].value, "dark")
        self.assertEqual(app.selectbox[2].value, "Hardware")
        self.assertEqual(
            [element.value for element in app.header],
            ["Sales performance", "Customers and regions", "Data status"],
        )
        html = "\n".join(element.proto.body for element in app.get("html"))
        self.assertIn("€20.00", html)
        self.assertIn("background: #0F172A", html)

        with patch.dict(
            os.environ,
            {
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: "main",
            },
        ):
            app.button[0].click().run(timeout=20)
        self.assertEqual(app.selectbox[2].value, "__all__")
        self.assertEqual(app.selectbox[3].value, "__all__")

        with patch.dict(
            os.environ,
            {
                DATABASE_PATH_ENV: str(self.database_path),
                DBT_SCHEMA_ENV: "main",
            },
        ):
            app.selectbox[2].select("Hardware").run(timeout=20)
            app.selectbox[0].select("it").run(timeout=20)
        self.assertEqual(app.selectbox[0].value, "it")
        self.assertEqual(app.selectbox[1].value, "dark")
        self.assertEqual(app.selectbox[2].value, "Hardware")


if __name__ == "__main__":
    unittest.main()
