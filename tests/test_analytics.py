"""Integration tests for the four required analytical queries."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
import unittest
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPOSITORY_ROOT / "scripts"
ANALYSES_ROOT = REPOSITORY_ROOT / "analyses"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from raw_ingestion import RawIngestor  # noqa: E402
from synthetic_data import generate_dataset  # noqa: E402

ANALYSIS_COLUMNS = {
    "revenue_by_category_month": (
        "revenue_month",
        "category",
        "total_revenue",
    ),
    "top_10_customers_by_spend": (
        "customer_id",
        "customer_name",
        "total_spend",
    ),
    "completed_orders_by_region_week": (
        "order_year",
        "iso_week_number",
        "customer_region",
        "completed_order_count",
    ),
    "monthly_category_revenue_ranking": (
        "revenue_month",
        "category",
        "total_revenue",
        "revenue_position",
    ),
}


class AnalyticsTests(unittest.TestCase):
    """Build the complete mart and verify every analysis independently."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary_directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary_directory.cleanup)
        cls.root = Path(cls.temporary_directory.name)
        cls.input_root = cls.root / "generated"
        cls.database_path = cls.root / "warehouse" / "ecommerce.duckdb"
        cls.profiles_dir = cls.root / "profiles"
        cls.target_path = cls.root / "target"
        cls.log_path = cls.root / "logs"
        cls.profiles_dir.mkdir(parents=True)

        generate_dataset(cls.input_root, seed=20250730)
        RawIngestor(
            cls.database_path,
            input_root=cls.input_root,
        ).ingest_dataset()
        (cls.profiles_dir / "profiles.yml").write_text(
            yaml.safe_dump(
                {
                    "ecommerce_analytics": {
                        "target": "dev",
                        "outputs": {
                            "dev": {
                                "type": "duckdb",
                                "path": str(cls.database_path),
                                "schema": "analytics",
                                "threads": 4,
                            }
                        },
                    }
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )

        executable_candidate = Path(sys.executable).with_name("dbt")
        cls.dbt_executable = (
            str(executable_candidate)
            if executable_candidate.is_file()
            else shutil.which("dbt")
        )
        if cls.dbt_executable is None:
            raise RuntimeError("dbt executable is unavailable for analytics tests.")

        cls._run_dbt("build")
        cls._run_dbt("compile", "--select", "path:analyses")
        cls.results = {
            name: cls._execute_analysis(name) for name in ANALYSIS_COLUMNS
        }

    @classmethod
    def _run_dbt(cls, command: str, *arguments: str) -> None:
        environment = os.environ.copy()
        environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        result = subprocess.run(
            [
                cls.dbt_executable,
                command,
                "--project-dir",
                str(REPOSITORY_ROOT),
                "--profiles-dir",
                str(cls.profiles_dir),
                "--target-path",
                str(cls.target_path),
                "--log-path",
                str(cls.log_path),
                *arguments,
            ],
            cwd=REPOSITORY_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
        if result.returncode != 0:
            raise AssertionError(
                f"dbt {command} failed:\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )

    @classmethod
    def _execute_analysis(
        cls, name: str
    ) -> tuple[tuple[str, ...], tuple[tuple[object, ...], ...]]:
        compiled_path = (
            cls.target_path
            / "compiled"
            / "ecommerce_analytics"
            / "analyses"
            / f"{name}.sql"
        )
        sql = compiled_path.read_text(encoding="utf-8")
        connection = duckdb.connect(str(cls.database_path), read_only=True)
        try:
            cursor = connection.execute(sql)
            columns = tuple(column[0] for column in cursor.description)
            rows = tuple(cursor.fetchall())
        finally:
            connection.close()
        return columns, rows

    @classmethod
    def _mart_snapshot(cls) -> dict[str, tuple[tuple[object, ...], ...]]:
        connection = duckdb.connect(str(cls.database_path), read_only=True)
        try:
            return {
                "facts": tuple(
                    connection.execute(
                        """
                        SELECT
                            order_line_id,
                            order_id,
                            customer_sk,
                            product_sk,
                            date_sk,
                            revenue,
                            source_file
                        FROM analytics.fct_order_lines
                        ORDER BY order_line_id
                        """
                    ).fetchall()
                ),
                "customers": tuple(
                    connection.execute(
                        """
                        SELECT
                            customer_sk,
                            customer_id,
                            customer_name,
                            country_code
                        FROM analytics.dim_customers
                        """
                    ).fetchall()
                ),
                "products": tuple(
                    connection.execute(
                        """
                        SELECT product_sk, category
                        FROM analytics.dim_products
                        """
                    ).fetchall()
                ),
                "dates": tuple(
                    connection.execute(
                        """
                        SELECT date_sk, date_day, year_number, iso_week_number
                        FROM analytics.dim_date
                        """
                    ).fetchall()
                ),
            }
        finally:
            connection.close()

    def test_analysis_sources_are_thin_mart_consumers(self) -> None:
        self.assertEqual(
            {path.stem for path in ANALYSES_ROOT.glob("*.sql")},
            set(ANALYSIS_COLUMNS),
        )
        forbidden_business_columns = {
            "is_economically_valid",
            "order_status",
            "quantity",
            "unit_price",
        }
        allowed_refs = {
            "fct_order_lines",
            "dim_customers",
            "dim_products",
            "dim_date",
        }
        for name in ANALYSIS_COLUMNS:
            with self.subTest(analysis=name):
                source = (ANALYSES_ROOT / f"{name}.sql").read_text(
                    encoding="utf-8"
                )
                self.assertIn("facts.revenue", source)
                self.assertTrue(
                    forbidden_business_columns.isdisjoint(
                        re.findall(r"\b[a-z_]+\b", source.lower())
                    )
                )
                refs = set(re.findall(r"ref\('([^']+)'\)", source))
                self.assertIn("fct_order_lines", refs)
                self.assertLessEqual(refs, allowed_refs)

        ranking_source = (
            ANALYSES_ROOT / "monthly_category_revenue_ranking.sql"
        ).read_text(encoding="utf-8")
        self.assertRegex(
            ranking_source.lower(),
            r"dense_rank\(\)\s+over\s*\(\s*partition by revenue_month\s+"
            r"order by total_revenue desc\s*\)",
        )

    def test_compiled_queries_have_stable_columns_and_results(self) -> None:
        for name, expected_columns in ANALYSIS_COLUMNS.items():
            with self.subTest(analysis=name):
                columns, rows = self.results[name]
                self.assertEqual(columns, expected_columns)
                self.assertTrue(rows)
                self.assertEqual(self._execute_analysis(name), (columns, rows))

    def test_revenue_by_category_month_matches_python_oracle(self) -> None:
        snapshot = self._mart_snapshot()
        products = {row[0]: row[1] for row in snapshot["products"]}
        dates = {row[0]: row[1] for row in snapshot["dates"]}
        expected_totals: defaultdict[tuple[date, str], Decimal] = defaultdict(
            Decimal
        )
        contributing_source_files: set[str] = set()
        for (
            _order_line_id,
            _order_id,
            _customer_sk,
            product_sk,
            date_sk,
            revenue,
            source_file,
        ) in snapshot["facts"]:
            if revenue is None:
                continue
            order_date = dates[date_sk]
            month = order_date.replace(day=1)
            expected_totals[(month, products[product_sk])] += revenue
            contributing_source_files.add(source_file)

        expected = tuple(
            (month, category, total)
            for (month, category), total in sorted(expected_totals.items())
        )
        self.assertEqual(
            self.results["revenue_by_category_month"][1], expected
        )
        self.assertIn("Unknown", {row[1] for row in expected})
        self.assertTrue(
            {
                "run_1/orders.csv",
                "run_2/orders.csv",
                "run_3/orders.csv",
                "run_3/late_arriving/orders.csv",
            }.issubset(contributing_source_files)
        )

    def test_top_customers_and_completed_orders_match_python_oracles(self) -> None:
        snapshot = self._mart_snapshot()
        customers = {
            row[0]: {
                "customer_id": row[1],
                "customer_name": row[2],
                "region": row[3] or "Unknown",
            }
            for row in snapshot["customers"]
        }
        dates = {
            row[0]: {
                "year": row[2],
                "week": row[3],
            }
            for row in snapshot["dates"]
        }
        spend_by_customer: defaultdict[str, Decimal] = defaultdict(Decimal)
        distinct_orders: set[tuple[int, int, str, str]] = set()
        for (
            _order_line_id,
            order_id,
            customer_sk,
            _product_sk,
            date_sk,
            revenue,
            _source_file,
        ) in snapshot["facts"]:
            if revenue is None:
                continue
            spend_by_customer[customer_sk] += revenue
            distinct_orders.add(
                (
                    dates[date_sk]["year"],
                    dates[date_sk]["week"],
                    customers[customer_sk]["region"],
                    order_id,
                )
            )

        expected_top_customers = tuple(
            (
                customers[customer_sk]["customer_id"],
                customers[customer_sk]["customer_name"],
                total_spend,
            )
            for customer_sk, total_spend in sorted(
                spend_by_customer.items(),
                key=lambda item: (
                    -item[1],
                    customers[item[0]]["customer_id"],
                ),
            )[:10]
        )
        self.assertEqual(
            self.results["top_10_customers_by_spend"][1],
            expected_top_customers,
        )

        counts: defaultdict[tuple[int, int, str], int] = defaultdict(int)
        for year, week, region, _order_id in distinct_orders:
            counts[(year, week, region)] += 1
        expected_orders = tuple(
            (year, week, region, count)
            for (year, week, region), count in sorted(counts.items())
        )
        self.assertEqual(
            self.results["completed_orders_by_region_week"][1],
            expected_orders,
        )
        self.assertIn("Unknown", {row[2] for row in expected_orders})

    def test_monthly_category_ranking_has_dense_deterministic_positions(
        self,
    ) -> None:
        revenue_rows = self.results["revenue_by_category_month"][1]
        totals_by_month: defaultdict[date, set[Decimal]] = defaultdict(set)
        for month, _category, total_revenue in revenue_rows:
            totals_by_month[month].add(total_revenue)
        positions = {
            month: {
                total: position
                for position, total in enumerate(
                    sorted(totals, reverse=True), start=1
                )
            }
            for month, totals in totals_by_month.items()
        }
        expected = tuple(
            sorted(
                [
                    (
                        month,
                        category,
                        total_revenue,
                        positions[month][total_revenue],
                    )
                    for month, category, total_revenue in revenue_rows
                ],
                key=lambda row: (row[0], row[3], row[1]),
            )
        )
        self.assertEqual(
            self.results["monthly_category_revenue_ranking"][1], expected
        )
        self.assertEqual(
            {(row[0], row[1]) for row in expected},
            {(row[0], row[1]) for row in revenue_rows},
        )


if __name__ == "__main__":
    unittest.main()
