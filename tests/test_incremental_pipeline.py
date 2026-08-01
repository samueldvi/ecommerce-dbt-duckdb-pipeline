"""End-to-end tests for the dbt incremental order-line pipeline."""

from __future__ import annotations

import csv
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPOSITORY_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from raw_ingestion import RawIngestor  # noqa: E402
from synthetic_data import generate_dataset  # noqa: E402

CONTRACT_METRICS = yaml.safe_load(
    (REPOSITORY_ROOT / "docs/expected_run_metrics.yml").read_text(
        encoding="utf-8"
    )
)
FINAL_KEY_COUNT = CONTRACT_METRICS["order_lines"]["final"][
    "distinct_keys"
]["exact"]
LATE_METRICS = CONTRACT_METRICS["order_lines"]["runs"]["run_3"][
    "late_arriving"
]


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


class IncrementalPipelineTests(unittest.TestCase):
    """Exercise each ordinary window, the late batch, and every replay."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.temporary_directory = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary_directory.cleanup)
        cls.root = Path(cls.temporary_directory.name)
        cls.input_root = cls.root / "generated"
        cls.database_path = cls.root / "warehouse" / "incremental.duckdb"
        cls.profiles_dir = cls.root / "profiles"
        cls.target_path = cls.root / "target"
        cls.log_path = cls.root / "logs"
        cls.profiles_dir.mkdir(parents=True)
        generate_dataset(cls.input_root, seed=20250730)
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
            raise RuntimeError("dbt executable is unavailable for integration tests.")

        cls.source_rows = {
            "run_1": read_csv_rows(cls.input_root / "run_1/orders.csv"),
            "run_2": read_csv_rows(cls.input_root / "run_2/orders.csv"),
            "run_3": read_csv_rows(cls.input_root / "run_3/orders.csv"),
            "late": read_csv_rows(
                cls.input_root / "run_3/late_arriving/orders.csv"
            ),
        }
        cls.ordinary_counts = {
            run: len(rows)
            for run, rows in cls.source_rows.items()
            if run != "late"
        }
        cls.ingestor = RawIngestor(
            cls.database_path,
            input_root=cls.input_root,
        )
        cls.states: dict[str, tuple[tuple[object, ...], ...]] = {}
        cls.dimension_states: dict[
            str, tuple[tuple[tuple[object, ...], ...], ...]
        ] = {}
        cls.metrics: dict[str, dict[str, int]] = {}
        cls.replay_results: dict[str, object] = {}

        cls._load_build_and_replay("run_1", "after_run_1")
        cls._load_build_and_replay("run_2", "after_run_2")
        cls._load_build_and_replay("run_3", "after_run_3_ordinary")
        cls._load_build_and_replay(
            "run_3_late_arriving",
            "after_late_arriving",
        )

        final_state = cls.states["after_late_arriving"]
        reprocessing = cls.ingestor.ingest_dataset()
        cls._run_dbt_build()
        cls.reprocessing_result = reprocessing
        cls.states["after_full_reprocessing"] = cls._fact_state()
        if cls.states["after_full_reprocessing"] != final_state:
            raise AssertionError("Full reprocessing changed the final fact state.")
        if cls._dimension_state() != cls.dimension_states["after_late_arriving"]:
            raise AssertionError(
                "Full reprocessing changed a dimensional table."
            )

        cls.full_refresh_database_path = (
            cls.root / "warehouse" / "full-refresh.duckdb"
        )
        shutil.copy2(cls.database_path, cls.full_refresh_database_path)
        full_refresh_profiles = cls.root / "full-refresh-profiles"
        full_refresh_profiles.mkdir()
        (full_refresh_profiles / "profiles.yml").write_text(
            yaml.safe_dump(
                {
                    "ecommerce_analytics": {
                        "target": "dev",
                        "outputs": {
                            "dev": {
                                "type": "duckdb",
                                "path": str(cls.full_refresh_database_path),
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
        cls._run_dbt_build(
            profiles_dir=full_refresh_profiles,
            target_path=cls.root / "full-refresh-target",
            log_path=cls.root / "full-refresh-logs",
            full_refresh=True,
        )
        cls.full_refresh_fact_state = cls._fact_state(
            cls.full_refresh_database_path
        )
        cls.full_refresh_dimension_state = cls._dimension_state(
            cls.full_refresh_database_path
        )

    @classmethod
    def _load_build_and_replay(cls, source_batch: str, label: str) -> None:
        first_result = cls.ingestor.ingest_batch(source_batch)
        if first_result.files_loaded == 0:
            raise AssertionError(f"{source_batch} did not load any source file.")
        cls._run_dbt_build()
        state = cls._fact_state()
        cls.states[label] = state
        dimension_state = cls._dimension_state()
        cls.dimension_states[label] = dimension_state
        cls.metrics[label] = cls._database_metrics()

        replay_result = cls.ingestor.ingest_batch(source_batch)
        cls._run_dbt_build()
        replay_state = cls._fact_state()
        if replay_state != state:
            raise AssertionError(f"Replay changed the fact after {source_batch}.")
        if cls._dimension_state() != dimension_state:
            raise AssertionError(
                f"Replay changed a dimension after {source_batch}."
            )
        cls.replay_results[source_batch] = replay_result

    @classmethod
    def _run_dbt_build(
        cls,
        *,
        profiles_dir: Path | None = None,
        target_path: Path | None = None,
        log_path: Path | None = None,
        full_refresh: bool = False,
    ) -> None:
        environment = os.environ.copy()
        environment["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
        environment["PYTHONDONTWRITEBYTECODE"] = "1"
        command = [
            cls.dbt_executable,
            "build",
            "--project-dir",
            str(REPOSITORY_ROOT),
            "--profiles-dir",
            str(profiles_dir or cls.profiles_dir),
            "--target-path",
            str(target_path or cls.target_path),
            "--log-path",
            str(log_path or cls.log_path),
        ]
        if full_refresh:
            command.append("--full-refresh")
        result = subprocess.run(
            command,
            cwd=REPOSITORY_ROOT,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
        if result.returncode != 0:
            raise AssertionError(
                "dbt build failed:\n"
                f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
            )

    @classmethod
    def _fact_state(
        cls, database_path: Path | None = None
    ) -> tuple[tuple[object, ...], ...]:
        connection = duckdb.connect(
            str(database_path or cls.database_path), read_only=True
        )
        try:
            return tuple(
                connection.execute(
                    """
                    SELECT *
                    FROM analytics.fct_order_lines
                    ORDER BY order_line_id
                    """
                ).fetchall()
            )
        finally:
            connection.close()

    @classmethod
    def _fact_business_state(
        cls, label: str
    ) -> dict[str, tuple[object, ...]]:
        rows = cls.states[label]
        connection = duckdb.connect(str(cls.database_path), read_only=True)
        try:
            columns = [
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info('analytics.fct_order_lines')"
                ).fetchall()
            ]
        finally:
            connection.close()
        indexes = {
            column: columns.index(column)
            for column in (
                "order_line_id",
                "quantity",
                "unit_price",
                "updated_at",
                "sales_channel",
                "canonical_payload",
            )
        }
        return {
            str(row[indexes["order_line_id"]]): tuple(
                row[indexes[column]]
                for column in (
                    "quantity",
                    "unit_price",
                    "updated_at",
                    "sales_channel",
                    "canonical_payload",
                )
            )
            for row in rows
        }

    @classmethod
    def _dimension_state(
        cls, database_path: Path | None = None
    ) -> tuple[tuple[tuple[object, ...], ...], ...]:
        connection = duckdb.connect(
            str(database_path or cls.database_path), read_only=True
        )
        try:
            return tuple(
                tuple(
                    connection.execute(
                        f"SELECT * FROM analytics.{table} ORDER BY {key}"
                    ).fetchall()
                )
                for table, key in (
                    ("dim_customers", "customer_sk"),
                    ("dim_products", "product_sk"),
                    ("dim_date", "date_sk"),
                )
            )
        finally:
            connection.close()

    @classmethod
    def _database_metrics(cls) -> dict[str, int]:
        connection = duckdb.connect(str(cls.database_path), read_only=True)
        try:
            (
                row_count,
                distinct_keys,
                duplicate_keys,
                null_historical_channels,
            ) = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT order_line_id),
                    COUNT(*) - COUNT(DISTINCT order_line_id),
                    COUNT(*) FILTER (
                        WHERE source_file <> 'run_3/orders.csv'
                          AND sales_channel IS NULL
                    )
                FROM analytics.fct_order_lines
                """
            ).fetchone()
            return {
                "row_count": row_count,
                "distinct_keys": distinct_keys,
                "duplicate_keys": duplicate_keys,
                "null_historical_channels": null_historical_channels,
            }
        finally:
            connection.close()

    def test_ordinary_windows_and_final_key_metrics(self) -> None:
        self.assertEqual(self.ordinary_counts, {
            "run_1": 3_000,
            "run_2": 1_000,
            "run_3": 1_000,
        })
        self.assertEqual(sum(self.ordinary_counts.values()), 5_000)
        expected_cumulative = {
            "after_run_1": self.ordinary_counts["run_1"],
            "after_run_2": self.ordinary_counts["run_1"]
            + self.ordinary_counts["run_2"],
            "after_run_3_ordinary": sum(self.ordinary_counts.values()),
            "after_late_arriving": FINAL_KEY_COUNT,
        }
        for label, expected_count in expected_cumulative.items():
            with self.subTest(phase=label):
                self.assertEqual(
                    self.metrics[label]["row_count"], expected_count
                )
                self.assertEqual(
                    self.metrics[label]["distinct_keys"], expected_count
                )
                self.assertEqual(self.metrics[label]["duplicate_keys"], 0)

    def test_late_inserts_and_corrections_apply_exactly_once(self) -> None:
        before = self._fact_business_state("after_run_3_ordinary")
        after = self._fact_business_state("after_late_arriving")
        initial_keys = set(before)
        late_keys = {
            row["order_line_id"] for row in self.source_rows["late"]
        }
        inserted_keys = set(after) - initial_keys
        changed_keys = {
            key for key in initial_keys if before[key] != after[key]
        }
        self.assertEqual(len(self.source_rows["late"]), 15)
        self.assertEqual(len(inserted_keys), LATE_METRICS["inserts"]["exact"])
        self.assertEqual(
            len(changed_keys), LATE_METRICS["corrections"]["exact"]
        )
        self.assertEqual(inserted_keys, late_keys - initial_keys)
        self.assertEqual(changed_keys, late_keys & initial_keys)
        self.assertTrue(all(after[key][3] is None for key in late_keys))

        expected_late = {
            row["order_line_id"]: row for row in self.source_rows["late"]
        }
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            actual_late = {
                row[0]: row[1:]
                for row in connection.execute(
                    """
                    SELECT
                        order_line_id,
                        order_id,
                        customer_id,
                        product_id,
                        quantity,
                        unit_price,
                        order_status,
                        order_date,
                        updated_at,
                        sales_channel
                    FROM analytics.fct_order_lines
                    WHERE source_file = 'run_3/late_arriving/orders.csv'
                    """
                ).fetchall()
            }
        finally:
            connection.close()
        self.assertEqual(set(actual_late), late_keys)
        for key, source in expected_late.items():
            expected = (
                source["order_id"],
                source["customer_id"],
                source["product_id"],
                int(source["quantity"]),
                Decimal(source["unit_price"])
                if source["unit_price"]
                else None,
                source["order_status"],
                date.fromisoformat(source["order_date"]),
                datetime.strptime(
                    source["updated_at"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ).replace(tzinfo=timezone.utc),
                None,
            )
            self.assertEqual(actual_late[key], expected)

    def test_equal_timestamp_uses_canonical_payload_tie_break(self) -> None:
        tie_database = self.root / "tie-break.duckdb"
        shutil.copy2(self.database_path, tie_database)
        connection = duckdb.connect(str(tie_database))
        try:
            common = (
                "OL_TIE_BREAK",
                "CUS_000001",
                "PRD_000001",
                1,
                Decimal("10.00"),
                "completed",
                date(2025, 1, 1),
                datetime(2025, 1, 31, 12, tzinfo=timezone.utc),
                None,
            )
            metadata_time = datetime(2030, 1, 1, tzinfo=timezone.utc)
            connection.executemany(
                """
                INSERT INTO raw.order_lines (
                    order_line_id,
                    order_id,
                    customer_id,
                    product_id,
                    quantity,
                    unit_price,
                    order_status,
                    order_date,
                    updated_at,
                    sales_channel,
                    ingestion_id,
                    source_file,
                    source_run,
                    source_row_number,
                    row_hash,
                    file_checksum,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        common[0],
                        order_id,
                        *common[1:],
                        f"tie-{row_number}",
                        "test/tie-break.csv",
                        "test_tie_break",
                        row_number,
                        f"hash-{row_number}",
                        "test-checksum",
                        metadata_time,
                    )
                    for row_number, order_id in ((1, "ORD_A"), (2, "ORD_Z"))
                ],
            )
        finally:
            connection.close()

        tie_profiles = self.root / "tie-profiles"
        tie_profiles.mkdir()
        (tie_profiles / "profiles.yml").write_text(
            yaml.safe_dump(
                {
                    "ecommerce_analytics": {
                        "target": "dev",
                        "outputs": {
                            "dev": {
                                "type": "duckdb",
                                "path": str(tie_database),
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
        result = subprocess.run(
            [
                self.dbt_executable,
                "build",
                "--project-dir",
                str(REPOSITORY_ROOT),
                "--profiles-dir",
                str(tie_profiles),
                "--target-path",
                str(self.root / "tie-target"),
                "--log-path",
                str(self.root / "tie-logs"),
            ],
            cwd=REPOSITORY_ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        connection = duckdb.connect(str(tie_database), read_only=True)
        try:
            winner = connection.execute(
                """
                SELECT order_id
                FROM analytics.fct_order_lines
                WHERE order_line_id = 'OL_TIE_BREAK'
                """
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(winner, ("ORD_Z",))

    def test_schema_evolution_and_unknown_members(self) -> None:
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            columns = {
                row[1]
                for row in connection.execute(
                    "PRAGMA table_info('analytics.fct_order_lines')"
                ).fetchall()
            }
            self.assertIn("sales_channel", columns)
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM analytics.fct_order_lines
                    WHERE source_file <> 'run_3/orders.csv'
                      AND sales_channel IS NOT NULL
                    """
                ).fetchone()[0],
                0,
            )
            orphan_counts = connection.execute(
                """
                SELECT
                    COUNT(*) FILTER (
                        WHERE customer_id = 'CUS_ORPHAN'
                          AND customer_sk = 'customer|unknown'
                    ),
                    COUNT(*) FILTER (
                        WHERE product_id = 'PRD_ORPHAN'
                          AND product_sk = 'product|unknown'
                    )
                FROM analytics.fct_order_lines
                """
            ).fetchone()
            self.assertEqual(orphan_counts, (5, 5))
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM analytics.dim_customers"
                ).fetchone()[0],
                201,
            )
            self.assertEqual(
                connection.execute(
                    "SELECT COUNT(*) FROM analytics.dim_products"
                ).fetchone()[0],
                81,
            )
        finally:
            connection.close()

    def test_invalid_records_are_retained_and_excluded_from_revenue(self) -> None:
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            counts = connection.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE unit_price IS NULL),
                    COUNT(*) FILTER (WHERE unit_price < 0),
                    COUNT(*) FILTER (WHERE unit_price = 0),
                    COUNT(*) FILTER (WHERE order_status = 'cancelled'),
                    COUNT(*) FILTER (
                        WHERE order_status NOT IN ('completed', 'cancelled')
                    ),
                    COUNT(*) FILTER (
                        WHERE NOT is_economically_valid
                          AND revenue IS NOT NULL
                    ),
                    COUNT(*) FILTER (
                        WHERE unit_price = 0
                          AND (
                              NOT is_economically_valid
                              OR revenue <> cast(0 as decimal(18, 2))
                          )
                    )
                FROM analytics.fct_order_lines
                """
            ).fetchone()
            self.assertEqual(counts, (5, 5, 5, 5, 5, 0, 0))
        finally:
            connection.close()

    def test_every_replay_and_full_reprocessing_are_no_ops(self) -> None:
        for source_batch, result in self.replay_results.items():
            with self.subTest(source_batch=source_batch):
                self.assertEqual(result.files_loaded, 0)
                self.assertGreater(result.files_skipped, 0)
                self.assertEqual(result.rows_inserted, 0)
        self.assertEqual(self.reprocessing_result.files_loaded, 0)
        self.assertEqual(self.reprocessing_result.files_skipped, 6)
        self.assertEqual(self.reprocessing_result.rows_inserted, 0)
        self.assertEqual(
            self.states["after_full_reprocessing"],
            self.states["after_late_arriving"],
        )

    def test_incremental_result_matches_true_full_refresh(self) -> None:
        self.assertEqual(
            self.full_refresh_fact_state,
            self.states["after_late_arriving"],
        )
        self.assertEqual(
            self.full_refresh_dimension_state,
            self.dimension_states["after_late_arriving"],
        )


if __name__ == "__main__":
    unittest.main()
