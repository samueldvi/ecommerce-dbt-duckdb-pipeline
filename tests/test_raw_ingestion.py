"""Contract and ACID tests for the DuckDB raw ingestion layer."""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import threading
import traceback
import unittest
from collections import Counter
from datetime import datetime
from pathlib import Path

import duckdb
import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPOSITORY_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from raw_ingestion import (  # noqa: E402
    IngestionError,
    RawIngestor,
    SchemaValidationError,
    SourceConflictError,
)
from synthetic_data import generate_dataset  # noqa: E402

EXPECTED_FILES = (
    Path("run_1/customers.csv"),
    Path("run_1/products.csv"),
    Path("run_1/orders.csv"),
    Path("run_2/orders.csv"),
    Path("run_3/orders.csv"),
    Path("run_3/late_arriving/orders.csv"),
)
EXPECTED_TABLE_COLUMNS = {
    "customers": (
        "customer_id",
        "customer_name",
        "email",
        "country_code",
        "updated_at",
        "ingestion_id",
        "source_file",
        "source_run",
        "source_row_number",
        "row_hash",
        "file_checksum",
        "ingested_at",
    ),
    "products": (
        "product_id",
        "product_name",
        "category",
        "ingestion_id",
        "source_file",
        "source_run",
        "source_row_number",
        "row_hash",
        "file_checksum",
        "ingested_at",
    ),
    "order_lines": (
        "order_line_id",
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "unit_price",
        "order_status",
        "order_date",
        "updated_at",
        "sales_channel",
        "ingestion_id",
        "source_file",
        "source_run",
        "source_row_number",
        "row_hash",
        "file_checksum",
        "ingested_at",
    ),
    "ingestion_manifest": (
        "ingestion_id",
        "source_file",
        "dataset_name",
        "source_run",
        "file_checksum",
        "file_size_bytes",
        "row_count",
        "ingested_at",
    ),
}

with (REPOSITORY_ROOT / "docs/expected_run_metrics.yml").open(
    encoding="utf-8"
) as metrics_handle:
    CONTRACT_METRICS = yaml.safe_load(metrics_handle)

INITIAL_ORDER_COUNT = CONTRACT_METRICS["order_lines"]["initial_dataset"]["input_rows"][
    "exact"
]
FINAL_ORDER_KEYS = CONTRACT_METRICS["order_lines"]["final"]["distinct_keys"]["exact"]
LATE_METRICS = CONTRACT_METRICS["order_lines"]["runs"]["run_3"]["late_arriving"]
CUSTOMER_KEYS = CONTRACT_METRICS["master_data"]["customers"]["distinct_keys"]["exact"]
PRODUCT_KEYS = CONTRACT_METRICS["master_data"]["products"]["distinct_keys"]["exact"]


def read_csv(path: Path) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(reader.fieldnames or ()), list(reader)


def write_csv(path: Path, columns: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def file_checksum(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def independent_row_hash(columns: tuple[str, ...], values: tuple[str, ...]) -> str:
    payload = json.dumps(
        list(zip(columns, values, strict=True)),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def raw_counts(database_path: Path) -> tuple[int, int, int, int]:
    connection = duckdb.connect(str(database_path), read_only=True)
    try:
        return connection.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM raw.customers),
                (SELECT COUNT(*) FROM raw.products),
                (SELECT COUNT(*) FROM raw.order_lines),
                (SELECT COUNT(*) FROM raw.ingestion_manifest)
            """
        ).fetchone()
    finally:
        connection.close()


class FailingAfterSecondFileIngestor(RawIngestor):
    """Raise after prior data and manifest writes exist in the transaction."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.inserted_files = 0

    def _after_file_rows_inserted(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: object,
    ) -> None:
        self.inserted_files += 1
        if self.inserted_files == 2:
            raise RuntimeError("injected failure after partial inserts")


class PausingRawIngestor(RawIngestor):
    """Pause a transaction after real raw inserts and before commit."""

    def __init__(
        self,
        *args: object,
        rows_inserted: threading.Event,
        resume: threading.Event,
        **kwargs: object,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.rows_inserted = rows_inserted
        self.resume = resume
        self.paused = False

    def _after_file_rows_inserted(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: object,
    ) -> None:
        if self.paused:
            return
        self.paused = True
        self.rows_inserted.set()
        if not self.resume.wait(timeout=30):
            raise RuntimeError("timed out waiting to resume ingestion")


class RawIngestionTests(unittest.TestCase):
    """Verify raw grain, traceability, idempotence, and ACID boundaries."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.shared_directory = tempfile.TemporaryDirectory()
        cls.shared_root = Path(cls.shared_directory.name)
        cls.generated_root = cls.shared_root / "generated source with spaces"
        generate_dataset(cls.generated_root, seed=20250730)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.shared_directory.cleanup()

    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.input_root = self.root / "input data with spaces"
        shutil.copytree(self.generated_root, self.input_root)
        self.database_path = self.root / "warehouse with spaces" / "raw database.duckdb"
        self.ingestor = RawIngestor(self.database_path, input_root=self.input_root)

    def tearDown(self) -> None:
        self.directory.cleanup()

    def test_initialize_schema_on_empty_database_with_explicit_types(
        self,
    ) -> None:
        self.ingestor.initialize_schema()
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            rows = connection.execute(
                """
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'raw'
                ORDER BY table_name, ordinal_position
                """
            ).fetchall()
            actual_columns: dict[str, list[str]] = {}
            actual_types: dict[tuple[str, str], str] = {}
            for table_name, column_name, data_type in rows:
                actual_columns.setdefault(table_name, []).append(column_name)
                actual_types[(table_name, column_name)] = data_type
            self.assertEqual(
                actual_columns,
                {
                    table: list(columns)
                    for table, columns in EXPECTED_TABLE_COLUMNS.items()
                },
            )
            self.assertEqual(actual_types[("order_lines", "quantity")], "INTEGER")
            self.assertEqual(
                actual_types[("order_lines", "unit_price")],
                "DECIMAL(12,2)",
            )
            self.assertEqual(actual_types[("order_lines", "order_date")], "DATE")
            self.assertEqual(
                actual_types[("order_lines", "updated_at")],
                "TIMESTAMP WITH TIME ZONE",
            )
            self.assertEqual(raw_counts(self.database_path), (0, 0, 0, 0))
        finally:
            connection.close()

    def test_existing_incompatible_database_schema_is_rejected(self) -> None:
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        connection = duckdb.connect(str(self.database_path))
        try:
            connection.execute("CREATE SCHEMA raw")
            connection.execute(
                """
                CREATE TABLE raw.order_lines (
                    order_line_id VARCHAR NOT NULL,
                    order_id VARCHAR NOT NULL,
                    customer_id VARCHAR NOT NULL,
                    product_id VARCHAR NOT NULL,
                    quantity INTEGER NOT NULL,
                    unit_price VARCHAR,
                    order_status VARCHAR NOT NULL,
                    order_date DATE NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    sales_channel VARCHAR,
                    ingestion_id VARCHAR NOT NULL,
                    source_file VARCHAR NOT NULL,
                    source_run VARCHAR NOT NULL,
                    source_row_number BIGINT NOT NULL,
                    row_hash VARCHAR NOT NULL,
                    file_checksum VARCHAR NOT NULL,
                    ingested_at TIMESTAMPTZ NOT NULL
                )
                """
            )
        finally:
            connection.close()

        with self.assertRaisesRegex(
            SchemaValidationError, "Raw database schema mismatch"
        ):
            self.ingestor.initialize_schema()

        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            self.assertEqual(
                connection.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'raw'
                      AND table_name = 'order_lines'
                      AND column_name = 'unit_price'
                    """
                ).fetchone()[0],
                "VARCHAR",
            )
            self.assertEqual(
                connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'raw'
                    """
                ).fetchone()[0],
                1,
            )
        finally:
            connection.close()

    def test_complete_ingestion_preserves_raw_grain_and_corrections(
        self,
    ) -> None:
        csv_rows = {
            path: read_csv(self.input_root / path)[1] for path in EXPECTED_FILES
        }
        result = self.ingestor.ingest_dataset()
        self.assertEqual(result.files_loaded, 6)
        self.assertEqual(result.files_skipped, 0)
        self.assertEqual(
            result.rows_inserted,
            sum(len(rows) for rows in csv_rows.values()),
        )
        self.assertEqual(result.errors, ())

        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            self.assertEqual(
                raw_counts(self.database_path),
                (204, 80, INITIAL_ORDER_COUNT + 15, 6),
            )
            distinct_keys = connection.execute(
                "SELECT COUNT(DISTINCT order_line_id) FROM raw.order_lines"
            ).fetchone()[0]
            self.assertEqual(distinct_keys, FINAL_ORDER_KEYS)

            initial_keys = {
                row["order_line_id"]
                for path in EXPECTED_FILES[2:5]
                for row in csv_rows[path]
            }
            late_keys = {row["order_line_id"] for row in csv_rows[EXPECTED_FILES[5]]}
            correction_keys = initial_keys & late_keys
            self.assertEqual(
                len(correction_keys),
                LATE_METRICS["corrections"]["exact"],
            )
            duplicate_versions = dict(
                connection.execute(
                    """
                    SELECT order_line_id, COUNT(*)
                    FROM raw.order_lines
                    GROUP BY order_line_id
                    HAVING COUNT(*) > 1
                    """
                ).fetchall()
            )
            self.assertEqual(
                duplicate_versions,
                {key: 2 for key in correction_keys},
            )
        finally:
            connection.close()

    def test_anomalies_and_schema_evolution_are_preserved(self) -> None:
        self.ingestor.ingest_dataset()
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            anomaly_counts = connection.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE unit_price IS NULL),
                    COUNT(*) FILTER (WHERE unit_price < 0),
                    COUNT(*) FILTER (WHERE unit_price = 0),
                    COUNT(*) FILTER (WHERE order_status = 'cancelled'),
                    COUNT(*) FILTER (
                        WHERE order_status NOT IN ('completed', 'cancelled')
                    ),
                    COUNT(*) FILTER (WHERE customer_id = 'CUS_ORPHAN'),
                    COUNT(*) FILTER (WHERE product_id = 'PRD_ORPHAN')
                FROM raw.order_lines
                """
            ).fetchone()
            self.assertEqual(anomaly_counts, (5, 5, 5, 5, 5, 5, 5))
            non_run_3_channels = connection.execute(
                """
                SELECT COUNT(*)
                FROM raw.order_lines
                WHERE source_file <> 'run_3/orders.csv'
                  AND sales_channel IS NOT NULL
                """
            ).fetchone()[0]
            self.assertEqual(non_run_3_channels, 0)
            run_3_count, populated_channels = connection.execute(
                """
                SELECT
                    COUNT(*),
                    COUNT(*) FILTER (WHERE sales_channel IS NOT NULL)
                FROM raw.order_lines
                WHERE source_file = 'run_3/orders.csv'
                """
            ).fetchone()
            self.assertEqual(run_3_count, 1_000)
            self.assertGreater(populated_channels, 0)
        finally:
            connection.close()

    def test_manifest_and_row_metadata_match_source_bytes(self) -> None:
        self.ingestor.ingest_dataset()
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            manifest_rows = connection.execute(
                """
                SELECT
                    source_file,
                    dataset_name,
                    source_run,
                    file_checksum,
                    file_size_bytes,
                    row_count,
                    ingestion_id,
                    ingested_at
                FROM raw.ingestion_manifest
                ORDER BY source_file
                """
            ).fetchall()
            self.assertEqual(len(manifest_rows), 6)
            for (
                source_file,
                dataset_name,
                source_run,
                checksum,
                file_size,
                row_count,
                ingestion_id,
                ingested_at,
            ) in manifest_rows:
                source_path = self.input_root / source_file
                columns, rows = read_csv(source_path)
                self.assertEqual(checksum, file_checksum(source_path))
                self.assertEqual(file_size, len(source_path.read_bytes()))
                self.assertEqual(row_count, len(rows))
                self.assertFalse(Path(source_file).is_absolute())
                self.assertIn(dataset_name, {"customers", "products", "order_lines"})
                self.assertTrue(source_run.startswith("run_"))
                self.assertEqual(len(ingestion_id), 64)
                self.assertIsInstance(ingested_at, datetime)

                table = {
                    "customers": "customers",
                    "products": "products",
                    "order_lines": "order_lines",
                }[dataset_name]
                metadata_rows = connection.execute(
                    f"""
                    SELECT
                        source_row_number,
                        row_hash,
                        file_checksum,
                        source_file,
                        source_run
                    FROM raw.{table}
                    WHERE ingestion_id = ?
                    ORDER BY source_row_number
                    """,
                    [ingestion_id],
                ).fetchall()
                self.assertEqual(len(metadata_rows), len(rows))
                with source_path.open(encoding="utf-8", newline="") as handle:
                    csv_reader = csv.reader(handle)
                    self.assertEqual(tuple(next(csv_reader)), columns)
                    expected_hashes = [
                        (
                            csv_reader.line_num,
                            independent_row_hash(columns, tuple(values)),
                            checksum,
                            source_file,
                            source_run,
                        )
                        for values in csv_reader
                    ]
                self.assertEqual(metadata_rows, expected_hashes)
        finally:
            connection.close()

    def test_manifest_and_raw_rows_commit_atomically_and_remain_consistent(
        self,
    ) -> None:
        self.ingestor.ingest_dataset()
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            manifest = connection.execute(
                """
                SELECT ingestion_id, dataset_name, row_count, file_checksum
                FROM raw.ingestion_manifest
                """
            ).fetchall()
            self.assertEqual(len(manifest), 6)
            for ingestion_id, dataset_name, row_count, checksum in manifest:
                table = {
                    "customers": "customers",
                    "products": "products",
                    "order_lines": "order_lines",
                }[dataset_name]
                actual = connection.execute(
                    f"""
                    SELECT COUNT(*), COUNT(DISTINCT file_checksum)
                    FROM raw.{table}
                    WHERE ingestion_id = ? AND file_checksum = ?
                    """,
                    [ingestion_id, checksum],
                ).fetchone()
                self.assertEqual(actual, (row_count, 1))
            orphan_raw_rows = sum(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM raw.{table} AS source
                    LEFT JOIN raw.ingestion_manifest AS manifest
                      ON manifest.ingestion_id = source.ingestion_id
                    WHERE manifest.ingestion_id IS NULL
                    """
                ).fetchone()[0]
                for table in ("customers", "products", "order_lines")
            )
            self.assertEqual(orphan_raw_rows, 0)
            ingestion_time_mismatches = sum(
                connection.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM raw.{table} AS source
                    JOIN raw.ingestion_manifest AS manifest
                      ON manifest.ingestion_id = source.ingestion_id
                    WHERE source.ingested_at <> manifest.ingested_at
                    """
                ).fetchone()[0]
                for table in ("customers", "products", "order_lines")
            )
            self.assertEqual(ingestion_time_mismatches, 0)
        finally:
            connection.close()

    def test_replay_after_reopen_is_durable_and_idempotent(self) -> None:
        first_result = self.ingestor.ingest_dataset()
        first_counts = raw_counts(self.database_path)
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            first_manifest = connection.execute(
                """
                SELECT *
                FROM raw.ingestion_manifest
                ORDER BY source_file
                """
            ).fetchall()
        finally:
            connection.close()

        reopened_ingestor = RawIngestor(self.database_path, input_root=self.input_root)
        replay = reopened_ingestor.ingest_dataset()
        self.assertEqual(first_result.files_loaded, 6)
        self.assertEqual(replay.files_loaded, 0)
        self.assertEqual(replay.files_skipped, 6)
        self.assertEqual(replay.rows_inserted, 0)
        self.assertTrue(all(file.status == "skipped" for file in replay.files))
        self.assertEqual(raw_counts(self.database_path), first_counts)

        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            second_manifest = connection.execute(
                """
                SELECT *
                FROM raw.ingestion_manifest
                ORDER BY source_file
                """
            ).fetchall()
        finally:
            connection.close()
        self.assertEqual(first_manifest, second_manifest)

    def test_single_file_load_and_replay_are_idempotent(self) -> None:
        first = self.ingestor.ingest_file(Path("run_2/orders.csv"))
        reopened = RawIngestor(self.database_path, input_root=self.input_root)
        second = reopened.ingest_file(Path("run_2/orders.csv"))
        source_rows = len(read_csv(self.input_root / "run_2/orders.csv")[1])
        self.assertEqual(first.files_loaded, 1)
        self.assertEqual(first.rows_inserted, source_rows)
        self.assertEqual(second.files_skipped, 1)
        self.assertEqual(second.rows_inserted, 0)
        self.assertEqual(raw_counts(self.database_path), (0, 0, source_rows, 1))

    def test_customer_snapshot_accepts_three_versioned_customer_keys(
        self,
    ) -> None:
        source = self.input_root / "run_1/customers.csv"
        columns, rows = read_csv(source)
        counts = Counter(row["customer_id"] for row in rows)
        customer_to_reduce = next(
            customer_id for customer_id, count in counts.items() if count > 1
        )
        retained_version = False
        reduced_rows = []
        for row in rows:
            if row["customer_id"] != customer_to_reduce:
                reduced_rows.append(row)
            elif not retained_version:
                reduced_rows.append(row)
                retained_version = True
        write_csv(source, columns, reduced_rows)

        result = self.ingestor.ingest_file(Path("run_1/customers.csv"))

        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            row_count, distinct_keys, versioned_keys = connection.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM raw.customers),
                    COUNT(*),
                    COUNT(*) FILTER (WHERE version_count > 1)
                FROM (
                    SELECT customer_id, COUNT(*) AS version_count
                    FROM raw.customers
                    GROUP BY customer_id
                )
                """
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(result.rows_inserted, len(reduced_rows))
        self.assertEqual(
            (row_count, distinct_keys, versioned_keys),
            (len(reduced_rows), 200, 3),
        )

    def test_product_snapshot_accepts_duplicate_business_keys(self) -> None:
        source = self.input_root / "run_1/products.csv"
        columns, rows = read_csv(source)
        duplicate = dict(rows[0])
        duplicate["product_name"] = "ZZZ Canonical Product Variant"
        rows.append(duplicate)
        write_csv(source, columns, rows)

        result = self.ingestor.ingest_file(Path("run_1/products.csv"))

        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            actual = connection.execute(
                """
                SELECT COUNT(*), COUNT(DISTINCT product_id)
                FROM raw.products
                """
            ).fetchone()
        finally:
            connection.close()
        self.assertEqual(result.rows_inserted, 81)
        self.assertEqual(actual, (81, 80))

    def test_out_of_domain_sales_channel_is_preserved_in_raw(self) -> None:
        source = self.input_root / "run_3/orders.csv"
        columns, rows = read_csv(source)
        target_key = rows[0]["order_line_id"]
        rows[0]["sales_channel"] = "store"
        write_csv(source, columns, rows)

        result = self.ingestor.ingest_file(Path("run_3/orders.csv"))

        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            preserved_value = connection.execute(
                """
                SELECT sales_channel
                FROM raw.order_lines
                WHERE order_line_id = ?
                """,
                [target_key],
            ).fetchone()[0]
        finally:
            connection.close()
        self.assertEqual(result.rows_inserted, len(rows))
        self.assertEqual(preserved_value, "store")

    def test_logical_batch_load_and_replay_are_atomic_and_idempotent(
        self,
    ) -> None:
        first = self.ingestor.ingest_batch("run_1")
        second = RawIngestor(
            self.database_path, input_root=self.input_root
        ).ingest_batch("run_1")
        self.assertEqual(first.files_loaded, 3)
        self.assertEqual(first.rows_inserted, 3_284)
        self.assertEqual(second.files_skipped, 3)
        self.assertEqual(second.rows_inserted, 0)
        self.assertEqual(raw_counts(self.database_path), (204, 80, 3_000, 3))

    def test_run_2_rejects_existing_order_keys_without_partial_changes(
        self,
    ) -> None:
        self.ingestor.ingest_batch("run_1")
        source = self.input_root / "run_2/orders.csv"
        columns, rows = read_csv(source)
        run_1_rows = read_csv(self.input_root / "run_1/orders.csv")[1]
        rows[0]["order_line_id"] = run_1_rows[0]["order_line_id"]
        write_csv(source, columns, rows)

        with self.assertRaisesRegex(
            SchemaValidationError, "must contain only new order_line_id"
        ):
            self.ingestor.ingest_batch("run_2")

        self.assertEqual(raw_counts(self.database_path), (204, 80, 3_000, 3))

    def test_run_3_rejects_existing_order_keys_without_partial_changes(
        self,
    ) -> None:
        self.ingestor.ingest_batch("run_1")
        self.ingestor.ingest_batch("run_2")
        source = self.input_root / "run_3/orders.csv"
        columns, rows = read_csv(source)
        run_2_rows = read_csv(self.input_root / "run_2/orders.csv")[1]
        rows[0]["order_line_id"] = run_2_rows[0]["order_line_id"]
        write_csv(source, columns, rows)

        with self.assertRaisesRegex(
            SchemaValidationError, "must contain only new order_line_id"
        ):
            self.ingestor.ingest_batch("run_3")

        self.assertEqual(raw_counts(self.database_path), (204, 80, 4_000, 4))

    def test_late_batch_rejects_correction_outside_run_1_atomically(
        self,
    ) -> None:
        self.ingestor.ingest_batch("run_1")
        self.ingestor.ingest_batch("run_2")
        self.ingestor.ingest_batch("run_3")
        source = self.input_root / "run_3/late_arriving/orders.csv"
        columns, rows = read_csv(source)
        run_1_keys = {
            row["order_line_id"]
            for row in read_csv(self.input_root / "run_1/orders.csv")[1]
        }
        correction = next(row for row in rows if row["order_line_id"] in run_1_keys)
        run_2_key = read_csv(self.input_root / "run_2/orders.csv")[1][0][
            "order_line_id"
        ]
        correction["order_line_id"] = run_2_key
        write_csv(source, columns, rows)

        with self.assertRaisesRegex(SchemaValidationError, "exclusively in Run 1"):
            self.ingestor.ingest_batch("run_3_late_arriving")

        self.assertEqual(raw_counts(self.database_path), (204, 80, 5_000, 5))

    def test_complete_ingestion_rejects_late_insert_before_run_2_cutoff(
        self,
    ) -> None:
        source = self.input_root / "run_3/late_arriving/orders.csv"
        columns, rows = read_csv(source)
        run_2_rows = read_csv(self.input_root / "run_2/orders.csv")[1]
        rows[0]["updated_at"] = max(row["updated_at"] for row in run_2_rows)
        write_csv(source, columns, rows)

        with self.assertRaisesRegex(
            SchemaValidationError, "updated_at later than Run 2"
        ):
            self.ingestor.ingest_dataset()

        self.assertFalse(self.database_path.exists())

    def test_late_batch_rejects_late_insert_before_run_2_cutoff_atomically(
        self,
    ) -> None:
        self.ingestor.ingest_batch("run_1")
        self.ingestor.ingest_batch("run_2")
        self.ingestor.ingest_batch("run_3")
        source = self.input_root / "run_3/late_arriving/orders.csv"
        columns, rows = read_csv(source)
        run_2_rows = read_csv(self.input_root / "run_2/orders.csv")[1]
        rows[0]["updated_at"] = max(row["updated_at"] for row in run_2_rows)
        write_csv(source, columns, rows)

        with self.assertRaisesRegex(
            SchemaValidationError, "updated_at later than Run 2"
        ):
            self.ingestor.ingest_batch("run_3_late_arriving")

        self.assertEqual(raw_counts(self.database_path), (204, 80, 5_000, 5))

    def test_changed_source_path_conflicts_without_partial_changes(self) -> None:
        source = Path("run_1/products.csv")
        self.ingestor.ingest_file(source)
        original_counts = raw_counts(self.database_path)
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            original_manifest = connection.execute(
                "SELECT * FROM raw.ingestion_manifest"
            ).fetchall()
        finally:
            connection.close()

        columns, rows = read_csv(self.input_root / source)
        rows[0]["product_name"] += " Changed"
        write_csv(self.input_root / source, columns, rows)
        with self.assertRaisesRegex(
            SourceConflictError, "already ingested with checksum"
        ):
            self.ingestor.ingest_dataset()

        self.assertEqual(raw_counts(self.database_path), original_counts)
        connection = duckdb.connect(str(self.database_path), read_only=True)
        try:
            self.assertEqual(
                connection.execute("SELECT * FROM raw.ingestion_manifest").fetchall(),
                original_manifest,
            )
        finally:
            connection.close()

    def test_structural_and_type_errors_leave_no_ingestion_state(self) -> None:
        cases = (
            "missing_column",
            "unexpected_column",
            "integer",
            "decimal",
            "timestamp",
            "csv",
        )
        for case in cases:
            with self.subTest(case=case):
                case_root = self.root / case
                input_root = case_root / "input"
                shutil.copytree(self.generated_root, input_root)
                database = case_root / "database.duckdb"
                source = input_root / "run_1/orders.csv"
                if case == "missing_column":
                    columns, rows = read_csv(source)
                    reduced_columns = columns[:-1]
                    write_csv(
                        source,
                        reduced_columns,
                        [
                            {column: row[column] for column in reduced_columns}
                            for row in rows
                        ],
                    )
                elif case == "unexpected_column":
                    columns, rows = read_csv(source)
                    extra_columns = columns + ("unexpected",)
                    for row in rows:
                        row["unexpected"] = "value"
                    write_csv(source, extra_columns, rows)
                elif case == "integer":
                    columns, rows = read_csv(source)
                    rows[0]["quantity"] = "not-an-integer"
                    write_csv(source, columns, rows)
                elif case == "decimal":
                    columns, rows = read_csv(source)
                    rows[0]["unit_price"] = "1.234"
                    write_csv(source, columns, rows)
                elif case == "timestamp":
                    columns, rows = read_csv(source)
                    rows[0]["updated_at"] = "2025-01-01T00:00:00Z"
                    write_csv(source, columns, rows)
                else:
                    source.write_text(
                        ",".join(
                            (
                                "order_line_id",
                                "order_id",
                                "customer_id",
                                "product_id",
                                "quantity",
                                "unit_price",
                                "order_status",
                                "order_date",
                                "updated_at",
                            )
                        )
                        + '\n"unterminated',
                        encoding="utf-8",
                    )
                with self.assertRaises(SchemaValidationError):
                    RawIngestor(database, input_root=input_root).ingest_file(
                        Path("run_1/orders.csv")
                    )
                self.assertFalse(database.exists())

    def test_error_after_partial_inserts_rolls_back_data_and_manifest(
        self,
    ) -> None:
        ingestor = FailingAfterSecondFileIngestor(
            self.database_path, input_root=self.input_root
        )
        with self.assertRaisesRegex(
            IngestionError, "injected failure after partial inserts"
        ):
            ingestor.ingest_dataset()
        self.assertEqual(raw_counts(self.database_path), (0, 0, 0, 0))

    def test_uncommitted_data_and_manifest_are_invisible_to_second_connection(
        self,
    ) -> None:
        self.ingestor.initialize_schema()
        rows_inserted = threading.Event()
        resume = threading.Event()
        ingestor = PausingRawIngestor(
            self.database_path,
            input_root=self.input_root,
            rows_inserted=rows_inserted,
            resume=resume,
        )
        worker_errors: list[str] = []

        def run_ingestion() -> None:
            try:
                ingestor.ingest_dataset()
            except BaseException:
                worker_errors.append(traceback.format_exc())

        worker = threading.Thread(target=run_ingestion, daemon=True)
        try:
            worker.start()
            self.assertTrue(
                rows_inserted.wait(timeout=30),
                "Writer did not reach the uncommitted insert boundary.",
            )
            second_connection = duckdb.connect(str(self.database_path))
            try:
                visible_counts = second_connection.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM raw.customers),
                        (SELECT COUNT(*) FROM raw.products),
                        (SELECT COUNT(*) FROM raw.order_lines),
                        (SELECT COUNT(*) FROM raw.ingestion_manifest)
                    """
                ).fetchone()
            finally:
                second_connection.close()
            self.assertEqual(visible_counts, (0, 0, 0, 0))
            resume.set()
            worker.join(timeout=30)
        finally:
            resume.set()
            worker.join(timeout=30)
        self.assertFalse(worker.is_alive(), "Ingestion worker did not stop.")
        self.assertEqual(worker_errors, [])
        self.assertEqual(raw_counts(self.database_path), (204, 80, 5_015, 6))

    def test_complete_load_uses_managed_paths_not_filesystem_order(self) -> None:
        reordered_root = self.root / "reordered input"
        for relative_path in reversed(EXPECTED_FILES):
            target = reordered_root / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.generated_root / relative_path, target)
        foreign_file = reordered_root / "run_2/foreign.csv"
        foreign_file.write_text("not,managed\n1,2\n", encoding="utf-8")
        database = self.root / "reordered.duckdb"
        result = RawIngestor(database, input_root=reordered_root).ingest_dataset()
        self.assertEqual(
            [Path(file.source_file) for file in result.files],
            list(EXPECTED_FILES),
        )
        connection = duckdb.connect(str(database), read_only=True)
        try:
            manifest_files = {
                Path(row[0])
                for row in connection.execute(
                    "SELECT source_file FROM raw.ingestion_manifest"
                ).fetchall()
            }
            self.assertEqual(manifest_files, set(EXPECTED_FILES))
        finally:
            connection.close()

    def test_cli_initializes_loads_and_reports_structured_results(self) -> None:
        cli = REPOSITORY_ROOT / "scripts/ingest_raw_data.py"
        command_prefix = [
            sys.executable,
            str(cli),
            "--database",
            str(self.database_path),
            "--input-dir",
            str(self.input_root),
        ]
        initialized = subprocess.run(
            [*command_prefix, "init"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(initialized.returncode, 0, initialized.stderr)
        self.assertEqual(json.loads(initialized.stdout)["status"], "initialized")

        loaded = subprocess.run(
            [*command_prefix, "load-all"],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )
        self.assertEqual(loaded.returncode, 0, loaded.stderr)
        payload = json.loads(loaded.stdout)
        self.assertEqual(payload["files_loaded"], 6)
        self.assertEqual(payload["rows_inserted"], 5_299)

        replay = subprocess.run(
            [*command_prefix, "load-file", "run_2/orders.csv"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(replay.returncode, 0, replay.stderr)
        replay_payload = json.loads(replay.stdout)
        self.assertEqual(replay_payload["files_skipped"], 1)
        self.assertEqual(replay_payload["rows_inserted"], 0)

        batch_replay = subprocess.run(
            [*command_prefix, "load-batch", "run_1"],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
        self.assertEqual(batch_replay.returncode, 0, batch_replay.stderr)
        batch_payload = json.loads(batch_replay.stdout)
        self.assertEqual(batch_payload["files_skipped"], 3)
        self.assertEqual(batch_payload["rows_inserted"], 0)


if __name__ == "__main__":
    unittest.main()
