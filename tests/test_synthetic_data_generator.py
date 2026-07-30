from __future__ import annotations

import csv
import hashlib
import multiprocessing
import queue
import re
import subprocess
import sys
import tempfile
import time
import traceback
import unittest
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import yaml

import scripts.synthetic_data.generator as generator_module
from scripts.synthetic_data.generator import (
    GenerationError,
    OutputExistsError,
    generate_dataset,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
CLI_PATH = REPOSITORY_ROOT / "scripts" / "generate_synthetic_data.py"
DEFAULT_OUTPUT_DIR = REPOSITORY_ROOT / "data" / "raw"
CONTRACT_METRICS = yaml.safe_load(
    (REPOSITORY_ROOT / "docs/expected_run_metrics.yml").read_text(
        encoding="utf-8"
    )
)

DEFAULT_SEED = 20250730
EXPECTED_FILES = (
    Path("run_1/customers.csv"),
    Path("run_1/products.csv"),
    Path("run_1/orders.csv"),
    Path("run_2/orders.csv"),
    Path("run_3/orders.csv"),
)
CUSTOMER_COLUMNS = (
    "customer_id",
    "customer_name",
    "email",
    "country_code",
    "updated_at",
)
PRODUCT_COLUMNS = ("product_id", "product_name", "category")
ORDER_BASE_COLUMNS = (
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
ORDER_EVOLVED_COLUMNS = ORDER_BASE_COLUMNS + ("sales_channel",)
EXPECTED_SCHEMAS = {
    Path("run_1/customers.csv"): CUSTOMER_COLUMNS,
    Path("run_1/products.csv"): PRODUCT_COLUMNS,
    Path("run_1/orders.csv"): ORDER_BASE_COLUMNS,
    Path("run_2/orders.csv"): ORDER_EVOLVED_COLUMNS,
    Path("run_3/orders.csv"): ORDER_EVOLVED_COLUMNS,
}
ALLOWED_ORDER_STATUSES = frozenset({"completed", "cancelled"})
ALLOWED_SALES_CHANNELS = frozenset({"web", "mobile", "marketplace"})
EXPECTED_CORRECTION_KEYS = {
    "OL_00000100",
    "OL_00000500",
    "OL_00001000",
    "OL_00002000",
    "OL_00003000",
}
DECIMAL_PATTERN = re.compile(r"-?\d{1,10}\.\d{2}")
TIMESTAMP_PATTERN = re.compile(
    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z"
)

RUN_METRICS = CONTRACT_METRICS["order_lines"]["runs"]
RUN_1_COUNT = RUN_METRICS["run_1"]["input_rows"]["exact"]
RUN_2_COUNT = RUN_METRICS["run_2"]["input_rows"]["exact"]
RUN_3_COUNT = RUN_METRICS["run_3"]["input_rows"]["exact"]
RUN_3_INSERTS = RUN_METRICS["run_3"]["inserts"]["exact"]
RUN_3_CORRECTIONS = RUN_METRICS["run_3"]["corrections"]["exact"]
FINAL_DISTINCT = CONTRACT_METRICS["order_lines"]["final"]["distinct_keys"][
    "exact"
]


def read_csv(path: Path) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        return tuple(reader.fieldnames or ()), list(reader)


def file_hashes(root: Path) -> dict[Path, str]:
    return {
        relative_path: hashlib.sha256(
            (root / relative_path).read_bytes()
        ).hexdigest()
        for relative_path in EXPECTED_FILES
    }


def parse_timestamp(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")


def order_precedence(row: dict[str, str]) -> tuple[datetime, tuple[str, ...]]:
    payload = tuple(
        row.get(column, "")
        for column in ORDER_EVOLVED_COLUMNS
        if column != "order_line_id"
    )
    return parse_timestamp(row["updated_at"]), payload


def apply_run(
    current: dict[str, dict[str, str]],
    rows: list[dict[str, str]],
) -> tuple[int, int, int]:
    inserts = 0
    updates = 0
    no_ops = 0
    for candidate in rows:
        key = candidate["order_line_id"]
        existing = current.get(key)
        if existing is None:
            current[key] = dict(candidate)
            inserts += 1
        elif order_precedence(candidate) > order_precedence(existing):
            current[key] = dict(candidate)
            updates += 1
        else:
            no_ops += 1
    return inserts, updates, no_ops


def quality_keys(rows: list[dict[str, str]]) -> dict[str, set[str]]:
    customer_keys = {
        row["customer_id"]
        for row in SyntheticDataGeneratorTests.customers
    }
    product_keys = {
        row["product_id"]
        for row in SyntheticDataGeneratorTests.products
    }
    return {
        "cancelled": {
            row["order_line_id"]
            for row in rows
            if row["order_status"] == "cancelled"
        },
        "null_price": {
            row["order_line_id"]
            for row in rows
            if row["unit_price"] == ""
        },
        "non_positive_price": {
            row["order_line_id"]
            for row in rows
            if row["unit_price"]
            and Decimal(row["unit_price"]) <= Decimal("0.00")
        },
        "invalid_status": {
            row["order_line_id"]
            for row in rows
            if row["order_status"] not in ALLOWED_ORDER_STATUSES
        },
        "orphan_customer": {
            row["order_line_id"]
            for row in rows
            if row["customer_id"] not in customer_keys
        },
        "orphan_product": {
            row["order_line_id"]
            for row in rows
            if row["product_id"] not in product_keys
        },
    }


def run_paused_generation(
    output_root: str,
    seed: int,
    first_promotion_completed: object,
    resume_publication: object,
    result_queue: object,
) -> None:
    real_replace = generator_module.os.replace
    promotion_paused = False

    def pause_after_first_promotion(source: object, target: object) -> None:
        nonlocal promotion_paused
        real_replace(source, target)
        if "staged" in Path(source).parts and not promotion_paused:
            promotion_paused = True
            first_promotion_completed.set()
            if not resume_publication.wait(timeout=30):
                raise OSError("timed out waiting to resume publication")

    try:
        with patch.object(
            generator_module.os,
            "replace",
            side_effect=pause_after_first_promotion,
        ):
            generate_dataset(Path(output_root), seed=seed, force=True)
    except Exception:
        result_queue.put(("error", traceback.format_exc()))
    else:
        result_queue.put(("ok", seed))


def run_generation_after_lock_probe(
    output_root: str,
    seed: int,
    lock_probe_results: object,
    result_queue: object,
) -> None:
    try:
        if generator_module.fcntl is None:
            raise RuntimeError(
                "fcntl is unavailable in the contender process"
            )
        real_flock = generator_module.fcntl.flock

        def probe_then_acquire_lock(
            descriptor: int, operation: int
        ) -> None:
            try:
                real_flock(
                    descriptor,
                    operation | generator_module.fcntl.LOCK_NB,
                )
            except BlockingIOError:
                lock_probe_results.put("blocked")
                real_flock(descriptor, operation)
            else:
                lock_probe_results.put("acquired")

        with patch.object(
            generator_module.fcntl,
            "flock",
            side_effect=probe_then_acquire_lock,
        ):
            generate_dataset(Path(output_root), seed=seed, force=True)
    except Exception:
        result_queue.put(("error", traceback.format_exc()))
    else:
        result_queue.put(("ok", seed))


def run_generation(
    output_root: str,
    seed: int,
    result_queue: object,
) -> None:
    try:
        generate_dataset(Path(output_root), seed=seed, force=True)
    except Exception:
        result_queue.put(("error", traceback.format_exc()))
    else:
        result_queue.put(("ok", seed))


def hold_output_lock(
    output_root: str,
    lock_acquired: object,
    release_lock: object,
) -> None:
    destination = Path(output_root)
    destination.mkdir(parents=True, exist_ok=True)
    with generator_module._exclusive_output_lock(destination):
        lock_acquired.set()
        release_lock.wait(timeout=30)


WorkerMessage = tuple[str, int | str]
WorkerRecord = tuple[
    str,
    object,
    object,
    list[WorkerMessage],
]


def drain_worker_messages(
    result_queue: object,
    messages: list[WorkerMessage],
    *,
    wait_timeout: float = 0.0,
) -> None:
    if wait_timeout > 0:
        try:
            messages.append(result_queue.get(timeout=wait_timeout))
        except queue.Empty:
            pass
    while True:
        try:
            messages.append(result_queue.get_nowait())
        except queue.Empty:
            return


def raise_for_worker_failures(workers: list[WorkerRecord]) -> None:
    for label, process, result_queue, messages in workers:
        drain_worker_messages(result_queue, messages)
        failures = [
            payload for status, payload in messages if status == "error"
        ]
        if failures:
            raise AssertionError(
                f"{label} worker failed:\n" + "\n".join(map(str, failures))
            )
        if process.exitcode not in (None, 0):
            drain_worker_messages(
                result_queue,
                messages,
                wait_timeout=1.0,
            )
            failures = [
                payload for status, payload in messages if status == "error"
            ]
            detail = (
                "\n".join(map(str, failures))
                if failures
                else "No traceback was received from the worker queue."
            )
            raise AssertionError(
                f"{label} worker exited with code {process.exitcode}:\n{detail}"
            )


def wait_for_worker_event(
    event: object,
    worker: WorkerRecord,
    *,
    description: str,
    timeout: float = 30.0,
) -> None:
    deadline = time.monotonic() + timeout
    while not event.is_set():
        raise_for_worker_failures([worker])
        label, process, result_queue, messages = worker
        if process.exitcode is not None:
            drain_worker_messages(
                result_queue,
                messages,
                wait_timeout=1.0,
            )
            raise_for_worker_failures([worker])
            raise AssertionError(
                f"{label} worker exited with code {process.exitcode} "
                f"before {description}; messages: {messages!r}"
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssertionError(
                f"Timed out waiting for {description}; "
                f"{label} exitcode={process.exitcode}, messages={messages!r}"
            )
        event.wait(timeout=min(0.1, remaining))


def wait_for_control_message(
    control_queue: object,
    workers: list[WorkerRecord],
    *,
    description: str,
    timeout: float = 30.0,
) -> object:
    deadline = time.monotonic() + timeout
    while True:
        raise_for_worker_failures(workers)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            states = ", ".join(
                f"{label} exitcode={process.exitcode}, messages={messages!r}"
                for label, process, _, messages in workers
            )
            raise AssertionError(
                f"Timed out waiting for {description}; {states}"
            )
        try:
            return control_queue.get(timeout=min(0.1, remaining))
        except queue.Empty:
            continue


def wait_for_workers(
    workers: list[WorkerRecord],
    *,
    timeout: float = 30.0,
) -> None:
    deadline = time.monotonic() + timeout
    while any(process.is_alive() for _, process, _, _ in workers):
        raise_for_worker_failures(workers)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            states = ", ".join(
                f"{label} exitcode={process.exitcode}"
                for label, process, _, _ in workers
            )
            raise AssertionError(
                f"Timed out waiting for workers to finish; {states}"
            )
        for _, process, _, _ in workers:
            process.join(timeout=min(0.1, remaining))

    for _, process, result_queue, messages in workers:
        process.join(timeout=0)
        drain_worker_messages(
            result_queue,
            messages,
            wait_timeout=1.0 if not messages else 0.0,
        )
    raise_for_worker_failures(workers)


def stop_started_processes(
    processes: list[tuple[str, object]],
    *,
    timeout: float = 2.0,
) -> list[str]:
    errors: list[str] = []
    for label, process in processes:
        process.join(timeout=timeout)
        if process.is_alive():
            process.terminate()
            process.join(timeout=timeout)
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join(timeout=timeout)
        if process.is_alive():
            errors.append(f"{label} worker remained active after kill().")
        elif process.exitcode is None:
            errors.append(f"{label} worker has no exitcode after teardown.")
    return errors


def close_process_resources(
    processes: list[tuple[str, object]],
    queues: list[tuple[str, object]],
) -> list[str]:
    errors: list[str] = []
    for label, result_queue in queues:
        try:
            result_queue.close()
            result_queue.join_thread()
        except (OSError, ValueError) as error:
            errors.append(f"Unable to close {label} queue: {error}")
    for label, process in processes:
        try:
            process.close()
        except (OSError, ValueError) as error:
            errors.append(f"Unable to close {label} process: {error}")
    return errors


def worker_failure_details(workers: list[WorkerRecord]) -> list[str]:
    details: list[str] = []
    for label, _, result_queue, messages in workers:
        drain_worker_messages(
            result_queue,
            messages,
            wait_timeout=0.2,
        )
        for status, payload in messages:
            if status == "error":
                details.append(f"{label} worker traceback:\n{payload}")
    return details


class SyntheticDataGeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._default_output_state = {
            path: path.read_bytes() if path.is_file() else None
            for path in (
                DEFAULT_OUTPUT_DIR / relative for relative in EXPECTED_FILES
            )
        }
        cls._temporary_directory = tempfile.TemporaryDirectory()
        cls.output_root = Path(cls._temporary_directory.name) / "generated"
        cls.summary = generate_dataset(cls.output_root)
        cls.customers = read_csv(
            cls.output_root / "run_1/customers.csv"
        )[1]
        cls.products = read_csv(
            cls.output_root / "run_1/products.csv"
        )[1]
        cls.run_1 = read_csv(cls.output_root / "run_1/orders.csv")[1]
        cls.run_2 = read_csv(cls.output_root / "run_2/orders.csv")[1]
        cls.run_3 = read_csv(cls.output_root / "run_3/orders.csv")[1]

    @classmethod
    def tearDownClass(cls) -> None:
        cls._temporary_directory.cleanup()
        current_state = {
            path: path.read_bytes() if path.is_file() else None
            for path in cls._default_output_state
        }
        if current_state != cls._default_output_state:
            raise AssertionError("Tests wrote to the repository data directory.")

    def test_contract_metrics_are_loaded_from_normative_yaml(self) -> None:
        self.assertEqual(RUN_1_COUNT, 3_000)
        self.assertEqual(RUN_2_COUNT, 2_000)
        self.assertEqual(RUN_3_COUNT, 15)
        self.assertEqual(RUN_3_INSERTS, 10)
        self.assertEqual(RUN_3_CORRECTIONS, 5)
        self.assertEqual(FINAL_DISTINCT, 5_010)

    def test_default_generation_and_exact_file_set(self) -> None:
        self.assertEqual(self.summary.seed, DEFAULT_SEED)
        actual_files = {
            path.relative_to(self.output_root)
            for path in self.output_root.rglob("*")
            if path.is_file()
        }
        self.assertEqual(actual_files, set(EXPECTED_FILES))

    def test_schemas_and_column_order_use_independent_oracle(self) -> None:
        for relative_path, columns in EXPECTED_SCHEMAS.items():
            actual_columns, _ = read_csv(self.output_root / relative_path)
            self.assertEqual(actual_columns, columns)

    def test_run_counts_and_key_relationships(self) -> None:
        self.assertEqual(len(self.run_1), RUN_1_COUNT)
        self.assertEqual(len(self.run_2), RUN_2_COUNT)
        self.assertEqual(len(self.run_3), RUN_3_COUNT)
        run_1_keys = {row["order_line_id"] for row in self.run_1}
        run_2_keys = {row["order_line_id"] for row in self.run_2}
        run_3_keys = {row["order_line_id"] for row in self.run_3}
        self.assertEqual(len(run_1_keys), RUN_1_COUNT)
        self.assertEqual(len(run_2_keys), RUN_2_COUNT)
        self.assertEqual(len(run_3_keys), RUN_3_COUNT)
        self.assertEqual(len(run_1_keys | run_2_keys), 5_000)
        self.assertFalse(run_1_keys & run_2_keys)
        self.assertEqual(len(run_3_keys - run_1_keys - run_2_keys), 10)
        self.assertEqual(len(run_3_keys & run_1_keys), 5)
        self.assertFalse(run_3_keys & run_2_keys)

    def test_corrections_match_complete_run_1_records(self) -> None:
        originals = {row["order_line_id"]: row for row in self.run_1}
        run_2_keys = {row["order_line_id"] for row in self.run_2}
        corrections = [
            row for row in self.run_3 if row["order_line_id"] in originals
        ]
        self.assertEqual(
            {row["order_line_id"] for row in corrections},
            EXPECTED_CORRECTION_KEYS,
        )
        unchanged_columns = (
            "order_line_id",
            "order_id",
            "customer_id",
            "product_id",
            "order_status",
            "order_date",
        )
        for correction in corrections:
            original = originals[correction["order_line_id"]]
            self.assertNotIn(correction["order_line_id"], run_2_keys)
            self.assertEqual(tuple(correction), ORDER_EVOLVED_COLUMNS)
            self.assertGreater(
                parse_timestamp(correction["updated_at"]),
                parse_timestamp(original["updated_at"]),
            )
            self.assertTrue(
                correction["quantity"] != original["quantity"]
                or correction["unit_price"] != original["unit_price"]
            )
            for column in unchanged_columns:
                self.assertEqual(correction[column], original[column])
            self.assertEqual(correction["sales_channel"], "")

    def test_run_3_upsert_is_idempotent(self) -> None:
        current = {
            row["order_line_id"]: dict(row)
            for row in (*self.run_1, *self.run_2)
        }
        first_result = apply_run(current, self.run_3)
        first_state = {
            key: tuple(row.get(column, "") for column in ORDER_EVOLVED_COLUMNS)
            for key, row in current.items()
        }
        second_result = apply_run(current, self.run_3)
        second_state = {
            key: tuple(row.get(column, "") for column in ORDER_EVOLVED_COLUMNS)
            for key, row in current.items()
        }
        self.assertEqual(first_result, (10, 5, 0))
        self.assertEqual(second_result, (0, 0, 15))
        self.assertEqual(len(current), FINAL_DISTINCT)
        self.assertEqual(first_state, second_state)

    def test_customer_versions_and_equal_timestamp_tie_break(self) -> None:
        counts = Counter(row["customer_id"] for row in self.customers)
        self.assertEqual(len(counts), 500)
        self.assertEqual(sum(count > 1 for count in counts.values()), 10)
        for customer_id, count in counts.items():
            if count == 1:
                continue
            versions = [
                row
                for row in self.customers
                if row["customer_id"] == customer_id
            ]
            current = max(
                versions,
                key=lambda row: (
                    parse_timestamp(row["updated_at"]),
                    row["customer_name"],
                    row["email"],
                    row["country_code"],
                ),
            )
            self.assertEqual(
                generator_module._select_current_customer(versions),
                current,
            )

        timestamp = "2025-01-01T00:00:00.000000Z"
        lower_payload = {
            "customer_id": "CUS_TEST",
            "customer_name": "Synthetic Alpha",
            "email": "alpha@example.test",
            "country_code": "IT",
            "updated_at": timestamp,
        }
        higher_payload = {
            **lower_payload,
            "customer_name": "Synthetic Zeta",
        }
        selected = generator_module._select_current_customer(
            [lower_payload, higher_payload]
        )
        self.assertEqual(selected, higher_payload)

    def test_current_customer_name_does_not_require_updated_marker(self) -> None:
        generated = generator_module._build_generated_files(12)
        customer_rows = generated[Path("run_1/customers.csv")]
        versions = [
            row
            for row in customer_rows
            if row["customer_id"] == "CUS_000001"
        ]
        current = max(versions, key=lambda row: parse_timestamp(row["updated_at"]))
        current["customer_name"] = "Synthetic Current Customer"

        generator_module._validate_generated_data(generated)

        selected = generator_module._select_current_customer(versions)
        self.assertEqual(selected, current)
        self.assertNotIn("Updated", selected["customer_name"])

    def test_monetary_timestamp_and_newline_serialization(self) -> None:
        for relative_path in EXPECTED_FILES:
            content = (self.output_root / relative_path).read_bytes()
            self.assertNotIn(b"\r", content)
            self.assertTrue(content.endswith(b"\n"))

        order_rows = self.run_1 + self.run_2 + self.run_3
        for row in order_rows:
            if row["unit_price"]:
                self.assertRegex(row["unit_price"], DECIMAL_PATTERN)
                self.assertNotIn("e", row["unit_price"].lower())
                self.assertEqual(
                    Decimal(row["unit_price"]).as_tuple().exponent, -2
                )
            self.assertRegex(row["updated_at"], TIMESTAMP_PATTERN)
            parse_timestamp(row["updated_at"])
        for row in self.customers:
            self.assertRegex(row["updated_at"], TIMESTAMP_PATTERN)
            parse_timestamp(row["updated_at"])

    def test_run_date_ranges_and_late_arrival_timestamps(self) -> None:
        run_1_min = date.fromisoformat(
            RUN_METRICS["run_1"]["order_date"]["min"]
        )
        run_1_max = date.fromisoformat(
            RUN_METRICS["run_1"]["order_date"]["max"]
        )
        run_2_min = date.fromisoformat(
            RUN_METRICS["run_2"]["order_date"]["min"]
        )
        run_2_max = date.fromisoformat(
            RUN_METRICS["run_2"]["order_date"]["max"]
        )
        self.assertTrue(
            all(
                run_1_min <= date.fromisoformat(row["order_date"]) <= run_1_max
                for row in self.run_1
            )
        )
        self.assertTrue(
            all(
                run_2_min <= date.fromisoformat(row["order_date"]) <= run_2_max
                for row in self.run_2
            )
        )
        first_two_run_keys = {
            row["order_line_id"] for row in (*self.run_1, *self.run_2)
        }
        late_inserts = [
            row
            for row in self.run_3
            if row["order_line_id"] not in first_two_run_keys
        ]
        latest_run_2_update = max(
            parse_timestamp(row["updated_at"]) for row in self.run_2
        )
        self.assertEqual(len(late_inserts), RUN_3_INSERTS)
        for row in late_inserts:
            self.assertGreater(
                parse_timestamp(row["updated_at"]), latest_run_2_update
            )
            self.assertGreaterEqual(
                date.fromisoformat(row["order_date"]), run_1_min
            )
            self.assertLessEqual(
                date.fromisoformat(row["order_date"]), run_1_max
            )
            self.assertTrue(
                row["sales_channel"] == ""
                or row["sales_channel"] in ALLOWED_SALES_CHANNELS
            )

    def test_quality_scenarios_are_separate_in_run_1_and_run_2(self) -> None:
        for rows in (self.run_1, self.run_2):
            scenarios = quality_keys(rows)
            self.assertTrue(all(scenarios.values()))
            all_keys = [
                key for keys in scenarios.values() for key in keys
            ]
            self.assertEqual(len(all_keys), len(set(all_keys)))

    def test_master_data_counts_and_synthetic_values(self) -> None:
        self.assertEqual(len(self.customers), 510)
        self.assertEqual(len(self.products), 100)
        self.assertTrue(
            all(
                row["email"].endswith("@example.test")
                for row in self.customers
            )
        )
        self.assertTrue(
            all(
                row["product_name"].startswith("Synthetic ")
                for row in self.products
            )
        )

    def test_same_seed_is_byte_identical(self) -> None:
        with tempfile.TemporaryDirectory() as first_directory:
            with tempfile.TemporaryDirectory() as second_directory:
                first_root = Path(first_directory) / "output"
                second_root = Path(second_directory) / "output"
                generate_dataset(first_root, seed=77)
                generate_dataset(second_root, seed=77)
                self.assertEqual(
                    file_hashes(first_root), file_hashes(second_root)
                )

    def test_different_seed_changes_all_files_without_contract_drift(self) -> None:
        with tempfile.TemporaryDirectory() as first_directory:
            with tempfile.TemporaryDirectory() as second_directory:
                first_root = Path(first_directory) / "output"
                second_root = Path(second_directory) / "output"
                generate_dataset(first_root, seed=77)
                generate_dataset(second_root, seed=78)
                first_hashes = file_hashes(first_root)
                second_hashes = file_hashes(second_root)
                self.assertTrue(
                    all(
                        first_hashes[path] != second_hashes[path]
                        for path in EXPECTED_FILES
                    )
                )
                for relative_path, expected_schema in EXPECTED_SCHEMAS.items():
                    first_schema, first_rows = read_csv(
                        first_root / relative_path
                    )
                    second_schema, second_rows = read_csv(
                        second_root / relative_path
                    )
                    self.assertEqual(first_schema, expected_schema)
                    self.assertEqual(second_schema, expected_schema)
                    self.assertEqual(len(first_rows), len(second_rows))

    def test_force_replaces_all_managed_files_and_preserves_foreign_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_root = Path(directory) / "output"
            generate_dataset(output_root, seed=77)
            before = file_hashes(output_root)
            foreign_root_file = output_root / "notes.txt"
            foreign_nested_file = output_root / "run_2/foreign.csv"
            foreign_root_file.write_text("preserve root", encoding="utf-8")
            foreign_nested_file.write_text("preserve nested", encoding="utf-8")
            with self.assertRaises(OutputExistsError):
                generate_dataset(output_root)

            generate_dataset(output_root, seed=78, force=True)
            after = file_hashes(output_root)
            self.assertTrue(
                all(before[path] != after[path] for path in EXPECTED_FILES)
            )
            self.assertEqual(
                foreign_root_file.read_text(encoding="utf-8"),
                "preserve root",
            )
            self.assertEqual(
                foreign_nested_file.read_text(encoding="utf-8"),
                "preserve nested",
            )

    def test_concurrent_force_publications_are_serialized_per_output_root(
        self,
    ) -> None:
        context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            output_root = base / "shared"
            expected_a = base / "expected-a"
            expected_b = base / "expected-b"
            generate_dataset(output_root, seed=70)
            generate_dataset(expected_a, seed=71)
            generate_dataset(expected_b, seed=72)
            expected_hashes = (file_hashes(expected_a), file_hashes(expected_b))

            first_promotion_completed = context.Event()
            resume_publication = context.Event()
            lock_probe_results = context.Queue()
            process_a_results = context.Queue()
            process_b_results = context.Queue()
            process_a = context.Process(
                target=run_paused_generation,
                args=(
                    str(output_root),
                    71,
                    first_promotion_completed,
                    resume_publication,
                    process_a_results,
                ),
            )
            process_b = context.Process(
                target=run_generation_after_lock_probe,
                args=(
                    str(output_root),
                    72,
                    lock_probe_results,
                    process_b_results,
                ),
            )
            process_a_messages: list[WorkerMessage] = []
            process_b_messages: list[WorkerMessage] = []
            workers = [
                (
                    "seed A",
                    process_a,
                    process_a_results,
                    process_a_messages,
                ),
                (
                    "seed B",
                    process_b,
                    process_b_results,
                    process_b_messages,
                ),
            ]
            processes = [("seed A", process_a), ("seed B", process_b)]
            queues = [
                ("lock probe", lock_probe_results),
                ("seed A result", process_a_results),
                ("seed B result", process_b_results),
            ]
            started_processes: list[tuple[str, object]] = []
            primary_failure: str | None = None
            cleanup_errors: list[str] = []
            worker_errors: list[str] = []
            try:
                process_a.start()
                started_processes.append(("seed A", process_a))
                wait_for_worker_event(
                    first_promotion_completed,
                    workers[0],
                    description="seed A to complete its first promotion",
                )

                process_b.start()
                started_processes.append(("seed B", process_b))
                self.assertEqual(
                    wait_for_control_message(
                        lock_probe_results,
                        workers,
                        description="seed B to probe the occupied lock",
                    ),
                    "blocked",
                    "Seed B acquired the lock while seed A was publishing.",
                )
                resume_publication.set()
                wait_for_workers(workers)

                self.assertEqual(process_a.exitcode, 0)
                self.assertEqual(process_b.exitcode, 0)
                self.assertEqual(process_a_messages, [("ok", 71)])
                self.assertEqual(process_b_messages, [("ok", 72)])

                final_hashes = file_hashes(output_root)
                self.assertIn(final_hashes, expected_hashes)
                self.assertEqual(
                    {
                        path.relative_to(output_root)
                        for path in output_root.rglob("*")
                        if path.is_file()
                    },
                    set(EXPECTED_FILES),
                )
                self.assertEqual(
                    list(output_root.glob(".synthetic-data-*")),
                    [],
                )
            except BaseException:
                primary_failure = traceback.format_exc()
            finally:
                resume_publication.set()
                cleanup_errors.extend(
                    stop_started_processes(started_processes)
                )
                worker_errors.extend(worker_failure_details(workers))
                cleanup_errors.extend(
                    close_process_resources(processes, queues)
                )

            failures = [
                detail
                for detail in (
                    primary_failure,
                    *worker_errors,
                    *cleanup_errors,
                )
                if detail
            ]
            if failures:
                self.fail("\n\n".join(failures))

    def test_different_output_roots_do_not_share_a_lock(self) -> None:
        context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            paused_root = base / "paused"
            independent_root = base / "independent"
            generate_dataset(paused_root, seed=70)
            first_promotion_completed = context.Event()
            resume_publication = context.Event()
            paused_results = context.Queue()
            independent_results = context.Queue()
            paused_process = context.Process(
                target=run_paused_generation,
                args=(
                    str(paused_root),
                    71,
                    first_promotion_completed,
                    resume_publication,
                    paused_results,
                ),
            )
            independent_process = context.Process(
                target=run_generation,
                args=(str(independent_root), 72, independent_results),
            )
            paused_messages: list[WorkerMessage] = []
            independent_messages: list[WorkerMessage] = []
            workers = [
                (
                    "paused root",
                    paused_process,
                    paused_results,
                    paused_messages,
                ),
                (
                    "independent root",
                    independent_process,
                    independent_results,
                    independent_messages,
                ),
            ]
            processes = [
                ("paused root", paused_process),
                ("independent root", independent_process),
            ]
            queues = [
                ("paused root result", paused_results),
                ("independent root result", independent_results),
            ]
            started_processes: list[tuple[str, object]] = []
            primary_failure: str | None = None
            cleanup_errors: list[str] = []
            worker_errors: list[str] = []
            try:
                paused_process.start()
                started_processes.append(("paused root", paused_process))
                wait_for_worker_event(
                    first_promotion_completed,
                    workers[0],
                    description="the paused root to complete its first promotion",
                )

                independent_process.start()
                started_processes.append(
                    ("independent root", independent_process)
                )
                wait_for_workers([workers[1]])
                raise_for_worker_failures(workers)

                self.assertEqual(independent_process.exitcode, 0)
                self.assertEqual(independent_messages, [("ok", 72)])

                resume_publication.set()
                wait_for_workers([workers[0]])
                self.assertEqual(paused_process.exitcode, 0)
                self.assertEqual(paused_messages, [("ok", 71)])
                self.assertEqual(
                    list(paused_root.glob(".synthetic-data-*")),
                    [],
                )
                self.assertEqual(
                    list(independent_root.glob(".synthetic-data-*")),
                    [],
                )
            except BaseException:
                primary_failure = traceback.format_exc()
            finally:
                resume_publication.set()
                cleanup_errors.extend(
                    stop_started_processes(started_processes)
                )
                worker_errors.extend(worker_failure_details(workers))
                cleanup_errors.extend(
                    close_process_resources(processes, queues)
                )

            failures = [
                detail
                for detail in (
                    primary_failure,
                    *worker_errors,
                    *cleanup_errors,
                )
                if detail
            ]
            if failures:
                self.fail("\n\n".join(failures))

    def test_output_lock_is_released_when_holder_process_terminates(self) -> None:
        context = multiprocessing.get_context("fork")
        with tempfile.TemporaryDirectory() as directory:
            output_root = Path(directory) / "output"
            lock_acquired = context.Event()
            release_lock = context.Event()
            holder = context.Process(
                target=hold_output_lock,
                args=(str(output_root), lock_acquired, release_lock),
            )

            holder.start()
            self.assertTrue(lock_acquired.wait(timeout=30))
            holder.terminate()
            holder.join(timeout=30)
            self.assertFalse(holder.is_alive())

            generate_dataset(output_root, seed=73)
            self.assertEqual(
                {
                    path.relative_to(output_root)
                    for path in output_root.rglob("*")
                    if path.is_file()
                },
                set(EXPECTED_FILES),
            )
            self.assertEqual(list(output_root.glob(".synthetic-data-*")), [])

    def test_missing_fcntl_fails_without_publishing(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_root = Path(directory) / "output"
            with patch.object(generator_module, "fcntl", None):
                with self.assertRaisesRegex(
                    GenerationError,
                    "Concurrent output protection is unavailable",
                ):
                    generate_dataset(output_root, seed=73)

            self.assertEqual(list(output_root.rglob("*.csv")), [])
            self.assertEqual(list(output_root.glob(".synthetic-data-*")), [])

    def test_publication_failure_rolls_back_existing_files_byte_for_byte(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_root = Path(directory) / "output"
            generate_dataset(output_root, seed=77)
            before = file_hashes(output_root)
            real_replace = generator_module.os.replace
            promotion_calls = 0

            def fail_second_promotion(source: object, target: object) -> None:
                nonlocal promotion_calls
                if "staged" in Path(source).parts:
                    promotion_calls += 1
                    if promotion_calls == 2:
                        raise OSError("simulated publication failure")
                real_replace(source, target)

            with patch.object(
                generator_module.os,
                "replace",
                side_effect=fail_second_promotion,
            ):
                with self.assertRaisesRegex(
                    GenerationError, "previous output state was restored"
                ):
                    generate_dataset(output_root, seed=78, force=True)

            self.assertEqual(file_hashes(output_root), before)
            self.assertEqual(list(output_root.glob(".synthetic-data-*")), [])

    def test_publication_failure_leaves_empty_destination_without_csv(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_root = Path(directory) / "output"
            output_root.mkdir()
            foreign_file = output_root / "notes.txt"
            foreign_file.write_text("preserve", encoding="utf-8")
            real_replace = generator_module.os.replace
            promotion_calls = 0

            def fail_second_promotion(source: object, target: object) -> None:
                nonlocal promotion_calls
                if "staged" in Path(source).parts:
                    promotion_calls += 1
                    if promotion_calls == 2:
                        raise OSError("simulated publication failure")
                real_replace(source, target)

            with patch.object(
                generator_module.os,
                "replace",
                side_effect=fail_second_promotion,
            ):
                with self.assertRaisesRegex(
                    GenerationError, "previous output state was restored"
                ):
                    generate_dataset(output_root, seed=78)

            self.assertEqual(list(output_root.rglob("*.csv")), [])
            self.assertEqual(
                foreign_file.read_text(encoding="utf-8"), "preserve"
            )
            self.assertEqual(list(output_root.glob(".synthetic-data-*")), [])

    def test_rollback_failure_reports_and_retains_recovery_data(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_root = Path(directory) / "output"
            generate_dataset(output_root, seed=77)
            real_replace = generator_module.os.replace
            promotion_calls = 0
            rollback_failed = False

            def fail_publication_and_rollback(
                source: object, target: object
            ) -> None:
                nonlocal promotion_calls, rollback_failed
                source_path = Path(source)
                if "staged" in source_path.parts:
                    promotion_calls += 1
                    if promotion_calls == 2:
                        raise OSError("simulated publication failure")
                if "backup" in source_path.parts and not rollback_failed:
                    rollback_failed = True
                    raise OSError("simulated rollback failure")
                real_replace(source, target)

            with patch.object(
                generator_module.os,
                "replace",
                side_effect=fail_publication_and_rollback,
            ):
                with self.assertRaises(GenerationError) as raised:
                    generate_dataset(output_root, seed=78, force=True)

            recovery_match = re.search(
                r"Recovery data was retained at (.+)\.$",
                str(raised.exception),
            )
            self.assertIsNotNone(recovery_match)
            recovery_path = Path(recovery_match.group(1))
            self.assertTrue(recovery_path.is_dir())
            self.assertEqual(recovery_path.parent, output_root.resolve())
            self.assertTrue(
                recovery_path.name.startswith(".synthetic-data-")
            )
            self.assertTrue(
                (recovery_path / "backup/run_3/orders.csv").is_file()
            )
            self.assertTrue(
                (recovery_path / "staged/run_1/products.csv").is_file()
            )
            transaction_directories = list(
                output_root.glob(".synthetic-data-*")
            )
            self.assertEqual(len(transaction_directories), 1)
            self.assertTrue(transaction_directories[0].samefile(recovery_path))

    def test_output_root_existing_as_file_has_controlled_error(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output_file = Path(directory) / "output"
            output_file.write_text("not a directory", encoding="utf-8")
            with self.assertRaisesRegex(
                GenerationError, "Unable to prepare output directory"
            ):
                generate_dataset(output_file)

            result = subprocess.run(
                [
                    sys.executable,
                    str(CLI_PATH),
                    "--output-dir",
                    str(output_file),
                ],
                cwd=REPOSITORY_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Generation failed:", result.stderr)
            self.assertNotIn("Traceback", result.stderr)

    def test_validation_rejects_invalid_master_data(self) -> None:
        mutations = (
            ("customer_id", " "),
            ("customer_name", ""),
            ("email", "invalid email@example.test"),
            ("country_code", "ita"),
            ("updated_at", "2025-01-01T00:00:00.00000Z"),
        )
        customer_path = Path("run_1/customers.csv")
        for field_name, invalid_value in mutations:
            with self.subTest(dataset="customers", field=field_name):
                generated = generator_module._build_generated_files(12)
                generated[customer_path][20][field_name] = invalid_value
                with self.assertRaises(GenerationError):
                    generator_module._validate_generated_data(generated)

        product_path = Path("run_1/products.csv")
        for field_name in PRODUCT_COLUMNS:
            with self.subTest(dataset="products", field=field_name):
                generated = generator_module._build_generated_files(12)
                generated[product_path][20][field_name] = ""
                with self.assertRaises(GenerationError):
                    generator_module._validate_generated_data(generated)

    def test_validation_rejects_invalid_decimal_representations(self) -> None:
        order_path = Path("run_1/orders.csv")
        invalid_values = (
            "1.0",
            "1.000",
            "1e2",
            "12345678901.00",
            "+1.00",
        )
        for invalid_value in invalid_values:
            with self.subTest(unit_price=invalid_value):
                generated = generator_module._build_generated_files(12)
                generated[order_path][20]["unit_price"] = invalid_value
                with self.assertRaisesRegex(
                    GenerationError, "DECIMAL\\(12,2\\)"
                ):
                    generator_module._validate_generated_data(generated)

    def test_validation_rejects_temporal_and_correction_drift(self) -> None:
        run_1_path = Path("run_1/orders.csv")
        run_2_path = Path("run_2/orders.csv")
        run_3_path = Path("run_3/orders.csv")

        generated = generator_module._build_generated_files(12)
        generated[run_1_path][20]["order_date"] = "2025-01-11"
        with self.assertRaisesRegex(GenerationError, "run range"):
            generator_module._validate_generated_data(generated)

        generated = generator_module._build_generated_files(12)
        generated[run_2_path][20]["order_date"] = "2025-01-10"
        with self.assertRaisesRegex(GenerationError, "run range"):
            generator_module._validate_generated_data(generated)

        generated = generator_module._build_generated_files(12)
        generated[run_3_path][0][
            "updated_at"
        ] = "2025-01-20T00:00:00.000000Z"
        with self.assertRaisesRegex(GenerationError, "later than Run 2"):
            generator_module._validate_generated_data(generated)

        generated = generator_module._build_generated_files(12)
        generated[run_3_path][-1]["order_id"] = "ORD_CHANGED"
        with self.assertRaisesRegex(
            GenerationError, "unexpectedly changes order_id"
        ):
            generator_module._validate_generated_data(generated)

    def test_cli_help_generation_and_existing_output_error(self) -> None:
        help_result = subprocess.run(
            [sys.executable, str(CLI_PATH), "--help"],
            cwd=REPOSITORY_ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        self.assertEqual(help_result.returncode, 0, help_result.stderr)
        self.assertIn("--seed", help_result.stdout)
        self.assertIn("--output-dir", help_result.stdout)
        self.assertIn("--force", help_result.stdout)

        with tempfile.TemporaryDirectory() as directory:
            output_root = Path(directory) / "output"
            result = subprocess.run(
                [
                    sys.executable,
                    str(CLI_PATH),
                    "--seed",
                    "123",
                    "--output-dir",
                    str(output_root),
                ],
                cwd=REPOSITORY_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn("seed 123", result.stdout)
            self.assertEqual(
                {
                    path.relative_to(output_root)
                    for path in output_root.rglob("*.csv")
                },
                set(EXPECTED_FILES),
            )
            second_result = subprocess.run(
                [
                    sys.executable,
                    str(CLI_PATH),
                    "--output-dir",
                    str(output_root),
                ],
                cwd=REPOSITORY_ROOT,
                text=True,
                capture_output=True,
                check=False,
            )
            self.assertNotEqual(second_result.returncode, 0)
            self.assertIn("Use --force", second_result.stderr)
            self.assertNotIn("Traceback", second_result.stderr)
