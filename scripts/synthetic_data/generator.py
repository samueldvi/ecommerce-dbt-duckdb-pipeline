"""Generate deterministic CSV inputs that conform to the approved contracts."""

from __future__ import annotations

import csv
import hashlib
import os
import random
import re
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from filelock import FileLock, Timeout

DEFAULT_SEED = 20250730
CUSTOMER_COUNT = 200
# The approved contract permits three or four versioned customers. Four keeps
# the scenario deterministic while exercising the upper accepted boundary.
CUSTOMER_VERSION_COUNT = 4
PRODUCT_COUNT = 80
RUN_1_ORDER_COUNT = 3_000
RUN_2_ORDER_COUNT = 1_000
RUN_3_ORDER_COUNT = 1_000
RUN_3_LATE_INSERT_COUNT = 10
RUN_3_CORRECTION_COUNT = 5
MIN_MULTI_LINE_ORDER_RATIO = Decimal("0.15")
EXPECTED_QUALITY_SCENARIO_OCCURRENCES = 5

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_DIR = REPOSITORY_ROOT / "data" / "raw"

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

MANAGED_FILE_SCHEMAS: Mapping[Path, tuple[str, ...]] = {
    Path("run_1/customers.csv"): CUSTOMER_COLUMNS,
    Path("run_1/products.csv"): PRODUCT_COLUMNS,
    Path("run_1/orders.csv"): ORDER_BASE_COLUMNS,
    Path("run_2/orders.csv"): ORDER_BASE_COLUMNS,
    Path("run_3/orders.csv"): ORDER_EVOLVED_COLUMNS,
    Path("run_3/late_arriving/orders.csv"): ORDER_BASE_COLUMNS,
}

ALLOWED_ORDER_STATUSES = frozenset({"completed", "cancelled"})
ALLOWED_SALES_CHANNELS = frozenset({"web", "mobile", "marketplace"})
INVALID_ORDER_STATUS = "pending_review"
ORPHAN_CUSTOMER_ID = "CUS_ORPHAN"
ORPHAN_PRODUCT_ID = "PRD_ORPHAN"
NULL_VALUE = ""

RUN_1_START_DATE = date(2025, 1, 1)
RUN_2_START_DATE = date(2025, 1, 11)
RUN_3_START_DATE = date(2025, 1, 21)
LATE_ARRIVAL_TIME = datetime(2025, 1, 20, 18, 0, tzinfo=timezone.utc)
CORRECTION_LINE_NUMBERS = (100, 500, 1_000, 2_000, 3_000)
_ORDER_LINE_COUNT_PATTERN = (1, 1, 1, 1, 2)

_FIRST_NAMES = ("Alex", "Casey", "Jordan", "Morgan", "Riley", "Taylor")
_LAST_NAMES = ("Alpha", "Beta", "Delta", "Gamma", "Kappa", "Sigma")
_COUNTRY_CODES = ("DE", "ES", "FR", "IT", "NL", "PT")
_PRODUCT_CATEGORIES = ("Accessories", "Audio", "Computing", "Home", "Mobile")
_PRODUCT_TYPES = ("Adapter", "Headset", "Keyboard", "Monitor", "Speaker")
_DATE_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}")
_TIMESTAMP_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
_DECIMAL_PATTERN = re.compile(r"-?\d{1,10}\.\d{2}")
_INTEGER_PATTERN = re.compile(r"-?(?:0|[1-9]\d*)")
_COUNTRY_CODE_PATTERN = re.compile(r"[A-Z]{2}")
_TRANSACTION_PREFIX = ".synthetic-data-"
_LOCK_DIRECTORY_NAME = "ecommerce-dbt-duckdb-pipeline-locks"
_LOCK_TIMEOUT_SECONDS = 120


class GenerationError(RuntimeError):
    """Raised when generation or validation cannot complete safely."""


class OutputExistsError(GenerationError):
    """Raised when a managed output exists without replacement authorization."""


@dataclass(frozen=True)
class GenerationSummary:
    """Observable result of a successful generation."""

    seed: int
    output_dir: Path
    row_counts: Mapping[Path, int]


CsvRow = dict[str, str]
GeneratedFiles = dict[Path, list[CsvRow]]


def generate_dataset(
    output_dir: Path | str = DEFAULT_OUTPUT_DIR,
    *,
    seed: int = DEFAULT_SEED,
    force: bool = False,
) -> GenerationSummary:
    """Generate, validate, and safely publish all managed CSV files."""

    destination = Path(output_dir).expanduser().resolve()
    generated = _build_generated_files(seed)
    _validate_generated_data(generated)
    _publish_generated_files(destination, generated, force=force)

    return GenerationSummary(
        seed=seed,
        output_dir=destination,
        row_counts={
            relative_path: len(rows) for relative_path, rows in generated.items()
        },
    )


def _publish_generated_files(
    destination: Path,
    generated: GeneratedFiles,
    *,
    force: bool,
) -> None:
    with _exclusive_output_lock(destination):
        try:
            destination.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise GenerationError(
                f"Unable to prepare output directory {destination}: {error}"
            ) from error
        _publish_generated_files_under_lock(
            destination,
            generated,
            force=force,
        )


def _output_lock_path(destination: Path) -> Path:
    normalized_destination = os.path.normcase(str(destination.resolve(strict=False)))
    destination_digest = hashlib.sha256(
        normalized_destination.encode("utf-8")
    ).hexdigest()
    return (
        Path(tempfile.gettempdir())
        / _LOCK_DIRECTORY_NAME
        / f"{destination_digest}.lock"
    )


@contextmanager
def _exclusive_output_lock(destination: Path) -> Iterable[None]:
    lock_path = _output_lock_path(destination)
    lock = FileLock(lock_path, timeout=_LOCK_TIMEOUT_SECONDS)
    try:
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        lock.acquire()
    except Timeout as error:
        raise GenerationError(
            f"Timed out waiting for the output lock for {destination}."
        ) from error
    except (OSError, ValueError) as error:
        raise GenerationError(
            f"Unable to acquire output lock for {destination}: {error}"
        ) from error

    try:
        yield
    finally:
        try:
            lock.release()
        except (OSError, ValueError) as error:
            raise GenerationError(
                f"Unable to release output lock for {destination}: {error}"
            ) from error


def _publish_generated_files_under_lock(
    destination: Path,
    generated: GeneratedFiles,
    *,
    force: bool,
) -> None:
    transaction_root: Path | None = None
    try:
        managed_destinations = {
            relative_path: destination / relative_path
            for relative_path in MANAGED_FILE_SCHEMAS
        }
        existing = [path for path in managed_destinations.values() if path.exists()]
        if existing and not force:
            paths = ", ".join(
                str(path.relative_to(destination)) for path in sorted(existing)
            )
            raise OutputExistsError(
                f"Managed output already exists: {paths}. Use --force to replace it."
            )
        non_files = [path for path in existing if not path.is_file()]
        if non_files:
            paths = ", ".join(str(path) for path in sorted(non_files))
            raise GenerationError(f"Managed output path is not a file: {paths}")
        for target in managed_destinations.values():
            target.parent.mkdir(parents=True, exist_ok=True)
        transaction_root = Path(
            tempfile.mkdtemp(prefix=_TRANSACTION_PREFIX, dir=destination)
        )
    except GenerationError:
        raise
    except OSError as error:
        raise GenerationError(
            f"Unable to prepare output directory {destination}: {error}"
        ) from error

    staging_root = transaction_root / "staged"
    try:
        _write_generated_files(staging_root, generated)
        _validate_written_files(staging_root)
    except (GenerationError, OSError) as error:
        cleanup_error = _try_remove_transaction_directory(transaction_root, destination)
        detail = f"Unable to stage generated files: {error}"
        if cleanup_error is not None:
            detail += (
                f". Temporary data could not be removed from "
                f"{transaction_root}: {cleanup_error}"
            )
        raise GenerationError(detail) from error

    _promote_transaction(
        destination=destination,
        staging_root=staging_root,
        transaction_root=transaction_root,
    )


def _promote_transaction(
    *,
    destination: Path,
    staging_root: Path,
    transaction_root: Path,
) -> None:
    relative_paths = tuple(MANAGED_FILE_SCHEMAS)
    backup_root = transaction_root / "backup"
    originally_existing = {
        relative_path
        for relative_path in relative_paths
        if (destination / relative_path).is_file()
    }
    backed_up: set[Path] = set()
    promoted: set[Path] = set()

    try:
        for relative_path in relative_paths:
            if relative_path not in originally_existing:
                continue
            backup = backup_root / relative_path
            backup.parent.mkdir(parents=True, exist_ok=True)
            os.replace(destination / relative_path, backup)
            backed_up.add(relative_path)

        for relative_path in relative_paths:
            os.replace(
                staging_root / relative_path,
                destination / relative_path,
            )
            promoted.add(relative_path)
    except OSError as publication_error:
        rollback_errors = _rollback_transaction(
            destination=destination,
            backup_root=backup_root,
            relative_paths=relative_paths,
            originally_existing=originally_existing,
            backed_up=backed_up,
            promoted=promoted,
        )
        if rollback_errors:
            details = "; ".join(rollback_errors)
            raise GenerationError(
                f"Unable to publish generated files: {publication_error}. "
                f"Rollback also failed: {details}. Recovery data was retained "
                f"at {transaction_root}."
            ) from publication_error

        cleanup_error = _try_remove_transaction_directory(transaction_root, destination)
        detail = (
            f"Unable to publish generated files: {publication_error}. "
            "The previous output state was restored."
        )
        if cleanup_error is not None:
            detail += (
                f" Transaction data could not be removed from "
                f"{transaction_root}: {cleanup_error}"
            )
        raise GenerationError(detail) from publication_error

    cleanup_error = _try_remove_transaction_directory(transaction_root, destination)
    if cleanup_error is not None:
        raise GenerationError(
            "Generated files were published, but transaction data could not "
            f"be removed from {transaction_root}: {cleanup_error}"
        )


def _rollback_transaction(
    *,
    destination: Path,
    backup_root: Path,
    relative_paths: Sequence[Path],
    originally_existing: set[Path],
    backed_up: set[Path],
    promoted: set[Path],
) -> list[str]:
    errors: list[str] = []
    for relative_path in reversed(relative_paths):
        target = destination / relative_path
        try:
            if relative_path in backed_up:
                os.replace(backup_root / relative_path, target)
            elif (
                relative_path in promoted
                and relative_path not in originally_existing
                and target.exists()
            ):
                target.unlink()
        except OSError as rollback_error:
            errors.append(f"{relative_path}: {rollback_error}")
    return errors


def _try_remove_transaction_directory(
    transaction_root: Path, destination: Path
) -> OSError | None:
    if transaction_root.parent != destination or not transaction_root.name.startswith(
        _TRANSACTION_PREFIX
    ):
        return OSError(f"Refusing to remove unsafe path {transaction_root}")
    try:
        shutil.rmtree(transaction_root)
    except OSError as error:
        return error
    return None


def _build_generated_files(seed: int) -> GeneratedFiles:
    rng = random.Random(seed)
    customers = _generate_customers(rng)
    products = _generate_products(rng)
    run_1_order_count = _order_count_for_line_count(RUN_1_ORDER_COUNT)
    run_2_order_count = _order_count_for_line_count(RUN_2_ORDER_COUNT)
    run_1_orders = _generate_order_run(
        rng,
        first_line_number=1,
        first_order_number=1,
        count=RUN_1_ORDER_COUNT,
        start_date=RUN_1_START_DATE,
        include_sales_channel=False,
        quality_scenario_occurrences=3,
    )
    run_2_orders = _generate_order_run(
        rng,
        first_line_number=RUN_1_ORDER_COUNT + 1,
        first_order_number=run_1_order_count + 1,
        count=RUN_2_ORDER_COUNT,
        start_date=RUN_2_START_DATE,
        include_sales_channel=False,
        quality_scenario_occurrences=1,
    )
    run_3_orders = _generate_order_run(
        rng,
        first_line_number=RUN_1_ORDER_COUNT + RUN_2_ORDER_COUNT + 1,
        first_order_number=run_1_order_count + run_2_order_count + 1,
        count=RUN_3_ORDER_COUNT,
        start_date=RUN_3_START_DATE,
        include_sales_channel=True,
        quality_scenario_occurrences=1,
    )
    late_arriving_orders = _generate_late_arriving(rng, run_1_orders)
    return {
        Path("run_1/customers.csv"): customers,
        Path("run_1/products.csv"): products,
        Path("run_1/orders.csv"): run_1_orders,
        Path("run_2/orders.csv"): run_2_orders,
        Path("run_3/orders.csv"): run_3_orders,
        Path("run_3/late_arriving/orders.csv"): late_arriving_orders,
    }


def _generate_customers(rng: random.Random) -> list[CsvRow]:
    rows: list[CsvRow] = []
    base_time = datetime(2025, 1, 1, 8, 0, tzinfo=timezone.utc)
    for number in range(1, CUSTOMER_COUNT + 1):
        first_name = rng.choice(_FIRST_NAMES)
        last_name = rng.choice(_LAST_NAMES)
        customer_id = _customer_id(number)
        rows.append(
            {
                "customer_id": customer_id,
                "customer_name": f"Synthetic {first_name} {last_name} {number:03d}",
                "email": f"customer{number:04d}@example.test",
                "country_code": rng.choice(_COUNTRY_CODES),
                "updated_at": _format_timestamp(base_time + timedelta(seconds=number)),
            }
        )

    for number in range(1, CUSTOMER_VERSION_COUNT + 1):
        original = rows[number - 1]
        updated = dict(original)
        updated["customer_name"] = f"{original['customer_name']} Updated"
        updated["email"] = f"customer{number:04d}.updated@example.test"
        updated["updated_at"] = _format_timestamp(
            base_time + timedelta(days=1, seconds=number)
        )
        rows.append(updated)
    return rows


def _generate_products(rng: random.Random) -> list[CsvRow]:
    rows: list[CsvRow] = []
    for number in range(1, PRODUCT_COUNT + 1):
        product_type = rng.choice(_PRODUCT_TYPES)
        category = rng.choice(_PRODUCT_CATEGORIES)
        rows.append(
            {
                "product_id": _product_id(number),
                "product_name": f"Synthetic {product_type} {number:03d}",
                "category": category,
            }
        )
    return rows


def _generate_order_run(
    rng: random.Random,
    *,
    first_line_number: int,
    first_order_number: int,
    count: int,
    start_date: date,
    include_sales_channel: bool,
    quality_scenario_occurrences: int,
) -> list[CsvRow]:
    line_counts = _order_line_counts(count)
    quality_scenarios = _quality_scenarios_by_order(
        line_counts,
        first_line_number=first_line_number,
        occurrences=quality_scenario_occurrences,
    )
    rows: list[CsvRow] = []
    next_line_number = first_line_number
    for order_offset, line_count in enumerate(line_counts):
        scenario = quality_scenarios.get(order_offset)
        header = _generate_order_header(
            rng,
            order_number=first_order_number + order_offset,
            order_offset=order_offset,
            start_date=start_date,
            include_sales_channel=include_sales_channel,
        )
        _apply_order_header_quality_scenario(header, scenario)

        for line_offset in range(line_count):
            line_number = next_line_number + line_offset
            row = _generate_order_line(
                rng,
                header=header,
                line_number=line_number,
                include_sales_channel=include_sales_channel,
            )
            if line_offset == 0:
                _apply_order_line_quality_scenario(row, scenario)
            rows.append(row)
        next_line_number += line_count
    return rows


def _order_line_counts(line_count: int) -> tuple[int, ...]:
    cycle_line_count = sum(_ORDER_LINE_COUNT_PATTERN)
    full_cycles, remaining_lines = divmod(line_count, cycle_line_count)
    return _ORDER_LINE_COUNT_PATTERN * full_cycles + (1,) * remaining_lines


def _order_count_for_line_count(line_count: int) -> int:
    return len(_order_line_counts(line_count))


def _quality_scenarios_by_order(
    line_counts: Sequence[int],
    *,
    first_line_number: int,
    occurrences: int,
) -> dict[int, int]:
    eligible_orders: list[int] = []
    next_line_number = first_line_number
    for order_offset, line_count in enumerate(line_counts):
        if line_count == 1 and next_line_number not in CORRECTION_LINE_NUMBERS:
            eligible_orders.append(order_offset)
        next_line_number += line_count

    required_orders = occurrences * 7
    if len(eligible_orders) < required_orders:
        raise GenerationError(
            "Not enough single-line orders for isolated quality scenarios."
        )
    return {
        order_offset: assignment % 7
        for assignment, order_offset in enumerate(eligible_orders[:required_orders])
    }


def _generate_order_header(
    rng: random.Random,
    *,
    order_number: int,
    order_offset: int,
    start_date: date,
    include_sales_channel: bool,
) -> CsvRow:
    order_date = start_date + timedelta(days=order_offset % 10)
    header: CsvRow = {
        "order_id": f"ORD_{order_number:07d}",
        "customer_id": _customer_id(rng.randint(1, CUSTOMER_COUNT)),
        "order_status": "completed",
        "order_date": order_date.isoformat(),
    }
    if include_sales_channel:
        channel = rng.choice((None, *sorted(ALLOWED_SALES_CHANNELS)))
        header["sales_channel"] = NULL_VALUE if channel is None else channel
    return header


def _generate_order_line(
    rng: random.Random,
    *,
    header: CsvRow,
    line_number: int,
    include_sales_channel: bool,
) -> CsvRow:
    order_date = date.fromisoformat(header["order_date"])
    updated_at = datetime(
        order_date.year,
        order_date.month,
        order_date.day,
        12,
        tzinfo=timezone.utc,
    ) + timedelta(microseconds=line_number)
    row: CsvRow = {
        "order_line_id": _order_line_id(line_number),
        "order_id": header["order_id"],
        "customer_id": header["customer_id"],
        "product_id": _product_id(rng.randint(1, PRODUCT_COUNT)),
        "quantity": str(rng.randint(1, 5)),
        "unit_price": _random_price(rng),
        "order_status": header["order_status"],
        "order_date": header["order_date"],
        "updated_at": _format_timestamp(updated_at),
    }
    if include_sales_channel:
        row["sales_channel"] = header["sales_channel"]
    return row


def _generate_late_arriving(
    rng: random.Random, run_1_orders: Sequence[CsvRow]
) -> list[CsvRow]:
    late_inserts: list[CsvRow] = []
    first_late_line_number = (
        RUN_1_ORDER_COUNT + RUN_2_ORDER_COUNT + RUN_3_ORDER_COUNT + 1
    )
    for offset in range(RUN_3_LATE_INSERT_COUNT):
        line_number = first_late_line_number + offset
        historical_date = RUN_1_START_DATE + timedelta(days=offset)
        late_inserts.append(
            {
                "order_line_id": _order_line_id(line_number),
                "order_id": f"ORD_LATE_{offset + 1:03d}",
                "customer_id": _customer_id(rng.randint(1, CUSTOMER_COUNT)),
                "product_id": _product_id(rng.randint(1, PRODUCT_COUNT)),
                "quantity": str(rng.randint(1, 5)),
                "unit_price": _random_price(rng),
                "order_status": "completed",
                "order_date": historical_date.isoformat(),
                "updated_at": _format_timestamp(
                    LATE_ARRIVAL_TIME + timedelta(microseconds=offset)
                ),
            }
        )

    run_1_by_key = {row["order_line_id"]: row for row in run_1_orders}
    corrections: list[CsvRow] = []
    for offset, line_number in enumerate(CORRECTION_LINE_NUMBERS):
        original = run_1_by_key[_order_line_id(line_number)]
        correction = dict(original)
        if offset % 2 == 0:
            correction["quantity"] = str(int(original["quantity"]) + 1)
        else:
            original_price = Decimal(original["unit_price"])
            correction["unit_price"] = _format_decimal(original_price + Decimal("1.00"))
        correction["updated_at"] = _format_timestamp(
            LATE_ARRIVAL_TIME + timedelta(seconds=1, microseconds=offset)
        )
        corrections.append(correction)
    return late_inserts + corrections


def _apply_order_header_quality_scenario(header: CsvRow, scenario: int | None) -> None:
    if scenario == 0:
        header["order_status"] = "cancelled"
    elif scenario == 3:
        header["order_status"] = INVALID_ORDER_STATUS
    elif scenario == 4:
        header["customer_id"] = ORPHAN_CUSTOMER_ID


def _apply_order_line_quality_scenario(row: CsvRow, scenario: int | None) -> None:
    if scenario == 1:
        row["unit_price"] = NULL_VALUE
    elif scenario == 2:
        row["unit_price"] = "-1.00"
    elif scenario == 5:
        row["product_id"] = ORPHAN_PRODUCT_ID
    elif scenario == 6:
        row["unit_price"] = "0.00"


def _random_price(rng: random.Random) -> str:
    cents = rng.randint(500, 50_000)
    return _format_decimal(Decimal(cents) / Decimal(100))


def _format_decimal(value: Decimal) -> str:
    return format(value.quantize(Decimal("0.01")), "f")


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _customer_id(number: int) -> str:
    return f"CUS_{number:06d}"


def _product_id(number: int) -> str:
    return f"PRD_{number:06d}"


def _order_line_id(number: int) -> str:
    return f"OL_{number:08d}"


def _validate_generated_data(generated: GeneratedFiles) -> None:
    if set(generated) != set(MANAGED_FILE_SCHEMAS):
        raise GenerationError("Generated file set does not match the contract.")
    for relative_path, rows in generated.items():
        expected_columns = MANAGED_FILE_SCHEMAS[relative_path]
        for row in rows:
            if tuple(row) != expected_columns:
                raise GenerationError(
                    f"Invalid column order in {relative_path}: {tuple(row)}"
                )

    customers = generated[Path("run_1/customers.csv")]
    products = generated[Path("run_1/products.csv")]
    run_1 = generated[Path("run_1/orders.csv")]
    run_2 = generated[Path("run_2/orders.csv")]
    run_3 = generated[Path("run_3/orders.csv")]
    late_arriving = generated[Path("run_3/late_arriving/orders.csv")]

    _validate_customers(customers)
    _validate_products(products)
    _require_row_count_and_unique_keys(run_1, RUN_1_ORDER_COUNT, "Run 1")
    _require_row_count_and_unique_keys(run_2, RUN_2_ORDER_COUNT, "Run 2")
    _require_row_count_and_unique_keys(run_3, RUN_3_ORDER_COUNT, "Run 3")
    _require_row_count_and_unique_keys(
        late_arriving,
        RUN_3_LATE_INSERT_COUNT + RUN_3_CORRECTION_COUNT,
        "Late-arriving batch",
    )

    run_1_keys = _keys(run_1)
    run_2_keys = _keys(run_2)
    run_3_keys = _keys(run_3)
    initial_keys = run_1_keys | run_2_keys | run_3_keys
    if (
        run_1_keys & run_2_keys
        or run_1_keys & run_3_keys
        or run_2_keys & run_3_keys
        or len(initial_keys)
        != RUN_1_ORDER_COUNT + RUN_2_ORDER_COUNT + RUN_3_ORDER_COUNT
    ):
        raise GenerationError(
            "Initial order_line_id values must be unique across all runs."
        )
    late_arriving_keys = _keys(late_arriving)
    existing_late_keys = late_arriving_keys & initial_keys
    new_late_keys = late_arriving_keys - initial_keys
    if len(new_late_keys) != RUN_3_LATE_INSERT_COUNT:
        raise GenerationError(
            "The late-arriving batch must contain exactly 10 new keys."
        )
    if (
        len(existing_late_keys) != RUN_3_CORRECTION_COUNT
        or not existing_late_keys <= run_1_keys
    ):
        raise GenerationError(
            "Late-arriving corrections must be five distinct Run 1 keys."
        )

    _validate_orders(
        run_1,
        include_sales_channel=False,
        minimum_order_date=RUN_1_START_DATE,
        maximum_order_date=RUN_1_START_DATE + timedelta(days=9),
    )
    _validate_orders(
        run_2,
        include_sales_channel=False,
        minimum_order_date=RUN_2_START_DATE,
        maximum_order_date=RUN_2_START_DATE + timedelta(days=9),
    )
    _validate_orders(
        run_3,
        include_sales_channel=True,
        minimum_order_date=RUN_3_START_DATE,
        maximum_order_date=RUN_3_START_DATE + timedelta(days=9),
    )
    _validate_orders(
        late_arriving,
        include_sales_channel=False,
        minimum_order_date=RUN_1_START_DATE,
        maximum_order_date=RUN_1_START_DATE + timedelta(days=9),
    )
    initial_orders = run_1 + run_2 + run_3
    _validate_order_references(
        customers,
        products,
        initial_orders + late_arriving,
    )
    _validate_corrections(run_1, late_arriving, existing_late_keys)
    latest_run_2_update = max(_parse_timestamp(row["updated_at"]) for row in run_2)
    _validate_late_inserts(
        late_arriving,
        new_late_keys,
        latest_run_2_update=latest_run_2_update,
    )
    _validate_order_header_consistency(run_1, include_sales_channel=False)
    _validate_order_header_consistency(run_2, include_sales_channel=False)
    _validate_order_header_consistency(run_3, include_sales_channel=True)
    _validate_order_header_consistency(initial_orders, include_sales_channel=True)
    _validate_multi_line_orders(initial_orders)
    _validate_logical_upsert(run_1, run_2, run_3, late_arriving)
    _validate_quality_scenarios(initial_orders + late_arriving)


def _validate_customers(customers: Sequence[CsvRow]) -> None:
    if len(customers) != CUSTOMER_COUNT + CUSTOMER_VERSION_COUNT:
        raise GenerationError("Unexpected customers row count.")
    grouped: dict[str, list[CsvRow]] = {}
    for row in customers:
        _require_non_empty(row["customer_id"], "customer_id")
        _require_non_empty(row["customer_name"], "customer_name")
        email = row["email"]
        if email and (email.count("@") != 1 or any(char.isspace() for char in email)):
            raise GenerationError(f"Invalid customer email: {email}")
        country_code = row["country_code"]
        if country_code and not _COUNTRY_CODE_PATTERN.fullmatch(country_code):
            raise GenerationError(f"Invalid customer country_code: {country_code}")
        _parse_timestamp(row["updated_at"])
        grouped.setdefault(row["customer_id"], []).append(row)

    versioned = {key: rows for key, rows in grouped.items() if len(rows) > 1}
    if len(grouped) != CUSTOMER_COUNT or len(versioned) != CUSTOMER_VERSION_COUNT:
        raise GenerationError("Customer version scenario is not deterministic.")
    for rows in versioned.values():
        _select_current_customer(rows)


def _validate_products(products: Sequence[CsvRow]) -> None:
    if len(products) != PRODUCT_COUNT:
        raise GenerationError("Unexpected products row count.")
    for row in products:
        _require_non_empty(row["product_id"], "product_id")
        _require_non_empty(row["product_name"], "product_name")
        _require_non_empty(row["category"], "category")


def _select_current_customer(rows: Sequence[CsvRow]) -> CsvRow:
    if not rows:
        raise GenerationError("Cannot select a customer from an empty sequence.")
    return max(
        rows,
        key=lambda row: (
            _parse_timestamp(row["updated_at"]),
            row["customer_name"],
            row["email"],
            row["country_code"],
        ),
    )


def _require_non_empty(value: str, field_name: str) -> None:
    if not value.strip():
        raise GenerationError(f"{field_name} must not be empty.")


def _require_row_count_and_unique_keys(
    rows: Sequence[CsvRow], expected_count: int, label: str
) -> None:
    if len(rows) != expected_count:
        raise GenerationError(
            f"{label} must contain {expected_count} rows, found {len(rows)}."
        )
    if len(_keys(rows)) != expected_count:
        raise GenerationError(f"{label} must contain unique order_line_id values.")


def _keys(rows: Iterable[CsvRow]) -> set[str]:
    return {row["order_line_id"] for row in rows}


def _validate_orders(
    rows: Sequence[CsvRow],
    *,
    include_sales_channel: bool,
    minimum_order_date: date,
    maximum_order_date: date,
) -> None:
    expected_columns = (
        ORDER_EVOLVED_COLUMNS if include_sales_channel else ORDER_BASE_COLUMNS
    )
    for row in rows:
        if tuple(row) != expected_columns:
            raise GenerationError("Order schema does not match its run.")
        for field_name in (
            "order_line_id",
            "order_id",
            "customer_id",
            "product_id",
            "quantity",
            "order_status",
            "order_date",
            "updated_at",
        ):
            _require_non_empty(row[field_name], field_name)
        if not _INTEGER_PATTERN.fullmatch(row["quantity"]):
            raise GenerationError(f"Invalid INTEGER quantity: {row['quantity']}")
        order_date = _parse_date(row["order_date"])
        if not minimum_order_date <= order_date <= maximum_order_date:
            raise GenerationError(
                f"order_date {order_date} is outside the expected run range."
            )
        try:
            int(row["quantity"])
            _parse_timestamp(row["updated_at"])
        except (ValueError, InvalidOperation) as error:
            raise GenerationError(f"Invalid logical order type: {error}") from error
        _validate_decimal(row["unit_price"])
        if include_sales_channel and (
            row["sales_channel"] and row["sales_channel"] not in ALLOWED_SALES_CHANNELS
        ):
            raise GenerationError("Invalid sales_channel value.")


def _parse_timestamp(value: str) -> datetime:
    if not _TIMESTAMP_PATTERN.fullmatch(value):
        raise GenerationError(f"Invalid TIMESTAMP(6) UTC representation: {value}")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as error:
        raise GenerationError(f"Invalid timestamp: {value}") from error
    return parsed.replace(tzinfo=timezone.utc)


def _parse_date(value: str) -> date:
    if not _DATE_PATTERN.fullmatch(value):
        raise GenerationError(f"Invalid ISO 8601 date representation: {value}")
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise GenerationError(f"Invalid date: {value}") from error


def _validate_decimal(value: str) -> None:
    if value == NULL_VALUE:
        return
    if not _DECIMAL_PATTERN.fullmatch(value):
        raise GenerationError(f"Invalid DECIMAL(12,2) representation: {value}")
    try:
        Decimal(value)
    except InvalidOperation as error:
        raise GenerationError(f"Invalid decimal value: {value}") from error


def _validate_order_references(
    customers: Sequence[CsvRow],
    products: Sequence[CsvRow],
    orders: Sequence[CsvRow],
) -> None:
    customer_keys = {row["customer_id"] for row in customers}
    product_keys = {row["product_id"] for row in products}
    for row in orders:
        if (
            row["customer_id"] != ORPHAN_CUSTOMER_ID
            and row["customer_id"] not in customer_keys
        ):
            raise GenerationError(
                f"Unexpected unresolved customer_id: {row['customer_id']}"
            )
        if (
            row["product_id"] != ORPHAN_PRODUCT_ID
            and row["product_id"] not in product_keys
        ):
            raise GenerationError(
                f"Unexpected unresolved product_id: {row['product_id']}"
            )


def _validate_corrections(
    run_1: Sequence[CsvRow],
    run_3: Sequence[CsvRow],
    correction_keys: set[str],
) -> None:
    originals = {row["order_line_id"]: row for row in run_1}
    corrections = [row for row in run_3 if row["order_line_id"] in correction_keys]
    if len(corrections) != RUN_3_CORRECTION_COUNT:
        raise GenerationError("Unexpected correction count.")
    for correction in corrections:
        original = originals[correction["order_line_id"]]
        if _parse_timestamp(correction["updated_at"]) <= _parse_timestamp(
            original["updated_at"]
        ):
            raise GenerationError("A correction is not newer than its original.")
        if (
            correction["quantity"] == original["quantity"]
            and correction["unit_price"] == original["unit_price"]
        ):
            raise GenerationError("A correction does not change quantity or price.")
        unchanged_columns = (
            "order_line_id",
            "order_id",
            "customer_id",
            "product_id",
            "order_status",
            "order_date",
        )
        for column in unchanged_columns:
            if correction[column] != original[column]:
                raise GenerationError(f"A correction unexpectedly changes {column}.")


def _validate_late_inserts(
    run_3: Sequence[CsvRow],
    late_insert_keys: set[str],
    *,
    latest_run_2_update: datetime,
) -> None:
    minimum_date = RUN_1_START_DATE
    maximum_date = RUN_1_START_DATE + timedelta(days=9)
    for row in run_3:
        if row["order_line_id"] not in late_insert_keys:
            continue
        order_date = _parse_date(row["order_date"])
        if not minimum_date <= order_date <= maximum_date:
            raise GenerationError("A late insert is outside the historical range.")
        if _parse_timestamp(row["updated_at"]) <= latest_run_2_update:
            raise GenerationError("A late insert updated_at must be later than Run 2.")


def _validate_logical_upsert(
    run_1: Sequence[CsvRow],
    run_2: Sequence[CsvRow],
    run_3: Sequence[CsvRow],
    late_arriving: Sequence[CsvRow],
) -> None:
    current = {row["order_line_id"]: row for row in (*run_1, *run_2, *run_3)}
    updates = 0
    for candidate in late_arriving:
        key = candidate["order_line_id"]
        existing = current.get(key)
        if existing is None:
            current[key] = candidate
            continue
        if _order_precedence(candidate) > _order_precedence(existing):
            current[key] = candidate
            updates += 1
    if len(current) != 5_010:
        raise GenerationError("Logical upsert must produce 5,010 distinct keys.")
    if updates != RUN_3_CORRECTION_COUNT:
        raise GenerationError("Logical upsert must apply exactly five updates.")


def _order_precedence(row: CsvRow) -> tuple[str, tuple[str, ...]]:
    payload = tuple(
        row.get(column, NULL_VALUE)
        for column in ORDER_EVOLVED_COLUMNS
        if column != "order_line_id"
    )
    return row["updated_at"], payload


def _validate_multi_line_orders(rows: Sequence[CsvRow]) -> None:
    order_lines_by_order: dict[str, set[str]] = {}
    for row in rows:
        order_lines_by_order.setdefault(row["order_id"], set()).add(
            row["order_line_id"]
        )
    if not order_lines_by_order:
        raise GenerationError("Initial orders must not be empty.")
    multi_line_orders = sum(
        len(order_line_ids) >= 2 for order_line_ids in order_lines_by_order.values()
    )
    if multi_line_orders == len(order_lines_by_order):
        raise GenerationError(
            "Initial orders must include single-line and multi-line orders."
        )
    ratio = Decimal(multi_line_orders) / Decimal(len(order_lines_by_order))
    if ratio < MIN_MULTI_LINE_ORDER_RATIO:
        raise GenerationError(
            "At least 15% of distinct order_id values must be multi-line."
        )


def _validate_order_header_consistency(
    rows: Sequence[CsvRow], *, include_sales_channel: bool
) -> None:
    header_columns = ["customer_id", "order_date", "order_status"]
    if include_sales_channel:
        header_columns.append("sales_channel")
    grouped: dict[str, list[CsvRow]] = {}
    for row in rows:
        grouped.setdefault(row["order_id"], []).append(row)
    for order_id, order_rows in grouped.items():
        for column in header_columns:
            if len({row.get(column, NULL_VALUE) for row in order_rows}) != 1:
                raise GenerationError(f"Inconsistent {column} for order_id {order_id}.")


def _is_economically_invalid_unit_price(value: str) -> bool:
    return value == NULL_VALUE or Decimal(value) < Decimal("0.00")


def _validate_quality_scenarios(rows: Sequence[CsvRow]) -> None:
    scenario_keys = {
        "cancelled order": {
            row["order_line_id"] for row in rows if row["order_status"] == "cancelled"
        },
        "null unit_price": {
            row["order_line_id"] for row in rows if row["unit_price"] == NULL_VALUE
        },
        "negative unit_price": {
            row["order_line_id"]
            for row in rows
            if row["unit_price"]
            and _is_economically_invalid_unit_price(row["unit_price"])
        },
        "zero unit_price": {
            row["order_line_id"] for row in rows if row["unit_price"] == "0.00"
        },
        "out-of-domain status": {
            row["order_line_id"]
            for row in rows
            if row["order_status"] not in ALLOWED_ORDER_STATUSES
        },
        "orphan customer": {
            row["order_line_id"]
            for row in rows
            if row["customer_id"] == ORPHAN_CUSTOMER_ID
        },
        "orphan product": {
            row["order_line_id"]
            for row in rows
            if row["product_id"] == ORPHAN_PRODUCT_ID
        },
    }
    unexpected_counts = {
        name: len(keys)
        for name, keys in scenario_keys.items()
        if len(keys) != EXPECTED_QUALITY_SCENARIO_OCCURRENCES
    }
    if unexpected_counts:
        raise GenerationError(
            f"Unexpected quality scenario counts: {unexpected_counts}"
        )
    all_keys = [key for keys in scenario_keys.values() for key in keys]
    if len(all_keys) != len(set(all_keys)):
        raise GenerationError("Quality scenarios must not overlap.")


def _write_generated_files(
    root: Path, generated: Mapping[Path, Sequence[CsvRow]]
) -> None:
    for relative_path, rows in generated.items():
        path = root / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=MANAGED_FILE_SCHEMAS[relative_path],
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(rows)


def _validate_written_files(
    root: Path,
) -> None:
    actual_files = {
        path.relative_to(root) for path in root.rglob("*") if path.is_file()
    }
    if actual_files != set(MANAGED_FILE_SCHEMAS):
        raise GenerationError("Staged output contains an unexpected file set.")
    written: GeneratedFiles = {}
    for relative_path in MANAGED_FILE_SCHEMAS:
        with (root / relative_path).open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != MANAGED_FILE_SCHEMAS[relative_path]:
                raise GenerationError(f"Invalid written schema: {relative_path}")
            written[relative_path] = list(reader)
    _validate_generated_data(written)
