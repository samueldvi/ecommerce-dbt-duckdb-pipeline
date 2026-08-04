"""Load generated CSV files into a typed, traceable DuckDB raw layer."""

from __future__ import annotations

import csv
import hashlib
import io
import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Mapping, Sequence

import duckdb

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT_ROOT = REPOSITORY_ROOT / "data" / "raw"
DEFAULT_DATABASE_PATH = REPOSITORY_ROOT / "data" / "warehouse" / "ecommerce.duckdb"

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
METADATA_COLUMNS = (
    "ingestion_id",
    "source_file",
    "source_run",
    "source_row_number",
    "row_hash",
    "file_checksum",
    "ingested_at",
)


class IngestionError(RuntimeError):
    """Raised when raw ingestion cannot complete safely."""


class SourceConflictError(IngestionError):
    """Raised when a previously loaded source path has different content."""


class SchemaValidationError(IngestionError):
    """Raised when a CSV or database schema violates the raw contract."""


@dataclass(frozen=True)
class FileSpec:
    """Static contract for one managed source file."""

    relative_path: Path
    dataset_name: str
    table_name: str
    source_run: str
    columns: tuple[str, ...]
    parser: Callable[[Mapping[str, str]], tuple[object, ...]]
    minimum_order_date: date | None = None
    maximum_order_date: date | None = None


@dataclass(frozen=True)
class PreparedRow:
    """Validated values derived from one physical CSV record."""

    source_row_number: int
    typed_values: tuple[object, ...]
    row_hash: str


@dataclass(frozen=True)
class PreparedFile:
    """Immutable source content used throughout a transaction."""

    spec: FileSpec
    source_file: str
    file_checksum: str
    file_size_bytes: int
    rows: tuple[PreparedRow, ...]

    @property
    def ingestion_id(self) -> str:
        identity = f"{self.source_file}\0{self.file_checksum}".encode("utf-8")
        return hashlib.sha256(identity).hexdigest()


@dataclass(frozen=True)
class FileIngestionResult:
    """Outcome for one managed file."""

    source_file: str
    status: str
    rows_inserted: int
    file_checksum: str


@dataclass(frozen=True)
class IngestionResult:
    """Structured result for a successful ingestion request."""

    files_loaded: int
    files_skipped: int
    rows_inserted: int
    files: tuple[FileIngestionResult, ...]
    errors: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        """Return a JSON-serializable representation."""

        return {
            "files_loaded": self.files_loaded,
            "files_skipped": self.files_skipped,
            "rows_inserted": self.rows_inserted,
            "files": [asdict(file_result) for file_result in self.files],
            "errors": list(self.errors),
        }


def _require_text(row: Mapping[str, str], column: str) -> str:
    value = row[column]
    if not value.strip():
        raise SchemaValidationError(f"{column} must not be empty.")
    return value


def _nullable_text(row: Mapping[str, str], column: str) -> str | None:
    value = row[column]
    return value if value != "" else None


def _parse_timestamp(value: str, column: str = "updated_at") -> datetime:
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError as error:
        raise SchemaValidationError(
            f"{column} must use YYYY-MM-DDTHH:MM:SS.ffffffZ: {value!r}."
        ) from error
    if parsed.strftime("%Y-%m-%dT%H:%M:%S.%fZ") != value:
        raise SchemaValidationError(
            f"{column} must use YYYY-MM-DDTHH:MM:SS.ffffffZ: {value!r}."
        )
    return parsed.replace(tzinfo=timezone.utc)


def _parse_date(value: str, column: str = "order_date") -> date:
    if len(value) != 10:
        raise SchemaValidationError(f"{column} must use YYYY-MM-DD: {value!r}.")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as error:
        raise SchemaValidationError(
            f"{column} must use YYYY-MM-DD: {value!r}."
        ) from error
    if parsed.isoformat() != value:
        raise SchemaValidationError(
            f"{column} must use canonical YYYY-MM-DD: {value!r}."
        )
    return parsed


def _parse_integer(value: str, column: str) -> int:
    if not value or value.strip() != value:
        raise SchemaValidationError(f"{column} is not an integer: {value!r}.")
    try:
        parsed = int(value)
    except ValueError as error:
        raise SchemaValidationError(
            f"{column} is not an integer: {value!r}."
        ) from error
    if str(parsed) != value and not (value == "-0" and parsed == 0):
        raise SchemaValidationError(
            f"{column} must use canonical integer syntax: {value!r}."
        )
    return parsed


def _parse_decimal(value: str, column: str) -> Decimal | None:
    if value == "":
        return None
    if value.strip() != value or "." not in value:
        raise SchemaValidationError(f"{column} is not a DECIMAL(12,2): {value!r}.")
    integer_part, fractional_part = value.lstrip("-").split(".", maxsplit=1)
    if (
        not integer_part
        or not integer_part.isdigit()
        or len(integer_part) > 10
        or len(fractional_part) != 2
        or not fractional_part.isdigit()
    ):
        raise SchemaValidationError(f"{column} is not a DECIMAL(12,2): {value!r}.")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise SchemaValidationError(
            f"{column} is not a DECIMAL(12,2): {value!r}."
        ) from error
    if not parsed.is_finite():
        raise SchemaValidationError(
            f"{column} is not a finite DECIMAL(12,2): {value!r}."
        )
    return parsed


def _parse_customer(row: Mapping[str, str]) -> tuple[object, ...]:
    return (
        _require_text(row, "customer_id"),
        _require_text(row, "customer_name"),
        _nullable_text(row, "email"),
        _nullable_text(row, "country_code"),
        _parse_timestamp(_require_text(row, "updated_at")),
    )


def _parse_product(row: Mapping[str, str]) -> tuple[object, ...]:
    return (
        _require_text(row, "product_id"),
        _require_text(row, "product_name"),
        _require_text(row, "category"),
    )


def _parse_order(
    row: Mapping[str, str], *, include_sales_channel: bool
) -> tuple[object, ...]:
    values: list[object] = [
        _require_text(row, "order_line_id"),
        _require_text(row, "order_id"),
        _require_text(row, "customer_id"),
        _require_text(row, "product_id"),
        _parse_integer(_require_text(row, "quantity"), "quantity"),
        _parse_decimal(row["unit_price"], "unit_price"),
        _require_text(row, "order_status"),
        _parse_date(_require_text(row, "order_date")),
        _parse_timestamp(_require_text(row, "updated_at")),
    ]
    if include_sales_channel:
        values.append(_nullable_text(row, "sales_channel"))
    return tuple(values)


def _parse_base_order(row: Mapping[str, str]) -> tuple[object, ...]:
    return _parse_order(row, include_sales_channel=False)


def _parse_evolved_order(row: Mapping[str, str]) -> tuple[object, ...]:
    return _parse_order(row, include_sales_channel=True)


FILE_SPECS = (
    FileSpec(
        Path("run_1/customers.csv"),
        "customers",
        "customers",
        "run_1",
        CUSTOMER_COLUMNS,
        _parse_customer,
    ),
    FileSpec(
        Path("run_1/products.csv"),
        "products",
        "products",
        "run_1",
        PRODUCT_COLUMNS,
        _parse_product,
    ),
    FileSpec(
        Path("run_1/orders.csv"),
        "order_lines",
        "order_lines",
        "run_1",
        ORDER_BASE_COLUMNS,
        _parse_base_order,
        date(2025, 1, 1),
        date(2025, 1, 10),
    ),
    FileSpec(
        Path("run_2/orders.csv"),
        "order_lines",
        "order_lines",
        "run_2",
        ORDER_BASE_COLUMNS,
        _parse_base_order,
        date(2025, 1, 11),
        date(2025, 1, 20),
    ),
    FileSpec(
        Path("run_3/orders.csv"),
        "order_lines",
        "order_lines",
        "run_3",
        ORDER_EVOLVED_COLUMNS,
        _parse_evolved_order,
        date(2025, 1, 21),
        date(2025, 1, 30),
    ),
    FileSpec(
        Path("run_3/late_arriving/orders.csv"),
        "order_lines",
        "order_lines",
        "run_3_late_arriving",
        ORDER_BASE_COLUMNS,
        _parse_base_order,
        date(2025, 1, 1),
        date(2025, 1, 10),
    ),
)
FILE_SPEC_BY_PATH = {spec.relative_path: spec for spec in FILE_SPECS}
BATCH_FILE_PATHS = {
    "run_1": (
        Path("run_1/customers.csv"),
        Path("run_1/products.csv"),
        Path("run_1/orders.csv"),
    ),
    "run_2": (Path("run_2/orders.csv"),),
    "run_3": (Path("run_3/orders.csv"),),
    "run_3_late_arriving": (Path("run_3/late_arriving/orders.csv"),),
}

TABLE_DDL = (
    "CREATE SCHEMA IF NOT EXISTS raw",
    """
    CREATE TABLE IF NOT EXISTS raw.ingestion_manifest (
        ingestion_id VARCHAR PRIMARY KEY,
        source_file VARCHAR NOT NULL UNIQUE,
        dataset_name VARCHAR NOT NULL,
        source_run VARCHAR NOT NULL,
        file_checksum VARCHAR NOT NULL,
        file_size_bytes BIGINT NOT NULL,
        row_count BIGINT NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.customers (
        customer_id VARCHAR NOT NULL,
        customer_name VARCHAR NOT NULL,
        email VARCHAR,
        country_code VARCHAR,
        updated_at TIMESTAMPTZ NOT NULL,
        ingestion_id VARCHAR NOT NULL,
        source_file VARCHAR NOT NULL,
        source_run VARCHAR NOT NULL,
        source_row_number BIGINT NOT NULL,
        row_hash VARCHAR NOT NULL,
        file_checksum VARCHAR NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL,
        UNIQUE (source_file, source_row_number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.products (
        product_id VARCHAR NOT NULL,
        product_name VARCHAR NOT NULL,
        category VARCHAR NOT NULL,
        ingestion_id VARCHAR NOT NULL,
        source_file VARCHAR NOT NULL,
        source_run VARCHAR NOT NULL,
        source_row_number BIGINT NOT NULL,
        row_hash VARCHAR NOT NULL,
        file_checksum VARCHAR NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL,
        UNIQUE (source_file, source_row_number)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw.order_lines (
        order_line_id VARCHAR NOT NULL,
        order_id VARCHAR NOT NULL,
        customer_id VARCHAR NOT NULL,
        product_id VARCHAR NOT NULL,
        quantity INTEGER NOT NULL,
        unit_price DECIMAL(12,2),
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
        ingested_at TIMESTAMPTZ NOT NULL,
        UNIQUE (source_file, source_row_number)
    )
    """,
)

EXPECTED_TABLE_SCHEMA = {
    "ingestion_manifest": (
        ("ingestion_id", "VARCHAR", "NO"),
        ("source_file", "VARCHAR", "NO"),
        ("dataset_name", "VARCHAR", "NO"),
        ("source_run", "VARCHAR", "NO"),
        ("file_checksum", "VARCHAR", "NO"),
        ("file_size_bytes", "BIGINT", "NO"),
        ("row_count", "BIGINT", "NO"),
        ("ingested_at", "TIMESTAMP WITH TIME ZONE", "NO"),
    ),
    "customers": (
        ("customer_id", "VARCHAR", "NO"),
        ("customer_name", "VARCHAR", "NO"),
        ("email", "VARCHAR", "YES"),
        ("country_code", "VARCHAR", "YES"),
        ("updated_at", "TIMESTAMP WITH TIME ZONE", "NO"),
        ("ingestion_id", "VARCHAR", "NO"),
        ("source_file", "VARCHAR", "NO"),
        ("source_run", "VARCHAR", "NO"),
        ("source_row_number", "BIGINT", "NO"),
        ("row_hash", "VARCHAR", "NO"),
        ("file_checksum", "VARCHAR", "NO"),
        ("ingested_at", "TIMESTAMP WITH TIME ZONE", "NO"),
    ),
    "products": (
        ("product_id", "VARCHAR", "NO"),
        ("product_name", "VARCHAR", "NO"),
        ("category", "VARCHAR", "NO"),
        ("ingestion_id", "VARCHAR", "NO"),
        ("source_file", "VARCHAR", "NO"),
        ("source_run", "VARCHAR", "NO"),
        ("source_row_number", "BIGINT", "NO"),
        ("row_hash", "VARCHAR", "NO"),
        ("file_checksum", "VARCHAR", "NO"),
        ("ingested_at", "TIMESTAMP WITH TIME ZONE", "NO"),
    ),
    "order_lines": (
        ("order_line_id", "VARCHAR", "NO"),
        ("order_id", "VARCHAR", "NO"),
        ("customer_id", "VARCHAR", "NO"),
        ("product_id", "VARCHAR", "NO"),
        ("quantity", "INTEGER", "NO"),
        ("unit_price", "DECIMAL(12,2)", "YES"),
        ("order_status", "VARCHAR", "NO"),
        ("order_date", "DATE", "NO"),
        ("updated_at", "TIMESTAMP WITH TIME ZONE", "NO"),
        ("sales_channel", "VARCHAR", "YES"),
        ("ingestion_id", "VARCHAR", "NO"),
        ("source_file", "VARCHAR", "NO"),
        ("source_run", "VARCHAR", "NO"),
        ("source_row_number", "BIGINT", "NO"),
        ("row_hash", "VARCHAR", "NO"),
        ("file_checksum", "VARCHAR", "NO"),
        ("ingested_at", "TIMESTAMP WITH TIME ZONE", "NO"),
    ),
}
EXPECTED_KEY_CONSTRAINTS = {
    ("ingestion_manifest", "PRIMARY KEY", ("ingestion_id",)),
    ("ingestion_manifest", "UNIQUE", ("source_file",)),
    ("customers", "UNIQUE", ("source_file", "source_row_number")),
    ("products", "UNIQUE", ("source_file", "source_row_number")),
    ("order_lines", "UNIQUE", ("source_file", "source_row_number")),
}


class RawIngestor:
    """Initialize and transactionally populate the DuckDB raw layer."""

    def __init__(
        self,
        database_path: Path | str = DEFAULT_DATABASE_PATH,
        *,
        input_root: Path | str = DEFAULT_INPUT_ROOT,
    ) -> None:
        self.database_path = Path(database_path).expanduser().resolve()
        self.input_root = Path(input_root).expanduser().resolve()

    def initialize_schema(self) -> None:
        """Create and verify the raw schema in an explicit transaction."""

        try:
            self.database_path.parent.mkdir(parents=True, exist_ok=True)
            connection = duckdb.connect(str(self.database_path))
        except Exception as error:
            raise IngestionError(
                f"Unable to open DuckDB database {self.database_path}: {error}"
            ) from error
        transaction_open = False
        try:
            connection.execute("BEGIN TRANSACTION")
            transaction_open = True
            for statement in TABLE_DDL:
                connection.execute(statement)
            self._validate_database_schema(connection)
            connection.execute("COMMIT")
            transaction_open = False
        except Exception as error:
            rollback_error = self._rollback(connection, transaction_open)
            detail = f"Unable to initialize the raw schema: {error}"
            if rollback_error is not None:
                detail += f". Rollback also failed: {rollback_error}"
            if isinstance(error, IngestionError):
                raise type(error)(detail) from error
            raise IngestionError(detail) from error
        finally:
            connection.close()

    def ingest_dataset(self) -> IngestionResult:
        """Load all six managed CSV files in one transaction."""

        prepared_files = tuple(self._prepare_file(spec) for spec in FILE_SPECS)
        self._validate_complete_dataset(prepared_files)
        return self._ingest_prepared_files(prepared_files)

    def ingest_file(self, source_file: Path | str) -> IngestionResult:
        """Load or replay one managed source file transactionally."""

        spec = self._resolve_file_spec(source_file)
        prepared_file = self._prepare_file(spec)
        return self._ingest_prepared_files((prepared_file,))

    def ingest_batch(self, source_batch: str) -> IngestionResult:
        """Load or replay one logical source batch transactionally."""

        batch_paths = BATCH_FILE_PATHS.get(source_batch)
        if batch_paths is None:
            allowed = ", ".join(BATCH_FILE_PATHS)
            raise IngestionError(
                f"Unknown source batch {source_batch!r}; expected one of {allowed}."
            )
        prepared_files = tuple(
            self._prepare_file(FILE_SPEC_BY_PATH[path]) for path in batch_paths
        )
        return self._ingest_prepared_files(prepared_files)

    def _ingest_prepared_files(
        self, prepared_files: Sequence[PreparedFile]
    ) -> IngestionResult:
        self.initialize_schema()
        try:
            connection = duckdb.connect(str(self.database_path))
        except Exception as error:
            raise IngestionError(
                f"Unable to open DuckDB database {self.database_path}: {error}"
            ) from error
        transaction_open = False
        try:
            connection.execute("BEGIN TRANSACTION")
            transaction_open = True
            results: list[FileIngestionResult] = []
            for prepared_file in prepared_files:
                manifest = self._manifest_for_source(
                    connection, prepared_file.source_file
                )
                if manifest is not None:
                    self._validate_replay_state(connection, prepared_file, manifest)
                    results.append(
                        FileIngestionResult(
                            prepared_file.source_file,
                            "skipped",
                            0,
                            prepared_file.file_checksum,
                        )
                    )
                    continue

                self._validate_order_batch_against_raw(connection, prepared_file)
                ingested_at = datetime.now(timezone.utc)
                self._insert_rows(connection, prepared_file, ingested_at)
                self._after_file_rows_inserted(connection, prepared_file)
                self._validate_inserted_rows(connection, prepared_file)
                self._insert_manifest(connection, prepared_file, ingested_at)
                self._validate_committed_pair(connection, prepared_file)
                results.append(
                    FileIngestionResult(
                        prepared_file.source_file,
                        "loaded",
                        len(prepared_file.rows),
                        prepared_file.file_checksum,
                    )
                )

            connection.execute("COMMIT")
            transaction_open = False
            return IngestionResult(
                files_loaded=sum(result.status == "loaded" for result in results),
                files_skipped=sum(result.status == "skipped" for result in results),
                rows_inserted=sum(result.rows_inserted for result in results),
                files=tuple(results),
            )
        except Exception as error:
            rollback_error = self._rollback(connection, transaction_open)
            detail = f"Raw ingestion failed: {error}"
            if rollback_error is not None:
                detail += f". Rollback also failed: {rollback_error}"
            if isinstance(error, IngestionError):
                raise type(error)(detail) from error
            raise IngestionError(detail) from error
        finally:
            connection.close()

    def _after_file_rows_inserted(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: PreparedFile,
    ) -> None:
        """Provide an observability seam while the transaction is still open."""

    def _resolve_file_spec(self, source_file: Path | str) -> FileSpec:
        requested = Path(source_file)
        candidate = (
            requested if requested.is_absolute() else self.input_root / requested
        ).resolve()
        try:
            relative_path = candidate.relative_to(self.input_root)
        except ValueError as error:
            raise IngestionError(
                f"Source file must be inside input root {self.input_root}."
            ) from error
        spec = FILE_SPEC_BY_PATH.get(relative_path)
        if spec is None:
            raise IngestionError(f"Unmanaged source file: {relative_path.as_posix()}.")
        return spec

    def _prepare_file(self, spec: FileSpec) -> PreparedFile:
        source_path = (self.input_root / spec.relative_path).resolve()
        try:
            source_path.relative_to(self.input_root)
        except ValueError as error:
            raise IngestionError(
                f"Source path escapes input root: {spec.relative_path}."
            ) from error
        try:
            content = source_path.read_bytes()
        except OSError as error:
            raise IngestionError(
                f"Unable to read {spec.relative_path.as_posix()}: {error}"
            ) from error
        file_checksum = hashlib.sha256(content).hexdigest()
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError as error:
            raise SchemaValidationError(
                f"{spec.relative_path.as_posix()} is not valid UTF-8."
            ) from error

        reader = csv.reader(io.StringIO(text, newline=""), strict=True)
        try:
            header = tuple(next(reader))
        except StopIteration as error:
            raise SchemaValidationError(
                f"{spec.relative_path.as_posix()} is empty."
            ) from error
        except csv.Error as error:
            raise SchemaValidationError(
                f"Invalid CSV header in {spec.relative_path.as_posix()}: {error}"
            ) from error
        if header != spec.columns:
            raise SchemaValidationError(
                f"Invalid schema for {spec.relative_path.as_posix()}: "
                f"expected {spec.columns}, received {header}."
            )

        rows: list[PreparedRow] = []
        try:
            for raw_values in reader:
                source_row_number = reader.line_num
                if len(raw_values) != len(spec.columns):
                    raise SchemaValidationError(
                        f"{spec.relative_path.as_posix()} line "
                        f"{source_row_number} has {len(raw_values)} values; "
                        f"expected {len(spec.columns)}."
                    )
                raw_values_tuple = tuple(raw_values)
                row = dict(zip(spec.columns, raw_values_tuple, strict=True))
                try:
                    typed_values = spec.parser(row)
                except SchemaValidationError as error:
                    raise SchemaValidationError(
                        f"{spec.relative_path.as_posix()} line "
                        f"{source_row_number}: {error}"
                    ) from error
                self._validate_file_specific_values(
                    spec, typed_values, source_row_number
                )
                rows.append(
                    PreparedRow(
                        source_row_number,
                        typed_values,
                        _row_hash(spec.columns, raw_values_tuple),
                    )
                )
        except csv.Error as error:
            raise SchemaValidationError(
                f"Invalid CSV in {spec.relative_path.as_posix()} at line "
                f"{reader.line_num}: {error}"
            ) from error

        self._validate_file_rows(spec, rows)
        return PreparedFile(
            spec=spec,
            source_file=spec.relative_path.as_posix(),
            file_checksum=file_checksum,
            file_size_bytes=len(content),
            rows=tuple(rows),
        )

    def _validate_file_specific_values(
        self,
        spec: FileSpec,
        typed_values: tuple[object, ...],
        source_row_number: int,
    ) -> None:
        if spec.dataset_name != "order_lines":
            return
        order_date = typed_values[7]
        if not isinstance(order_date, date):
            raise SchemaValidationError("order_date was not parsed as DATE.")
        if (
            spec.minimum_order_date is not None and order_date < spec.minimum_order_date
        ) or (
            spec.maximum_order_date is not None and order_date > spec.maximum_order_date
        ):
            raise SchemaValidationError(
                f"{spec.relative_path.as_posix()} line {source_row_number} "
                f"has order_date {order_date} outside its run window."
            )

    def _validate_file_rows(self, spec: FileSpec, rows: Sequence[PreparedRow]) -> None:
        if not rows:
            raise SchemaValidationError(
                f"{spec.relative_path.as_posix()} contains no data rows."
            )
        if spec.dataset_name == "customers":
            customer_ids = [str(row.typed_values[0]) for row in rows]
            if len(set(customer_ids)) != 200:
                raise SchemaValidationError(
                    "customers.csv must contain 200 distinct customer_id values."
                )
            versioned = {
                customer_id
                for customer_id in customer_ids
                if customer_ids.count(customer_id) > 1
            }
            if not 3 <= len(versioned) <= 4:
                raise SchemaValidationError(
                    "customers.csv must contain three or four versioned customers."
                )
        elif spec.dataset_name == "products":
            product_ids = [str(row.typed_values[0]) for row in rows]
            if len(set(product_ids)) != 80:
                raise SchemaValidationError(
                    "products.csv must contain 80 distinct product_id values."
                )
        else:
            order_line_ids = [str(row.typed_values[0]) for row in rows]
            if len(order_line_ids) != len(set(order_line_ids)):
                raise SchemaValidationError(
                    f"{spec.relative_path.as_posix()} contains duplicate "
                    "order_line_id values."
                )
            if (
                spec.relative_path == Path("run_3/late_arriving/orders.csv")
                and len(rows) != 15
            ):
                raise SchemaValidationError(
                    "The late-arriving file must contain exactly 15 rows."
                )

    def _validate_complete_dataset(
        self, prepared_files: Sequence[PreparedFile]
    ) -> None:
        prepared_by_path = {
            prepared.spec.relative_path: prepared for prepared in prepared_files
        }
        if set(prepared_by_path) != set(FILE_SPEC_BY_PATH):
            raise SchemaValidationError(
                "Complete ingestion requires all six managed files."
            )

        run_1 = prepared_by_path[Path("run_1/orders.csv")]
        run_2 = prepared_by_path[Path("run_2/orders.csv")]
        run_3 = prepared_by_path[Path("run_3/orders.csv")]
        late = prepared_by_path[Path("run_3/late_arriving/orders.csv")]
        initial_rows = run_1.rows + run_2.rows + run_3.rows
        initial_keys = {str(row.typed_values[0]) for row in initial_rows}
        if len(initial_rows) != 5_000 or len(initial_keys) != 5_000:
            raise SchemaValidationError(
                "The ordinary runs must contain 5,000 distinct order_line_id values."
            )
        late_keys = {str(row.typed_values[0]) for row in late.rows}
        run_1_keys = {str(row.typed_values[0]) for row in run_1.rows}
        if len(late_keys - initial_keys) != 10:
            raise SchemaValidationError(
                "The late-arriving file must contain 10 new keys."
            )
        existing_late_keys = late_keys & initial_keys
        if len(existing_late_keys) != 5 or not existing_late_keys <= run_1_keys:
            raise SchemaValidationError(
                "The late-arriving file must contain five Run 1 corrections."
            )
        if len(initial_keys | late_keys) != 5_010:
            raise SchemaValidationError(
                "The complete raw input must contain 5,010 distinct "
                "order_line_id values."
            )
        latest_run_2_updated_at = max(row.typed_values[8] for row in run_2.rows)
        self._validate_late_insert_timestamps(
            {str(row.typed_values[0]): row.typed_values for row in late.rows},
            late_keys - initial_keys,
            latest_run_2_updated_at,
        )
        self._validate_corrections(run_1, late, existing_late_keys)

    def _validate_corrections(
        self,
        run_1: PreparedFile,
        late: PreparedFile,
        correction_keys: set[str],
    ) -> None:
        originals = {str(row.typed_values[0]): row.typed_values for row in run_1.rows}
        correction_rows = {
            str(row.typed_values[0]): row.typed_values
            for row in late.rows
            if str(row.typed_values[0]) in correction_keys
        }
        preserved_indexes = (0, 1, 2, 3, 6, 7)
        for key, correction in correction_rows.items():
            original = originals[key]
            self._validate_correction_values(
                key, original, correction, preserved_indexes
            )

    def _validate_order_batch_against_raw(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: PreparedFile,
    ) -> None:
        """Validate cumulative order-key rules inside the write transaction."""

        if prepared_file.spec.dataset_name != "order_lines":
            return

        candidate_by_key = {
            str(row.typed_values[0]): row.typed_values for row in prepared_file.rows
        }
        existing_rows = connection.execute(
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
                source_run
            FROM raw.order_lines
            """
        ).fetchall()
        existing_by_key: dict[str, list[tuple[object, ...]]] = {}
        for existing_row in existing_rows:
            key = str(existing_row[0])
            if key in candidate_by_key:
                existing_by_key.setdefault(key, []).append(existing_row)

        if prepared_file.spec.source_run != "run_3_late_arriving":
            overlapping_keys = set(candidate_by_key) & set(existing_by_key)
            if overlapping_keys:
                sample = ", ".join(sorted(overlapping_keys)[:5])
                raise SchemaValidationError(
                    f"{prepared_file.source_file} must contain only new "
                    f"order_line_id values; existing keys include {sample}."
                )
            return

        existing_keys = set(existing_by_key)
        new_keys = set(candidate_by_key) - existing_keys
        if len(new_keys) != 10 or len(existing_keys) != 5:
            raise SchemaValidationError(
                "The late-arriving batch must contain exactly 10 new keys "
                "and five existing Run 1 correction keys."
            )

        latest_run_2_updated_at = connection.execute(
            """
            SELECT max(updated_at)
            FROM raw.order_lines
            WHERE source_run = 'run_2'
            """
        ).fetchone()[0]
        self._validate_late_insert_timestamps(
            candidate_by_key,
            new_keys,
            latest_run_2_updated_at,
        )

        correction_originals: dict[str, tuple[object, ...]] = {}
        for key in existing_keys:
            matching_rows = existing_by_key[key]
            run_1_rows = [row for row in matching_rows if row[9] == "run_1"]
            if len(run_1_rows) != 1 or len(matching_rows) != 1:
                raise SchemaValidationError(
                    "The late-arriving batch must correct five keys found "
                    "exclusively in Run 1."
                )
            correction_originals[key] = run_1_rows[0][:9]

        preserved_indexes = (0, 1, 2, 3, 6, 7)
        for key, original in correction_originals.items():
            self._validate_correction_values(
                key,
                original,
                candidate_by_key[key],
                preserved_indexes,
            )

    @staticmethod
    def _validate_late_insert_timestamps(
        candidate_by_key: dict[str, tuple[object, ...]],
        new_keys: set[str],
        latest_run_2_updated_at: object,
    ) -> None:
        if not isinstance(latest_run_2_updated_at, datetime):
            raise SchemaValidationError(
                "The late-arriving batch requires Run 2 to be loaded first."
            )
        # New keys have historical order dates but arrive after Run 2, so their
        # version timestamps must preserve that logical ingestion sequence.
        invalid_keys = sorted(
            key
            for key in new_keys
            if candidate_by_key[key][8] <= latest_run_2_updated_at
        )
        if invalid_keys:
            sample = ", ".join(invalid_keys[:5])
            raise SchemaValidationError(
                "Late-arriving inserts must have updated_at later than Run 2; "
                f"invalid keys include {sample}."
            )

    @staticmethod
    def _validate_correction_values(
        key: str,
        original: tuple[object, ...],
        correction: tuple[object, ...],
        preserved_indexes: tuple[int, ...],
    ) -> None:
        """Validate a complete correction against its original Run 1 row."""

        if not all(correction[index] == original[index] for index in preserved_indexes):
            raise SchemaValidationError(
                f"Correction {key} changes a preserved business field."
            )
        if correction[4] == original[4] and correction[5] == original[5]:
            raise SchemaValidationError(
                f"Correction {key} changes neither quantity nor unit_price."
            )
        if correction[8] <= original[8]:
            raise SchemaValidationError(
                f"Correction {key} must have a later updated_at."
            )

    def _validate_database_schema(self, connection: duckdb.DuckDBPyConnection) -> None:
        rows = connection.execute(
            """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'raw'
              AND table_name IN (
                  'ingestion_manifest',
                  'customers',
                  'products',
                  'order_lines'
              )
            ORDER BY table_name, ordinal_position
            """
        ).fetchall()
        actual: dict[str, list[tuple[str, str, str]]] = {}
        for table_name, column_name, data_type, is_nullable in rows:
            actual.setdefault(table_name, []).append(
                (column_name, data_type, is_nullable)
            )
        expected = {
            table_name: list(column_definitions)
            for table_name, column_definitions in EXPECTED_TABLE_SCHEMA.items()
        }
        if actual != expected:
            raise SchemaValidationError(
                f"Raw database schema mismatch: expected {expected}, received {actual}."
            )
        constraints = {
            (table_name, constraint_type, tuple(column_names))
            for table_name, constraint_type, column_names in connection.execute(
                """
                SELECT
                    table_name,
                    constraint_type,
                    constraint_column_names
                FROM duckdb_constraints()
                WHERE schema_name = 'raw'
                  AND constraint_type IN ('PRIMARY KEY', 'UNIQUE')
                """
            ).fetchall()
        }
        missing_constraints = EXPECTED_KEY_CONSTRAINTS - constraints
        if missing_constraints:
            raise SchemaValidationError(
                f"Raw database constraints are missing: {sorted(missing_constraints)}."
            )

    def _manifest_for_source(
        self, connection: duckdb.DuckDBPyConnection, source_file: str
    ) -> tuple[object, ...] | None:
        return connection.execute(
            """
            SELECT
                ingestion_id,
                dataset_name,
                source_run,
                file_checksum,
                file_size_bytes,
                row_count
            FROM raw.ingestion_manifest
            WHERE source_file = ?
            """,
            [source_file],
        ).fetchone()

    def _validate_replay_state(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: PreparedFile,
        manifest: tuple[object, ...],
    ) -> None:
        (
            ingestion_id,
            dataset_name,
            source_run,
            file_checksum,
            file_size_bytes,
            row_count,
        ) = manifest
        if file_checksum != prepared_file.file_checksum:
            raise SourceConflictError(
                f"{prepared_file.source_file} was already ingested with "
                f"checksum {file_checksum}; current checksum is "
                f"{prepared_file.file_checksum}."
            )
        expected_manifest = (
            prepared_file.ingestion_id,
            prepared_file.spec.dataset_name,
            prepared_file.spec.source_run,
            prepared_file.file_checksum,
            prepared_file.file_size_bytes,
            len(prepared_file.rows),
        )
        if manifest != expected_manifest:
            raise IngestionError(
                f"Manifest metadata is inconsistent for {prepared_file.source_file}."
            )
        raw_count, source_row_count, matching_metadata, hash_count = connection.execute(
            f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT source_row_number),
                    COUNT(*) FILTER (
                        WHERE file_checksum = ?
                          AND source_file = ?
                          AND source_run = ?
                    ),
                    COUNT(row_hash)
                FROM raw.{prepared_file.spec.table_name}
                WHERE ingestion_id = ?
                """,
            [
                prepared_file.file_checksum,
                prepared_file.source_file,
                prepared_file.spec.source_run,
                ingestion_id,
            ],
        ).fetchone()
        if (
            raw_count != row_count
            or source_row_count != row_count
            or matching_metadata != row_count
            or hash_count != row_count
        ):
            raise IngestionError(
                f"Raw rows are inconsistent with the manifest for "
                f"{prepared_file.source_file}."
            )

    def _insert_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: PreparedFile,
        ingested_at: datetime,
    ) -> None:
        business_columns = prepared_file.spec.columns
        if prepared_file.spec.dataset_name == "order_lines":
            insert_columns = ORDER_EVOLVED_COLUMNS + METADATA_COLUMNS
        else:
            insert_columns = business_columns + METADATA_COLUMNS
        placeholders = ", ".join("?" for _ in insert_columns)
        column_sql = ", ".join(insert_columns)
        parameters = []
        for row in prepared_file.rows:
            business_values = row.typed_values
            if (
                prepared_file.spec.dataset_name == "order_lines"
                and "sales_channel" not in business_columns
            ):
                business_values = business_values + (None,)
            parameters.append(
                business_values
                + (
                    prepared_file.ingestion_id,
                    prepared_file.source_file,
                    prepared_file.spec.source_run,
                    row.source_row_number,
                    row.row_hash,
                    prepared_file.file_checksum,
                    ingested_at,
                )
            )
        connection.executemany(
            f"""
            INSERT INTO raw.{prepared_file.spec.table_name} ({column_sql})
            VALUES ({placeholders})
            """,
            parameters,
        )

    def _validate_inserted_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: PreparedFile,
    ) -> None:
        row_count, source_row_count, checksum_count, hash_count = connection.execute(
            f"""
                SELECT
                    COUNT(*),
                    COUNT(DISTINCT source_row_number),
                    COUNT(DISTINCT file_checksum),
                    COUNT(row_hash)
                FROM raw.{prepared_file.spec.table_name}
                WHERE ingestion_id = ?
                """,
            [prepared_file.ingestion_id],
        ).fetchone()
        expected_count = len(prepared_file.rows)
        if (
            row_count != expected_count
            or source_row_count != expected_count
            or checksum_count != 1
            or hash_count != expected_count
        ):
            raise IngestionError(
                f"Inserted rows failed consistency checks for "
                f"{prepared_file.source_file}."
            )

    def _insert_manifest(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: PreparedFile,
        ingested_at: datetime,
    ) -> None:
        connection.execute(
            """
            INSERT INTO raw.ingestion_manifest (
                ingestion_id,
                source_file,
                dataset_name,
                source_run,
                file_checksum,
                file_size_bytes,
                row_count,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                prepared_file.ingestion_id,
                prepared_file.source_file,
                prepared_file.spec.dataset_name,
                prepared_file.spec.source_run,
                prepared_file.file_checksum,
                prepared_file.file_size_bytes,
                len(prepared_file.rows),
                ingested_at,
            ],
        )

    def _validate_committed_pair(
        self,
        connection: duckdb.DuckDBPyConnection,
        prepared_file: PreparedFile,
    ) -> None:
        manifest_count, manifest_rows, raw_rows, matching_rows = connection.execute(
            f"""
                SELECT
                    COUNT(DISTINCT m.ingestion_id),
                    COALESCE(MAX(m.row_count), 0),
                    COUNT(r.ingestion_id),
                    COUNT(r.ingestion_id) FILTER (
                        WHERE r.file_checksum = m.file_checksum
                          AND r.source_file = m.source_file
                          AND r.source_run = m.source_run
                          AND r.ingested_at = m.ingested_at
                    )
                FROM raw.ingestion_manifest AS m
                LEFT JOIN raw.{prepared_file.spec.table_name} AS r
                    ON r.ingestion_id = m.ingestion_id
                WHERE m.ingestion_id = ?
                GROUP BY
                    m.ingestion_id,
                    m.file_checksum,
                    m.source_file,
                    m.source_run,
                    m.ingested_at
                """,
            [prepared_file.ingestion_id],
        ).fetchone()
        expected_count = len(prepared_file.rows)
        if (
            manifest_count != 1
            or manifest_rows != expected_count
            or raw_rows != expected_count
            or matching_rows != expected_count
        ):
            raise IngestionError(
                f"Raw data and manifest are inconsistent for "
                f"{prepared_file.source_file}."
            )

    @staticmethod
    def _rollback(
        connection: duckdb.DuckDBPyConnection, transaction_open: bool
    ) -> Exception | None:
        if not transaction_open:
            return None
        try:
            connection.execute("ROLLBACK")
        except Exception as error:  # pragma: no cover - engine-level failure
            return error
        return None


def _row_hash(columns: Sequence[str], values: Sequence[str]) -> str:
    payload = json.dumps(
        list(zip(columns, values, strict=True)),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
