"""Read-only DuckDB access and aggregate loading for the dashboard."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import duckdb

from .config import DashboardConfig
from .queries import QUERY_BY_NAME

REQUIRED_COLUMNS = {
    ("raw", "ingestion_manifest"): {
        "source_file",
        "row_count",
        "ingested_at",
    },
    ("raw", "order_lines"): {
        "order_line_id",
        "source_file",
    },
    ("{schema}", "fct_order_lines"): {
        "order_line_id",
        "order_id",
        "customer_sk",
        "product_sk",
        "date_sk",
        "quantity",
        "unit_price",
        "order_status",
        "sales_channel",
        "is_economically_valid",
        "revenue",
    },
    ("{schema}", "dim_customers"): {
        "customer_sk",
        "customer_id",
        "customer_name",
        "country_code",
    },
    ("{schema}", "dim_products"): {
        "product_sk",
        "category",
    },
    ("{schema}", "dim_date"): {
        "date_sk",
        "date_day",
        "year_number",
        "iso_week_number",
    },
}


class DashboardDataError(RuntimeError):
    """Base class for controlled dashboard data failures."""


class DatabaseUnavailableError(DashboardDataError):
    """Raised when the configured DuckDB file cannot be read."""


class DatabaseIncompleteError(DashboardDataError):
    """Raised when required raw or mart resources are absent."""

    def __init__(self, missing_resources: Sequence[str]) -> None:
        self.missing_resources = tuple(sorted(missing_resources))
        super().__init__(
            "Required database resources are missing: "
            + ", ".join(self.missing_resources)
        )


class DashboardQueryError(DashboardDataError):
    """Raised when an aggregate dashboard query cannot complete."""


@dataclass(frozen=True)
class DatabaseFingerprint:
    """Cache identity that changes when the DuckDB file changes."""

    path: str
    modified_at_ns: int
    size_bytes: int


@dataclass(frozen=True)
class QueryResult:
    """Immutable query result independent from pandas and Streamlit."""

    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]

    def records(self) -> list[dict[str, object]]:
        return [
            dict(zip(self.columns, row, strict=True)) for row in self.rows
        ]


@dataclass(frozen=True)
class DashboardSnapshot:
    """All bounded aggregates rendered in one dashboard refresh."""

    kpis: QueryResult
    revenue_by_category_month: QueryResult
    top_customers: QueryResult
    orders_by_region_week: QueryResult
    category_ranking: QueryResult
    incremental_loads: QueryResult
    data_quality: QueryResult


def database_fingerprint(path: Path) -> DatabaseFingerprint:
    """Return path, mtime, and size for deterministic cache invalidation."""

    try:
        metadata = path.stat()
    except OSError as error:
        raise DatabaseUnavailableError(
            "The configured DuckDB database is unavailable."
        ) from error
    if not path.is_file():
        raise DatabaseUnavailableError(
            "The configured DuckDB database is not a regular file."
        )
    return DatabaseFingerprint(
        path=str(path),
        modified_at_ns=metadata.st_mtime_ns,
        size_bytes=metadata.st_size,
    )


@contextmanager
def open_read_only_database(
    path: Path,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open one short-lived read-only connection and always close it."""

    if not path.is_file():
        raise DatabaseUnavailableError(
            "The configured DuckDB database does not exist."
        )
    try:
        connection = duckdb.connect(str(path), read_only=True)
    except Exception as error:
        raise DatabaseUnavailableError(
            "The configured DuckDB database cannot be opened."
        ) from error
    try:
        yield connection
    finally:
        connection.close()


def _quoted_identifier(identifier: str) -> str:
    return f'"{identifier}"'


def _required_columns(schema: str) -> dict[tuple[str, str], set[str]]:
    return {
        (schema if schema_name == "{schema}" else schema_name, table): columns
        for (schema_name, table), columns in REQUIRED_COLUMNS.items()
    }


def validate_database(
    connection: duckdb.DuckDBPyConnection, schema: str
) -> None:
    """Verify required tables and columns before running dashboard queries."""

    expected = _required_columns(schema)
    rows = connection.execute(
        """
        select table_schema, table_name, column_name
        from information_schema.columns
        where table_schema = 'raw' or table_schema = ?
        """,
        [schema],
    ).fetchall()
    actual: dict[tuple[str, str], set[str]] = {}
    for schema_name, table_name, column_name in rows:
        actual.setdefault((schema_name, table_name), set()).add(column_name)

    missing: list[str] = []
    for resource, expected_columns in expected.items():
        schema_name, table_name = resource
        actual_columns = actual.get(resource)
        if actual_columns is None:
            missing.append(f"{schema_name}.{table_name}")
            continue
        for column in sorted(expected_columns - actual_columns):
            missing.append(f"{schema_name}.{table_name}.{column}")
    if missing:
        raise DatabaseIncompleteError(missing)


def _execute_query(
    connection: duckdb.DuckDBPyConnection,
    query: str,
    *,
    schema: str,
) -> QueryResult:
    compiled_query = query.format(schema=_quoted_identifier(schema))
    cursor = connection.execute(compiled_query)
    columns = tuple(column[0] for column in cursor.description)
    return QueryResult(columns=columns, rows=tuple(cursor.fetchall()))


def load_dashboard_snapshot(config: DashboardConfig) -> DashboardSnapshot:
    """Load only bounded SQL aggregates from a complete DuckDB mart."""

    try:
        with open_read_only_database(config.database_path) as connection:
            validate_database(connection, config.dbt_schema)
            results = {
                name: _execute_query(
                    connection, query, schema=config.dbt_schema
                )
                for name, query in QUERY_BY_NAME.items()
            }
    except (DatabaseUnavailableError, DatabaseIncompleteError):
        raise
    except Exception as error:
        raise DashboardQueryError(
            "Dashboard aggregates could not be loaded."
        ) from error

    return DashboardSnapshot(
        kpis=results["kpis"],
        revenue_by_category_month=results["revenue_by_category_month"],
        top_customers=results["top_customers"],
        orders_by_region_week=results["orders_by_region_week"],
        category_ranking=results["category_ranking"],
        incremental_loads=results["incremental_loads"],
        data_quality=results["data_quality"],
    )
