"""Portable configuration for the local analytics dashboard."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_RELATIVE_PATH = Path("data/warehouse/ecommerce.duckdb")
DATABASE_PATH_ENV = "ECOMMERCE_DB_PATH"
DBT_SCHEMA_ENV = "ECOMMERCE_DBT_SCHEMA"
DEFAULT_DBT_SCHEMA = "analytics"
VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class DashboardConfigurationError(ValueError):
    """Raised when dashboard configuration is unsafe or invalid."""


@dataclass(frozen=True)
class DashboardConfig:
    """Resolved database and schema used by one dashboard request."""

    database_path: Path
    dbt_schema: str
    project_root: Path

    @property
    def database_label(self) -> str:
        """Return a user-safe path without exposing personal directories."""

        try:
            return self.database_path.relative_to(self.project_root).as_posix()
        except ValueError:
            return f"configured file: {self.database_path.name}"


def resolve_database_path(
    *,
    project_root: Path = PROJECT_ROOT,
    environ: Mapping[str, str] | None = None,
) -> Path:
    """Resolve a configured path independently from the current directory."""

    environment = os.environ if environ is None else environ
    raw_path = environment.get(
        DATABASE_PATH_ENV, str(DEFAULT_DATABASE_RELATIVE_PATH)
    ).strip()
    if not raw_path:
        raise DashboardConfigurationError(
            f"{DATABASE_PATH_ENV} must not be empty."
        )
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = project_root / path
    return path.resolve(strict=False)


def resolve_dbt_schema(
    *, environ: Mapping[str, str] | None = None
) -> str:
    """Return a safe DuckDB schema identifier for mart queries."""

    environment = os.environ if environ is None else environ
    schema = environment.get(DBT_SCHEMA_ENV, DEFAULT_DBT_SCHEMA).strip()
    if not VALID_IDENTIFIER.fullmatch(schema):
        raise DashboardConfigurationError(
            f"{DBT_SCHEMA_ENV} must be a simple SQL identifier."
        )
    return schema


def load_dashboard_config(
    *,
    project_root: Path = PROJECT_ROOT,
    environ: Mapping[str, str] | None = None,
) -> DashboardConfig:
    """Load all dashboard configuration from safe local defaults or env."""

    resolved_root = project_root.resolve(strict=False)
    return DashboardConfig(
        database_path=resolve_database_path(
            project_root=resolved_root, environ=environ
        ),
        dbt_schema=resolve_dbt_schema(environ=environ),
        project_root=resolved_root,
    )
