"""Transactional raw ingestion for the synthetic e-commerce datasets."""

from .ingestor import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_INPUT_ROOT,
    IngestionError,
    IngestionResult,
    RawIngestor,
    SchemaValidationError,
    SourceConflictError,
)

__all__ = [
    "DEFAULT_DATABASE_PATH",
    "DEFAULT_INPUT_ROOT",
    "IngestionError",
    "IngestionResult",
    "RawIngestor",
    "SchemaValidationError",
    "SourceConflictError",
]
