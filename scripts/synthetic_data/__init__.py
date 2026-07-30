"""Deterministic synthetic dataset generation."""

from .generator import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SEED,
    GenerationError,
    GenerationSummary,
    OutputExistsError,
    generate_dataset,
)

__all__ = [
    "DEFAULT_OUTPUT_DIR",
    "DEFAULT_SEED",
    "GenerationError",
    "GenerationSummary",
    "OutputExistsError",
    "generate_dataset",
]
