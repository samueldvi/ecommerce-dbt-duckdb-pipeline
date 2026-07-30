#!/usr/bin/env python3
"""Command-line entry point for deterministic CSV generation."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from synthetic_data import (
    DEFAULT_OUTPUT_DIR,
    DEFAULT_SEED,
    GenerationError,
    generate_dataset,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate deterministic synthetic e-commerce CSV datasets."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Pseudo-random seed (default: {DEFAULT_SEED}).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output root (default: repository-relative data/raw).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Replace only the CSV files managed by this generator.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        summary = generate_dataset(
            output_dir=args.output_dir,
            seed=args.seed,
            force=args.force,
        )
    except GenerationError as error:
        print(f"Generation failed: {error}", file=sys.stderr)
        return 1

    print(f"Generated synthetic data with seed {summary.seed}:")
    for relative_path, row_count in summary.row_counts.items():
        print(f"- {relative_path}: {row_count} rows")
    print(f"Output directory: {summary.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
