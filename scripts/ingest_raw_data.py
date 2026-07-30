#!/usr/bin/env python3
"""Command-line entry point for transactional DuckDB raw ingestion."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from raw_ingestion import (
    DEFAULT_DATABASE_PATH,
    DEFAULT_INPUT_ROOT,
    IngestionError,
    RawIngestor,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Transactionally ingest synthetic CSV files into DuckDB."
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE_PATH,
        help="DuckDB database path.",
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_INPUT_ROOT,
        help="Root containing the six generated CSV files.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="Initialize the raw schema.")
    subparsers.add_parser(
        "load-all", help="Load or replay all six managed files."
    )
    load_batch = subparsers.add_parser(
        "load-batch", help="Load or replay one logical source batch."
    )
    load_batch.add_argument(
        "source_batch",
        choices=("run_1", "run_2", "run_3", "run_3_late_arriving"),
        help="Logical batch identifier.",
    )
    load_file = subparsers.add_parser(
        "load-file", help="Load or replay one managed file."
    )
    load_file.add_argument(
        "source_file",
        type=Path,
        help="Managed source path relative to --input-dir.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ingestor = RawIngestor(args.database, input_root=args.input_dir)
    try:
        if args.command == "init":
            ingestor.initialize_schema()
            output: dict[str, object] = {
                "status": "initialized",
                "database": str(args.database),
                "errors": [],
            }
        elif args.command == "load-all":
            output = ingestor.ingest_dataset().to_dict()
        elif args.command == "load-batch":
            output = ingestor.ingest_batch(args.source_batch).to_dict()
        else:
            output = ingestor.ingest_file(args.source_file).to_dict()
    except IngestionError as error:
        print(
            json.dumps(
                {"status": "failed", "errors": [str(error)]},
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1

    print(json.dumps(output, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
