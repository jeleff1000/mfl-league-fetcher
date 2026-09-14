"""Shared CLI argument parsing for import pipeline scripts.

Replaces --context with --db + --data-dir + --quick.
Legacy --context fallback for backward compatibility.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from multi_league.core.fetch_runtime import load_runtime


def add_import_args(parser: argparse.ArgumentParser):
    """Add standard import args to any transform script."""
    parser.add_argument("--db", help="League database name")
    parser.add_argument("--data-dir", help="Local data directory (contains .duckdb file)")
    parser.add_argument("--quick", action="store_true", help="Quick (single-year) import")
    # Legacy fallback
    parser.add_argument("--context", help="(deprecated) Path to league_context.json")


def resolve_import_args(args: argparse.Namespace) -> tuple[str, Path, bool]:
    """Resolve (db_name, data_dir, is_quick) from CLI args.

    Prefers --db + --data-dir. Falls back to --context for legacy support.

    Returns:
        (db_name, data_dir, is_quick)
    """
    if args.db and args.data_dir:
        return args.db, Path(args.data_dir), getattr(args, "quick", False)

    if getattr(args, "context", None):
        runtime = load_runtime(args.context)
        return runtime.db_name, runtime.data_dir, runtime.is_single_year_import

    raise SystemExit("Provide --db + --data-dir, or --context")
