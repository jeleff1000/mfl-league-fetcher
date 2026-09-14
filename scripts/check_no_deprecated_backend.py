#!/usr/bin/env python
"""Fail if active publish/workflow paths mention the deprecated DB backend."""

from __future__ import annotations

import re
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
PATTERN = re.compile(r"MotherDuck|MOTHERDUCK|md:|motherduck")
ACTIVE_PATHS = (
    ".github",
    "league-history-workers/.github",
    "duckdb-server",
    "fantasy_football_data_scripts/multi_league/core/delta_publish.py",
    "fantasy_football_data_scripts/multi_league/core/local_db.py",
    "fantasy_football_data_scripts/multi_league/core/targets/fly_target.py",
    "scripts/warm_vercel_cache.py",
)
SKIP_PARTS = {"__pycache__", ".git", ".pytest_cache", ".mypy_cache"}
SKIP_SUFFIXES = {".pyc", ".pyo", ".duckdb", ".parquet", ".gz", ".zip"}


def iter_files(path: Path):
    if path.is_file():
        yield path
        return
    for item in path.rglob("*"):
        if not item.is_file():
            continue
        if any(part in SKIP_PARTS for part in item.parts):
            continue
        if item.suffix.lower() in SKIP_SUFFIXES:
            continue
        yield item


def main() -> int:
    violations: list[str] = []
    for relative in ACTIVE_PATHS:
        root = ROOT / relative
        if not root.exists():
            continue
        for path in iter_files(root):
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for line_number, line in enumerate(text.splitlines(), start=1):
                if PATTERN.search(line):
                    violations.append(f"{path.relative_to(ROOT)}:{line_number}: {line.strip()}")

    if violations:
        print("Deprecated backend references found in active paths:", file=sys.stderr)
        for violation in violations:
            print(f"  {violation}", file=sys.stderr)
        return 1
    print("No deprecated backend references found in active publish/workflow paths")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
