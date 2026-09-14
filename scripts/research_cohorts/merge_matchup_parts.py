"""Merge temporary matchup runner outputs into one adaptive bundle."""
from __future__ import annotations

import argparse
from pathlib import Path

from build_bounded_matchup_cache import merge


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--parts", nargs="+", type=Path, required=True)
    args = parser.parse_args()
    parts = [p for p in args.parts if p.exists()]
    if not parts:
        raise SystemExit("no temporary matchup parts found")
    merge(parts, args.output)
    print(f"merged_parts={len(parts)} output={args.output}")


if __name__ == "__main__":
    main()
