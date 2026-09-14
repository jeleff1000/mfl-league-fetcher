"""Write a deterministic uncompressed release archive for the public research lake."""

from __future__ import annotations

import argparse
from pathlib import Path
import tarfile


FILES = (
    "corpus_snapshot.duckdb",
    "ops_cache.duckdb",
    "nfl_market_adp.parquet",
    "ladder_thresholds.json",
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("archive", type=Path)
    args = parser.parse_args()
    missing = [name for name in FILES if not (args.root / name).is_file()]
    if missing:
        raise FileNotFoundError(", ".join(str(args.root / name) for name in missing))
    with tarfile.open(args.archive, "w") as tar:
        for name in FILES:
            tar.add(args.root / name, arcname=f"research-public-lake/{name}", recursive=False)


if __name__ == "__main__":
    main()
