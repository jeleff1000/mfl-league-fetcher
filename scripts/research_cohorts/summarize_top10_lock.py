"""Reduce top-10 order-lock shards into raw and conservative tables."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--years", required=True)
    args = ap.parse_args()
    files = sorted(args.input_dir.glob("*.parquet"))
    if not files:
        raise RuntimeError("no top-10 shards found")
    df = pd.concat([pd.read_parquet(p) for p in files], ignore_index=True)
    expected_years = set(map(int, json.loads(args.years)))
    if not expected_years.issubset(set(df.year.astype(int))):
        raise RuntimeError("top-10 report is missing requested years")
    required = {"year", "position", "grain", "cohort_key", "metric", "available_leagues",
                "lock_75_leagues", "lock_85_leagues", "lock_95_leagues"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"top-10 report missing columns: {sorted(missing)}")
    args.out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out_dir / "top10_lock_by_year_position.parquet", index=False)
    df.to_csv(args.out_dir / "top10_lock_by_year_position.csv", index=False)
    key = ["position", "grain", "cohort_key", "metric"]
    pooled = df.groupby(key, as_index=False).agg(
        available_leagues=("available_leagues", "max"),
        lock_75_leagues=("lock_75_leagues", "max"),
        lock_85_leagues=("lock_85_leagues", "max"),
        lock_95_leagues=("lock_95_leagues", "max"),
        years=("year", lambda x: ",".join(map(str, sorted(set(x))))),
    )
    pooled.to_csv(args.out_dir / "top10_lock_pooled.csv", index=False)
    print(f"validated {len(df):,} top-10 rows across {len(files)} shards")


if __name__ == "__main__":
    main()
