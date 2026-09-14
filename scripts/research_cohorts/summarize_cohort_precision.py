"""Reduce year/position precision shards and validate the artifact contract."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


REQUIRED_METRICS = {"roster_pct", "start_pct", "healthy_start_pct", "win_pct"}
REQUIRED_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}
REQUIRED_TARGETS = set(range(10, 101, 10))
REQUIRED_MARGINS = {1, 3, 5}


def reduce_shards(input_dir: Path, out_dir: Path, years: list[int], confidence: float) -> None:
    files = sorted(input_dir.glob("*.parquet"))
    if not files:
        raise RuntimeError("no calibration shards found")
    frames = [pd.read_parquet(path) for path in files]
    df = pd.concat(frames, ignore_index=True)
    expected = {(year, pos) for year in years for pos in REQUIRED_POSITIONS}
    got = set(zip(df["year"].astype(int), df["position"].astype(str)))
    missing = expected - got
    if missing:
        raise RuntimeError(f"missing year-position shards: {sorted(missing)}")
    if abs(float(df["confidence"].iloc[0]) - confidence) > 1e-9:
        raise RuntimeError("confidence mismatch in shard output")
    keys = ["year", "position", "cohort_key", "metric", "target_pct", "margin_pct"]
    if df.duplicated(keys).any():
        raise RuntimeError("duplicate calibration keys")
    if set(df.metric) != REQUIRED_METRICS:
        raise RuntimeError(f"metric coverage is {sorted(set(df.metric))}")
    if set(df.target_pct.astype(int)) != REQUIRED_TARGETS or set(df.margin_pct.astype(int)) != REQUIRED_MARGINS:
        raise RuntimeError("target or margin coverage is incomplete")
    if (df.weekly_required_leagues <= 0).any() or (df.season_required_leagues <= 0).any():
        raise RuntimeError("nonpositive required league count")

    out_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_dir / "precision_by_year_position.parquet", index=False)
    df.to_csv(out_dir / "precision_by_year_position.csv", index=False)

    pooled = (
        df.groupby(["position", "cohort_key", "metric", "target_pct", "margin_pct"], as_index=False)
        .agg(
            available_leagues=("available_leagues", "sum"),
            weekly_required_leagues=("weekly_required_leagues", "max"),
            season_required_leagues=("season_required_leagues", "max"),
            max_rho=("rho", "max"),
            max_periods=("periods", "max"),
            years=("year", lambda x: ",".join(map(str, sorted(set(x))))),
        )
    )
    pooled["weekly_action"] = pooled.apply(
        lambda r: "retain" if r.available_leagues >= r.weekly_required_leagues else "pool", axis=1
    )
    pooled["season_action"] = pooled.apply(
        lambda r: "retain" if r.available_leagues >= r.season_required_leagues else "pool", axis=1
    )
    pooled.to_csv(out_dir / "precision_pooled_by_position_cohort.csv", index=False)

    rec = df[["year", "position", "cohort_key", "metric", "target_pct", "margin_pct",
              "available_leagues", "weekly_required_leagues", "season_required_leagues"]].copy()
    rec["weekly_action"] = (rec.available_leagues >= rec.weekly_required_leagues).map({True: "retain", False: "pool"})
    rec["season_action"] = (rec.available_leagues >= rec.season_required_leagues).map({True: "retain", False: "pool"})
    rec.to_csv(out_dir / "pooling_recommendations.csv", index=False)
    print(f"validated {len(df):,} rows across {len(files)} shards")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, required=True)
    ap.add_argument("--years", required=True)
    ap.add_argument("--confidence", type=float, required=True)
    args = ap.parse_args()
    reduce_shards(args.input_dir, args.out_dir, [int(x) for x in json.loads(args.years)], args.confidence)


if __name__ == "__main__":
    main()
