"""Summarize per-year empirical usage calibration into a conservative 2020-2025 table."""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--start-year", type=int, default=2020)
    ap.add_argument("--end-year", type=int, default=2025)
    args = ap.parse_args()

    frames = []
    for year in range(args.start_year, args.end_year + 1):
        path = args.input_dir / f"minimum_sizes_{year}.csv"
        if not path.exists():
            raise FileNotFoundError(path)
        frame = pd.read_csv(path)
        frame["year"] = year
        frames.append(frame)
    df = pd.concat(frames, ignore_index=True)
    threshold_cols = ("precision_n", "r75_n", "r85_n", "r95_n")
    for col in threshold_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    keys = ["position", "grain", "metric", "target_rate"]
    grouped = df.groupby(keys, dropna=False, sort=True)
    out = grouped.agg(
        years_observed=("year", "nunique"),
        years=("year", lambda s: ",".join(str(x) for x in sorted(set(s)))),
        eligible_league_seasons_min=("eligible_league_seasons", "min"),
        candidate_cells_min=("candidate_cells", "min"),
        precision_n_max=("precision_n", "max"),
        r75_n_max=("r75_n", "max"),
        r85_n_max=("r85_n", "max"),
        r95_n_max=("r95_n", "max"),
    ).reset_index()
    expected_years = args.end_year - args.start_year + 1
    out["all_years_present"] = out["years_observed"].eq(expected_years)
    for col in threshold_cols:
        out[f"{col}_all_years_reached"] = grouped[col].apply(lambda s: s.notna().all()).to_numpy()
    out["target_pct"] = (100 * out["target_rate"]).astype(int)
    out = out.sort_values(["position", "grain", "metric", "target_rate"])
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.output, index=False)
    print(f"wrote {len(out):,} rows to {args.output}")


if __name__ == "__main__":
    main()
