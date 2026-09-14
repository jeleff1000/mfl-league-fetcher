#!/usr/bin/env python3
"""
Playoff Odds Calibration Study

Measures how well-calibrated the playoff odds engine's predictions are across
all leagues in MotherDuck. Compares predicted probabilities (p_playoffs, p_champ)
against actual outcomes (made_playoffs, champion) to compute calibration error.

Usage:
    python -m multi_league.analysis.calibration_study
    python -m multi_league.analysis.calibration_study --max-leagues 10
    python -m multi_league.analysis.calibration_study --export calibration_raw.csv
    python -m multi_league.analysis.calibration_study --holdout 0.2
"""

import argparse
import sys

import numpy as np
import pandas as pd

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.core.db_reader import get_reader  # noqa: E402
from multi_league.core.logging_config import get_logger  # noqa: E402

logger = get_logger(__name__)

SKIP_DATABASES = {
    "___ops",
    "md_information_schema",
    "information_schema",
    "system",
    "temp",
    "memory",
}


def discover_league_databases(reader):
    """Discover league databases that have matchup tables with p_playoffs data.

    Returns a sorted list of database names.
    """
    all_dbs = reader.query_df("SHOW DATABASES", database="")
    db_col = all_dbs.columns[0]
    candidates = [
        name for name in all_dbs[db_col].tolist() if name not in SKIP_DATABASES and not name.startswith("___")
    ]

    valid = []
    for db_name in candidates:
        try:
            check = reader.query(
                f"""
                SELECT 1
                FROM {db_name}.public.matchup
                WHERE p_playoffs IS NOT NULL
                LIMIT 1
            """,
                database="",
            )
            if check:
                valid.append(db_name)
        except Exception:
            # Table doesn't exist or other error — skip
            continue

    valid.sort()
    logger.info(f"Discovered {len(valid)} league databases with playoff odds data")
    return valid


def extract_league_data(reader, db_name):
    """Extract prediction-outcome pairs from a single league database.

    Returns a DataFrame with columns: db_name, year, week, manager,
    p_playoffs, p_champ, made_playoffs, is_champion, num_teams.
    """
    sql = f"""
    WITH outcomes AS (
        SELECT
            year,
            franchise_id AS _mgr_id,
            manager,
            MAX(CASE WHEN is_playoffs = 1 THEN 1 ELSE 0 END) AS made_playoffs,
            MAX(CASE WHEN champion = 1 THEN 1 ELSE 0 END) AS is_champion
        FROM {db_name}.public.matchup
        WHERE LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
          AND TRIM(manager) != ''
        GROUP BY year, franchise_id, manager
    ),
    predictions AS (
        SELECT
            year,
            week,
            franchise_id AS _mgr_id,
            manager,
            p_playoffs,
            p_champ
        FROM {db_name}.public.matchup
        WHERE is_playoffs IS DISTINCT FROM 1
          AND p_playoffs IS NOT NULL
          AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
          AND TRIM(manager) != ''
    ),
    team_counts AS (
        SELECT
            year,
            COUNT(DISTINCT franchise_id) AS num_teams
        FROM {db_name}.public.matchup
        WHERE LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
          AND TRIM(manager) != ''
        GROUP BY year
    ),
    years_with_outcomes AS (
        SELECT DISTINCT year
        FROM outcomes
        WHERE made_playoffs = 1 OR is_champion = 1
    )
    SELECT
        p.year,
        p.week,
        p.manager,
        p.p_playoffs,
        p.p_champ,
        COALESCE(o.made_playoffs, 0) AS made_playoffs,
        COALESCE(o.is_champion, 0) AS is_champion,
        tc.num_teams
    FROM predictions p
    INNER JOIN years_with_outcomes ywo ON p.year = ywo.year
    LEFT JOIN outcomes o ON p.year = o.year AND p._mgr_id = o._mgr_id
    LEFT JOIN team_counts tc ON p.year = tc.year
    ORDER BY p.year, p.week, p.manager
    """
    try:
        df = reader.query_df(sql, database="")
        if not df.empty:
            df["db_name"] = db_name
        return df
    except Exception as e:
        logger.warning(f"Failed to extract data from {db_name}: {e}")
        return pd.DataFrame()


def compute_calibration(df, prob_col, outcome_col, n_bins=10):
    """Compute calibration metrics by binning predictions into equal-width bins.

    Args:
        df: DataFrame with prediction and outcome columns.
        prob_col: Column name for predicted probability (0-100 scale).
        outcome_col: Column name for binary outcome (0 or 1).
        n_bins: Number of equal-width bins.

    Returns:
        Dict with 'bins' (DataFrame), 'ece' (float), 'total_pairs' (int).
    """
    work = df[[prob_col, outcome_col]].dropna().copy()
    if work.empty:
        return {"bins": pd.DataFrame(), "ece": np.nan, "total_pairs": 0}

    bin_edges = np.linspace(0, 100, n_bins + 1)
    work["bin"] = pd.cut(
        work[prob_col],
        bins=bin_edges,
        include_lowest=True,
        right=True,
    )

    grouped = (
        work.groupby("bin", observed=False)
        .agg(
            predicted_avg=(prob_col, "mean"),
            actual_rate=(outcome_col, lambda x: x.mean() * 100),
            count=(outcome_col, "count"),
        )
        .reset_index()
    )

    grouped["abs_error"] = (grouped["predicted_avg"] - grouped["actual_rate"]).abs()

    total = grouped["count"].sum()
    if total > 0:
        ece = (grouped["abs_error"] * grouped["count"]).sum() / total
    else:
        ece = np.nan

    return {
        "bins": grouped,
        "ece": ece,
        "total_pairs": int(total),
    }


def compute_calibration_by_week(df, prob_col, outcome_col, n_bins=10):
    """Compute calibration ECE per week.

    Returns a DataFrame with columns: week, ece, total_pairs.
    """
    rows = []
    for week, week_df in df.groupby("week"):
        result = compute_calibration(week_df, prob_col, outcome_col, n_bins)
        rows.append(
            {
                "week": int(week),
                "ece": result["ece"],
                "total_pairs": result["total_pairs"],
            }
        )
    return pd.DataFrame(rows).sort_values("week").reset_index(drop=True)


def compute_calibration_by_league_size(df, prob_col, outcome_col, n_bins=10):
    """Compute calibration ECE by league size bucket.

    Buckets: <=10 (small), 10-12 (medium), 12+ (large).
    Returns a dict mapping bucket name to calibration result.
    """

    def _bucket(n):
        if n <= 10:
            return "small (<=10)"
        elif n <= 12:
            return "medium (10-12)"
        else:
            return "large (12+)"

    df = df.copy()
    df["size_bucket"] = df["num_teams"].apply(_bucket)

    results = {}
    for bucket, bucket_df in df.groupby("size_bucket"):
        results[bucket] = compute_calibration(bucket_df, prob_col, outcome_col, n_bins)
    return results


def print_report(all_data):
    """Print a full calibration report to the console."""
    if all_data.empty:
        print("No data available for calibration report.")
        return

    # --- Summary ---
    n_leagues = all_data["db_name"].nunique()
    n_seasons = all_data.groupby(["db_name", "year"]).ngroups
    n_pairs = len(all_data)
    year_min = int(all_data["year"].min())
    year_max = int(all_data["year"].max())

    print("=" * 70)
    print("PLAYOFF ODDS CALIBRATION STUDY")
    print("=" * 70)
    print(f"Leagues analyzed:    {n_leagues}")
    print(f"League-seasons:      {n_seasons}")
    print(f"Prediction pairs:    {n_pairs:,}")
    print(f"Year range:          {year_min}-{year_max}")
    print()

    # --- P_PLAYOFFS calibration ---
    print("-" * 70)
    print("P_PLAYOFFS CALIBRATION (10 bins)")
    print("-" * 70)
    playoffs_cal = compute_calibration(all_data, "p_playoffs", "made_playoffs", n_bins=10)
    if not playoffs_cal["bins"].empty:
        display = playoffs_cal["bins"][["bin", "predicted_avg", "actual_rate", "count", "abs_error"]].copy()
        display["predicted_avg"] = display["predicted_avg"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        display["actual_rate"] = display["actual_rate"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        display["abs_error"] = display["abs_error"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        display.columns = ["Bin", "Predicted %", "Actual %", "Count", "Abs Error"]
        print(display.to_string(index=False))
        print(f"\nECE (Expected Calibration Error): {playoffs_cal['ece']:.2f}%")
        print(f"Total prediction-outcome pairs:   {playoffs_cal['total_pairs']:,}")
    print()

    # --- P_CHAMP calibration ---
    print("-" * 70)
    print("P_CHAMP CALIBRATION (5 bins)")
    print("-" * 70)
    champ_cal = compute_calibration(all_data, "p_champ", "is_champion", n_bins=5)
    if not champ_cal["bins"].empty:
        display = champ_cal["bins"][["bin", "predicted_avg", "actual_rate", "count", "abs_error"]].copy()
        display["predicted_avg"] = display["predicted_avg"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        display["actual_rate"] = display["actual_rate"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        display["abs_error"] = display["abs_error"].map(lambda x: f"{x:.1f}" if pd.notna(x) else "N/A")
        display.columns = ["Bin", "Predicted %", "Actual %", "Count", "Abs Error"]
        print(display.to_string(index=False))
        print(f"\nECE (Expected Calibration Error): {champ_cal['ece']:.2f}%")
        print(f"Total prediction-outcome pairs:   {champ_cal['total_pairs']:,}")
    print()

    # --- By-week ECE ---
    print("-" * 70)
    print("P_PLAYOFFS ECE BY WEEK")
    print("-" * 70)
    by_week = compute_calibration_by_week(all_data, "p_playoffs", "made_playoffs", n_bins=10)
    if not by_week.empty:
        by_week_display = by_week.copy()
        by_week_display["ece"] = by_week_display["ece"].map(lambda x: f"{x:.2f}" if pd.notna(x) else "N/A")
        by_week_display.columns = ["Week", "ECE %", "Pairs"]
        print(by_week_display.to_string(index=False))
    print()

    # --- By league size ECE ---
    print("-" * 70)
    print("P_PLAYOFFS ECE BY LEAGUE SIZE")
    print("-" * 70)
    by_size = compute_calibration_by_league_size(all_data, "p_playoffs", "made_playoffs", n_bins=10)
    for bucket in sorted(by_size.keys()):
        result = by_size[bucket]
        ece_str = f"{result['ece']:.2f}" if pd.notna(result["ece"]) else "N/A"
        print(f"  {bucket:20s}  ECE: {ece_str}%  (n={result['total_pairs']:,})")
    print()

    # --- Verdict ---
    print("=" * 70)
    playoffs_ece = playoffs_cal["ece"]
    champ_ece = champ_cal["ece"]

    if pd.notna(playoffs_ece):
        champ_str = f", p_champ ECE: {champ_ece:.2f}%" if pd.notna(champ_ece) else ""
        if playoffs_ece < 3:
            verdict = "GOOD - Predictions are well-calibrated"
        elif playoffs_ece < 8:
            verdict = "TUNE - Consider adjusting Bayesian priors"
        else:
            verdict = "REDESIGN - Significant calibration issues"
        print(f"VERDICT: {verdict} (p_playoffs ECE: {playoffs_ece:.2f}%{champ_str})")
    else:
        print("VERDICT: Insufficient data for assessment")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Playoff Odds Calibration Study - measure prediction accuracy across leagues",
    )
    parser.add_argument(
        "--holdout",
        type=float,
        default=0.0,
        help="Fraction of leagues to hold out for validation (0.0-0.5)",
    )
    parser.add_argument(
        "--export",
        type=str,
        default=None,
        help="Path to export raw prediction-outcome data as CSV",
    )
    parser.add_argument(
        "--max-leagues",
        type=int,
        default=None,
        help="Maximum number of leagues to process (for testing)",
    )
    args = parser.parse_args()

    if args.holdout < 0 or args.holdout > 0.5:
        parser.error("--holdout must be between 0.0 and 0.5")

    logger.info("Connecting to MotherDuck...")
    reader = get_reader()

    logger.info("Discovering league databases...")
    databases = discover_league_databases(reader)

    if not databases:
        logger.error("No league databases found with playoff odds data.")
        sys.exit(1)

    if args.max_leagues:
        databases = databases[: args.max_leagues]
        logger.info(f"Limited to {len(databases)} leagues for testing")

    # Extract data from all leagues
    all_frames = []
    for i, db_name in enumerate(databases, 1):
        logger.info(f"[{i}/{len(databases)}] Extracting from {db_name}...")
        df = extract_league_data(reader, db_name)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        logger.error("No prediction-outcome data found in any league.")
        sys.exit(1)

    all_data = pd.concat(all_frames, ignore_index=True)
    logger.info(f"Collected {len(all_data):,} prediction-outcome pairs from {all_data['db_name'].nunique()} leagues")

    # Export if requested
    if args.export:
        all_data.to_csv(args.export, index=False)
        logger.info(f"Raw data exported to {args.export}")

    # Holdout split
    if args.holdout > 0:
        unique_leagues = all_data["db_name"].unique()
        rng = np.random.default_rng(42)
        rng.shuffle(unique_leagues)
        split_idx = int(len(unique_leagues) * (1 - args.holdout))
        train_leagues = set(unique_leagues[:split_idx])
        holdout_leagues = set(unique_leagues[split_idx:])

        train_data = all_data[all_data["db_name"].isin(train_leagues)].copy()
        holdout_data = all_data[all_data["db_name"].isin(holdout_leagues)].copy()

        print(f"\n>>> TRAIN SET ({len(train_leagues)} leagues) <<<\n")
        print_report(train_data)

        print(f"\n>>> HOLDOUT SET ({len(holdout_leagues)} leagues) <<<\n")
        print_report(holdout_data)
    else:
        print_report(all_data)


if __name__ == "__main__":
    main()
