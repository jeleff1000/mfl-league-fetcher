#!/usr/bin/env python3
"""
Parameter Tuning via Game-by-Game Brier Score Analysis

Evaluates team model quality by predicting individual game outcomes across all
leagues in MotherDuck. Tests different parameter combinations for SHRINK_K,
HALF_LIFE_WEEKS, and SIGMA_FLOOR_RATIO to find values that minimize Brier score.

Usage:
    python -m multi_league.analysis.parameter_tuning
    python -m multi_league.analysis.parameter_tuning --max-leagues 10
    python -m multi_league.analysis.parameter_tuning --holdout 0.25
"""

import argparse
import itertools
import sys
import time

import numpy as np
import pandas as pd
from scipy.stats import norm

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

from multi_league.core.db_reader import get_reader
from multi_league.core.logging_config import get_logger
from multi_league.analysis.calibration_study import discover_league_databases

logger = get_logger(__name__)

# Evaluation weeks — early, mid, late season where calibration matters most
SAMPLE_WEEKS = [4, 8, 12]

# Constants matching playoff_odds_import.py defaults
BOUNDARY_PENALTY = 0.05
PRIOR_W_CAP = 2.0
FALLBACK_CV = 0.15

# Current production parameters (baseline)
CURRENT_SHRINK_K = 3.0
CURRENT_HALF_LIFE = 10
CURRENT_SIGMA_FLOOR = 0.6

# Parameter grid
SHRINK_K_VALUES = [2, 3, 5, 8, 12, 16]
HALF_LIFE_VALUES = [6, 8, 10, 14, 20]
SIGMA_FLOOR_VALUES = [0.5, 0.6, 0.7, 0.8, 0.9]


def classify_league_type(reader, db_name):
    """Classify a league as dynasty, keeper, or redraft from settings."""
    try:
        rows = reader.query(
            f"SELECT settings_json FROM {db_name}.public.league_settings ORDER BY year DESC LIMIT 1",
            database="",
        )
        if rows:
            import json

            s = json.loads(rows[0]["settings_json"])
            dt = str(s.get("draft_type", "")).lower()
            if "dynasty" in dt:
                return "dynasty"
            if "keeper" in dt or s.get("max_keepers", 0) > 0:
                return "keeper"
    except Exception:  # noqa: broad-except
        pass
    # Fallback: check database name
    if "dynasty" in db_name.lower():
        return "dynasty"
    if "keeper" in db_name.lower():
        return "keeper"
    return "redraft"


def load_matchup_data(reader, db_name):
    """Load all matchup rows for a league, filtering to valid managers."""
    sql = f"""
    SELECT year, week, manager, opponent, team_points, opponent_points,
           COALESCE(is_playoffs, 0) as is_playoffs,
           COALESCE(is_consolation, 0) as is_consolation
    FROM {db_name}.public.matchup
    WHERE manager IS NOT NULL
      AND TRIM(manager) != ''
      AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
      AND team_points IS NOT NULL
    """
    try:
        df = reader.query_df(sql, database="")
        if not df.empty:
            df["year"] = pd.to_numeric(df["year"], errors="coerce")
            df["week"] = pd.to_numeric(df["week"], errors="coerce")
            df["team_points"] = pd.to_numeric(df["team_points"], errors="coerce")
            df["opponent_points"] = pd.to_numeric(df["opponent_points"], errors="coerce")
            df = df.dropna(subset=["year", "week", "team_points"])
            df["year"] = df["year"].astype(int)
            df["week"] = df["week"].astype(int)
        return df
    except Exception as e:
        logger.warning(f"Failed to load matchup data from {db_name}: {e}")
        return pd.DataFrame()


def _precompute_league_snapshots(matchup_df, sample_weeks):
    """Pre-compute per-league-season-week data slices and game lists.

    Returns list of (season, week, hist_df, games_df) tuples.
    hist_df: all data before this week (for building models)
    games_df: games AT this week (for evaluation)
    """
    snapshots = []
    years = matchup_df["year"].values
    weeks = matchup_df["week"].values
    is_playoffs = matchup_df["is_playoffs"].values
    is_consolation = matchup_df["is_consolation"].values

    for season in sorted(matchup_df["year"].unique()):
        season_mask = years == season
        max_week = int(weeks[season_mask].max())

        for eval_week in sample_weeks:
            if eval_week > max_week:
                continue

            # History: everything before this week
            hist_mask = (years < season) | ((years == season) & (weeks < eval_week))
            hist_df = matchup_df.iloc[np.where(hist_mask)[0]]
            if len(hist_df) < 2:
                continue

            # Games at this week (regular season only, deduplicated)
            game_mask = season_mask & (weeks == eval_week) & (is_playoffs == 0) & (is_consolation == 0)
            games_df = matchup_df.iloc[np.where(game_mask)[0]]
            if games_df.empty:
                continue

            # Deduplicate: manager < opponent
            managers = games_df["manager"].values
            opponents = games_df["opponent"].values
            dedup_mask = managers < opponents
            games_df = games_df.iloc[np.where(dedup_mask)[0]]
            if games_df.empty:
                continue

            snapshots.append((season, eval_week, hist_df, games_df))

    return snapshots


def build_team_models_fast(hist, season, week, shrink_k, half_life, sigma_floor_ratio):
    """Build team performance models for a given season/week with specified parameters.

    Returns (mu_hat, sigma_hat) dicts keyed by manager name.
    Replicates the logic from playoff_odds_import.build_team_models() in simplified form.
    """
    if len(hist) < 2:
        return {}, {}

    # Regular season weeks estimation
    cur_mask = hist["year"].values == season
    if cur_mask.any():
        regular_season_weeks = max(int(hist["week"].values[cur_mask].max()), 14)
    else:
        regular_season_weeks = 14

    boundary_weeks = max(3, regular_season_weeks // 4)

    # Recency weights — fully vectorized
    hl = max(1, int(half_life))
    lam = np.log(2.0) / hl

    years = hist["year"].values
    wks = hist["week"].values
    pts = hist["team_points"].values
    mgrs = hist["manager"].values

    timeline = (years - season) * 100 + (wks - week)
    weeks_ago = np.maximum(0, -timeline.astype(np.float64))
    base = np.exp(-lam * weeks_ago)

    prior = years < season
    fade = min(max((week - 1) / float(boundary_weeks), 0.0), 1.0)
    penalty = (1 - fade) * BOUNDARY_PENALTY + fade * 1.0
    w = base * np.where(prior, penalty, 1.0)

    # Cap prior season weights — vectorized with groupby on arrays
    if prior.any():
        prior_idx = np.where(prior)[0]
        prior_mgrs = mgrs[prior_idx]
        prior_w = w[prior_idx]

        unique_mgrs, inv = np.unique(prior_mgrs, return_inverse=True)
        mgr_sums = np.bincount(inv, weights=prior_w)

        for i, um in enumerate(unique_mgrs):
            if mgr_sums[i] > 1e-12:
                mask_i = inv == i
                w[prior_idx[mask_i]] = prior_w[mask_i] / mgr_sums[i] * PRIOR_W_CAP

    # League-level stats
    league_mu = float(pts.mean())
    league_sd = float(np.std(pts, ddof=1)) if len(pts) >= 2 else 0.0

    if league_sd == 0 or np.isnan(league_sd):
        if league_mu == 0 or np.isnan(league_mu):
            league_mu = 100.0
        league_sd = league_mu * FALLBACK_CV

    sigma_floor = league_sd * sigma_floor_ratio

    # Per-manager aggregation — vectorized
    unique_mgrs, inv = np.unique(mgrs, return_inverse=True)
    n_mgrs = len(unique_mgrs)

    w_sum = np.bincount(inv, weights=w, minlength=n_mgrs)
    weighted_pts = np.bincount(inv, weights=w * pts, minlength=n_mgrs)
    mu_raw = weighted_pts / (w_sum + 1e-12)

    # Per-manager SD (need loop but on unique managers only)
    sd_raw = np.full(n_mgrs, np.nan)
    for i in range(n_mgrs):
        mgr_pts = pts[inv == i]
        if len(mgr_pts) >= 2:
            sd_raw[i] = float(np.std(mgr_pts, ddof=1))

    # Shrinkage
    k = float(shrink_k)
    weeks_played = max(0, int(week))
    shrinkage_weeks = boundary_weeks
    k_eff = k * max(1.0, (shrinkage_weeks - min(weeks_played, shrinkage_weeks)) / shrinkage_weeks)

    w_eb = w_sum / (w_sum + k_eff)
    mu_hat_arr = w_eb * mu_raw + (1 - w_eb) * league_mu

    # Variance with shrinkage and floor
    k_sigma = k_eff / 2.0
    w_sigma = w_sum / (w_sum + k_sigma)
    sd_fill = np.where(np.isnan(sd_raw), league_sd, sd_raw)
    sigma_raw = w_sigma * sd_fill + (1 - w_sigma) * league_sd
    sigma_hat_arr = np.maximum(sigma_raw, sigma_floor)

    mu_hat = dict(zip(unique_mgrs, mu_hat_arr))
    sigma_hat = dict(zip(unique_mgrs, sigma_hat_arr))

    return mu_hat, sigma_hat


def predict_games_vec(games_df, mu_hat, sigma_hat):
    """Predict game outcomes for pre-filtered, deduplicated games.

    Returns (p_win_array, actual_array) numpy arrays.
    """
    managers = games_df["manager"].values
    opponents = games_df["opponent"].values
    pts_a = games_df["team_points"].values
    pts_b = games_df["opponent_points"].values

    n = len(managers)
    p_win = np.full(n, np.nan)
    actual = np.full(n, np.nan)

    for i in range(n):
        a, b = managers[i], opponents[i]
        if a not in mu_hat or b not in mu_hat:
            continue
        if a not in sigma_hat or b not in sigma_hat:
            continue
        if np.isnan(pts_b[i]):
            continue

        denom = np.sqrt(sigma_hat[a] ** 2 + sigma_hat[b] ** 2)
        if denom < 1e-12:
            continue

        p_win[i] = norm.cdf((mu_hat[a] - mu_hat[b]) / denom)
        if pts_a[i] > pts_b[i]:
            actual[i] = 1.0
        elif pts_b[i] > pts_a[i]:
            actual[i] = 0.0
        else:
            actual[i] = 0.5

    valid = ~np.isnan(p_win) & ~np.isnan(actual)
    return p_win[valid], actual[valid]


def evaluate_params_fast(snapshots_by_league, shrink_k, half_life, sigma_floor_ratio):
    """Evaluate a parameter combo using pre-computed snapshots.

    Args:
        snapshots_by_league: dict of db_name -> list of (season, week, hist_df, games_df)

    Returns:
        dict with 'brier', 'n_games', 'predictions'
    """
    all_p = []
    all_a = []

    for db_name, snapshots in snapshots_by_league.items():
        for season, week, hist_df, games_df in snapshots:
            mu_hat, sigma_hat = build_team_models_fast(hist_df, season, week, shrink_k, half_life, sigma_floor_ratio)
            if not mu_hat:
                continue

            p_win, actual = predict_games_vec(games_df, mu_hat, sigma_hat)
            if len(p_win) > 0:
                all_p.append(p_win)
                all_a.append(actual)

    if not all_p:
        return {"brier": np.nan, "n_games": 0, "predictions": []}

    p_arr = np.concatenate(all_p)
    a_arr = np.concatenate(all_a)

    brier = float(np.mean((p_arr - a_arr) ** 2))
    predictions = list(zip(p_arr.tolist(), a_arr.tolist()))

    return {
        "brier": brier,
        "n_games": len(p_arr),
        "predictions": predictions,
    }


def brier_decomposition(predictions):
    """Compute reliability (calibration) and resolution components of Brier score.

    Uses 10 equal-width bins for decomposition.

    Returns dict with 'reliability', 'resolution', 'uncertainty', 'brier'.
    """
    if not predictions:
        return {"reliability": np.nan, "resolution": np.nan, "uncertainty": np.nan, "brier": np.nan}

    preds = np.array(predictions)
    p = preds[:, 0]
    o = preds[:, 1]
    n = len(p)

    brier = np.mean((p - o) ** 2)
    o_bar = np.mean(o)
    uncertainty = o_bar * (1 - o_bar)

    # Bin into 10 equal-width bins
    bin_edges = np.linspace(0, 1, 11)
    bin_indices = np.digitize(p, bin_edges) - 1
    bin_indices = np.clip(bin_indices, 0, 9)

    reliability = 0.0
    resolution = 0.0
    for k in range(10):
        mask = bin_indices == k
        n_k = mask.sum()
        if n_k == 0:
            continue
        p_k = p[mask].mean()
        o_k = o[mask].mean()
        reliability += n_k * (p_k - o_k) ** 2
        resolution += n_k * (o_k - o_bar) ** 2

    reliability /= n
    resolution /= n

    return {
        "reliability": reliability,
        "resolution": resolution,
        "uncertainty": uncertainty,
        "brier": brier,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Parameter tuning via game-by-game Brier score analysis",
    )
    parser.add_argument(
        "--max-leagues",
        type=int,
        default=None,
        help="Maximum number of leagues to process (for testing)",
    )
    parser.add_argument(
        "--holdout",
        type=float,
        default=0.25,
        help="Fraction of leagues to hold out for validation (default: 0.25)",
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

    # Load all matchup data + classify league types
    logger.info(f"Loading matchup data from {len(databases)} leagues...")
    league_matchups = {}
    league_types = {}
    for i, db_name in enumerate(databases, 1):
        logger.info(f"  [{i}/{len(databases)}] Loading {db_name}...")
        df = load_matchup_data(reader, db_name)
        if not df.empty:
            league_matchups[db_name] = df
            league_types[db_name] = classify_league_type(reader, db_name)

    if not league_matchups:
        logger.error("No matchup data found in any league.")
        sys.exit(1)

    logger.info(f"Loaded data from {len(league_matchups)} leagues")

    # Train/holdout split
    all_db_names = sorted(league_matchups.keys())
    rng = np.random.default_rng(42)
    shuffled = np.array(all_db_names)
    rng.shuffle(shuffled)

    split_idx = int(len(shuffled) * (1 - args.holdout))
    train_dbs = set(shuffled[:split_idx])
    holdout_dbs = set(shuffled[split_idx:])

    train_matchups = {k: v for k, v in league_matchups.items() if k in train_dbs}
    holdout_matchups = {k: v for k, v in league_matchups.items() if k in holdout_dbs}

    logger.info(f"Train: {len(train_matchups)} leagues, Holdout: {len(holdout_matchups)} leagues")

    # Pre-compute snapshots (data slices + game lists) — done once, reused for all param combos
    logger.info("Pre-computing snapshots...")
    train_snapshots = {}
    for db_name, mdf in train_matchups.items():
        snaps = _precompute_league_snapshots(mdf, SAMPLE_WEEKS)
        if snaps:
            train_snapshots[db_name] = snaps

    holdout_snapshots = {}
    for db_name, mdf in holdout_matchups.items():
        snaps = _precompute_league_snapshots(mdf, SAMPLE_WEEKS)
        if snaps:
            holdout_snapshots[db_name] = snaps

    total_train_snaps = sum(len(v) for v in train_snapshots.values())
    total_holdout_snaps = sum(len(v) for v in holdout_snapshots.values())
    logger.info(f"Train: {total_train_snaps} snapshots, Holdout: {total_holdout_snaps} snapshots")

    # Build parameter grid
    param_grid = list(itertools.product(SHRINK_K_VALUES, HALF_LIFE_VALUES, SIGMA_FLOOR_VALUES))
    logger.info(f"Evaluating {len(param_grid)} parameter combinations...")

    t0 = time.time()
    results = []
    for i, (sk, hl, sfr) in enumerate(param_grid, 1):
        if i % 25 == 0 or i == 1:
            elapsed = time.time() - t0
            logger.info(f"  [{i}/{len(param_grid)}] ({elapsed:.0f}s elapsed)...")

        res = evaluate_params_fast(train_snapshots, sk, hl, sfr)
        results.append(
            {
                "shrink_k": sk,
                "half_life": hl,
                "sigma_floor_ratio": sfr,
                "brier": res["brier"],
                "n_games": res["n_games"],
                "predictions": res["predictions"],
            }
        )

    elapsed = time.time() - t0
    logger.info(f"Grid search complete in {elapsed:.1f}s")

    # Build results table
    results_df = (
        pd.DataFrame([{k: v for k, v in r.items() if k != "predictions"} for r in results])
        .sort_values("brier")
        .reset_index(drop=True)
    )

    # Find baseline
    baseline_row = results_df[
        (results_df["shrink_k"] == CURRENT_SHRINK_K)
        & (results_df["half_life"] == CURRENT_HALF_LIFE)
        & (results_df["sigma_floor_ratio"] == CURRENT_SIGMA_FLOOR)
    ]

    # Print report
    print()
    print("=" * 78)
    print("PARAMETER TUNING RESULTS — Brier Score (lower is better)")
    print("=" * 78)
    print(f"Leagues: {len(train_matchups)} (train), {len(holdout_matchups)} (holdout)")
    print(f"Evaluation weeks: {SAMPLE_WEEKS}")
    print(f"Parameter combos: {len(param_grid)}")
    print(f"Time: {elapsed:.1f}s")
    print()

    # Baseline
    print("-" * 78)
    print("BASELINE (current production params)")
    print("-" * 78)
    if not baseline_row.empty:
        bl = baseline_row.iloc[0]
        bl_rank = (
            results_df.index[
                (results_df["shrink_k"] == CURRENT_SHRINK_K)
                & (results_df["half_life"] == CURRENT_HALF_LIFE)
                & (results_df["sigma_floor_ratio"] == CURRENT_SIGMA_FLOOR)
            ][0]
            + 1
        )
        print(f"  SHRINK_K={CURRENT_SHRINK_K}, HALF_LIFE={CURRENT_HALF_LIFE}, " f"SIGMA_FLOOR={CURRENT_SIGMA_FLOOR}")
        print(f"  Brier Score: {bl['brier']:.6f}  (rank {bl_rank}/{len(param_grid)}, " f"n={int(bl['n_games'])} games)")

        # Decomposition for baseline
        bl_result = [
            r
            for r in results
            if r["shrink_k"] == CURRENT_SHRINK_K
            and r["half_life"] == CURRENT_HALF_LIFE
            and r["sigma_floor_ratio"] == CURRENT_SIGMA_FLOOR
        ][0]
        bl_decomp = brier_decomposition(bl_result["predictions"])
        print(f"  Reliability (calibration): {bl_decomp['reliability']:.6f}")
        print(f"  Resolution (discrimination): {bl_decomp['resolution']:.6f}")
        print(f"  Uncertainty: {bl_decomp['uncertainty']:.6f}")
    else:
        print("  (baseline params not in grid)")
    print()

    # Top 10
    print("-" * 78)
    print("TOP 10 PARAMETER COMBOS (train set)")
    print("-" * 78)
    print(
        f"{'Rank':>4}  {'SHRINK_K':>8}  {'HALF_LIFE':>9}  {'SIG_FLOOR':>9}  "
        f"{'Brier':>10}  {'vs Base':>8}  {'Games':>6}"
    )
    print("-" * 78)

    baseline_brier = bl["brier"] if not baseline_row.empty else np.nan

    for idx, row in results_df.head(10).iterrows():
        rank = idx + 1
        diff = row["brier"] - baseline_brier if pd.notna(baseline_brier) else np.nan
        diff_str = f"{diff:+.6f}" if pd.notna(diff) else "N/A"
        print(
            f"{rank:>4}  {row['shrink_k']:>8.0f}  {row['half_life']:>9.0f}  "
            f"{row['sigma_floor_ratio']:>9.1f}  {row['brier']:>10.6f}  "
            f"{diff_str:>8}  {int(row['n_games']):>6}"
        )

    # Decomposition for top combo
    top = results_df.iloc[0]
    top_result = [
        r
        for r in results
        if r["shrink_k"] == top["shrink_k"]
        and r["half_life"] == top["half_life"]
        and r["sigma_floor_ratio"] == top["sigma_floor_ratio"]
    ][0]
    top_decomp = brier_decomposition(top_result["predictions"])
    print()
    print("Top combo decomposition:")
    print(f"  Reliability (calibration): {top_decomp['reliability']:.6f}")
    print(f"  Resolution (discrimination): {top_decomp['resolution']:.6f}")
    print(f"  Uncertainty: {top_decomp['uncertainty']:.6f}")

    # Holdout validation
    if holdout_matchups:
        print()
        print("-" * 78)
        print("HOLDOUT VALIDATION")
        print("-" * 78)

        # Evaluate top combo on holdout
        top_params = (top["shrink_k"], top["half_life"], top["sigma_floor_ratio"])
        holdout_top = evaluate_params_fast(holdout_snapshots, *top_params)
        holdout_baseline = evaluate_params_fast(
            holdout_snapshots,
            CURRENT_SHRINK_K,
            CURRENT_HALF_LIFE,
            CURRENT_SIGMA_FLOOR,
        )

        print(f"  {'Params':<40}  {'Brier':>10}  {'Games':>6}")
        print(f"  {'-'*40}  {'-'*10}  {'-'*6}")

        bl_str = f"Baseline (K={CURRENT_SHRINK_K}, HL={CURRENT_HALF_LIFE}, " f"SF={CURRENT_SIGMA_FLOOR})"
        print(f"  {bl_str:<40}  {holdout_baseline['brier']:>10.6f}  " f"{holdout_baseline['n_games']:>6}")

        top_str = f"Top (K={top_params[0]:.0f}, HL={top_params[1]:.0f}, " f"SF={top_params[2]:.1f})"
        print(f"  {top_str:<40}  {holdout_top['brier']:>10.6f}  " f"{holdout_top['n_games']:>6}")

        if holdout_baseline["brier"] > 0:
            improvement = holdout_baseline["brier"] - holdout_top["brier"]
            pct = improvement / holdout_baseline["brier"] * 100
            print(f"\n  Holdout improvement: {improvement:+.6f} ({pct:+.2f}%)")

        # Decomposition on holdout
        hd_top_decomp = brier_decomposition(holdout_top["predictions"])
        hd_bl_decomp = brier_decomposition(holdout_baseline["predictions"])
        print("\n  Holdout decomposition (top combo):")
        print(f"    Reliability: {hd_top_decomp['reliability']:.6f}  " f"(baseline: {hd_bl_decomp['reliability']:.6f})")
        print(f"    Resolution:  {hd_top_decomp['resolution']:.6f}  " f"(baseline: {hd_bl_decomp['resolution']:.6f})")

    # Parameter sensitivity summary
    print()
    print("-" * 78)
    print("PARAMETER SENSITIVITY (mean Brier by parameter value)")
    print("-" * 78)

    for param_name, param_col, values in [
        ("SHRINK_K", "shrink_k", SHRINK_K_VALUES),
        ("HALF_LIFE_WEEKS", "half_life", HALF_LIFE_VALUES),
        ("SIGMA_FLOOR_RATIO", "sigma_floor_ratio", SIGMA_FLOOR_VALUES),
    ]:
        print(f"\n  {param_name}:")
        for val in values:
            subset = results_df[results_df[param_col] == val]
            mean_brier = subset["brier"].mean()
            marker = (
                " <-- current"
                if (
                    (param_col == "shrink_k" and val == CURRENT_SHRINK_K)
                    or (param_col == "half_life" and val == CURRENT_HALF_LIFE)
                    or (param_col == "sigma_floor_ratio" and val == CURRENT_SIGMA_FLOOR)
                )
                else ""
            )
            print(f"    {val:>6}  ->  {mean_brier:.6f}{marker}")

    # Stratified analysis by league type (dynasty vs redraft)
    print()
    print("-" * 78)
    print("STRATIFIED BY LEAGUE TYPE (dynasty vs redraft)")
    print("-" * 78)

    # Merge keeper into redraft (too few keeper leagues for separate analysis)
    type_groups = {"dynasty": [], "redraft": []}
    for db_name in train_matchups:
        lt = league_types.get(db_name, "redraft")
        bucket = "dynasty" if lt == "dynasty" else "redraft"
        type_groups[bucket].append(db_name)

    for lt_name in ["redraft", "dynasty"]:
        lt_dbs = type_groups[lt_name]
        if not lt_dbs:
            continue

        lt_snapshots = {k: v for k, v in train_snapshots.items() if k in set(lt_dbs)}
        if not lt_snapshots:
            continue

        n_snaps = sum(len(v) for v in lt_snapshots.values())
        print(f"\n  {lt_name.upper()} ({len(lt_dbs)} leagues, {n_snaps} snapshots):")

        # Evaluate all HALF_LIFE values with best K and SF
        best_k = top["shrink_k"]
        best_sf = top["sigma_floor_ratio"]
        print(f"  (fixing SHRINK_K={best_k:.0f}, SIGMA_FLOOR={best_sf:.1f})")
        print(f"  {'HALF_LIFE':>12}  {'Brier':>10}  {'Games':>6}")

        for hl in HALF_LIFE_VALUES:
            res = evaluate_params_fast(lt_snapshots, best_k, hl, best_sf)
            marker = " <-- current" if hl == CURRENT_HALF_LIFE else ""
            if pd.notna(res["brier"]):
                print(f"  {hl:>12}  {res['brier']:>10.6f}  {res['n_games']:>6}{marker}")

    print()
    print("=" * 78)


if __name__ == "__main__":
    main()
