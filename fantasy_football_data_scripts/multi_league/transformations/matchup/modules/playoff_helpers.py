def ensure_normalized(func):
    """Decorator to ensure data normalization"""

    @wraps(func)
    def wrapper(df: pd.DataFrame, *args, **kwargs):
        # Normalize input
        df = normalize_numeric_columns(df)

        # Run transformation
        result = func(df, *args, **kwargs)

        # Normalize output
        result = normalize_numeric_columns(result)

        # Ensure league_id present
        if "league_id" in df.columns:
            league_id = df["league_id"].iloc[0] if len(df) > 0 else None
            if league_id:
                result = ensure_league_id(result, league_id)

        return result

    return wrapper


"""
Playoff Helpers Module

Utility functions for playoff odds calculations.

This module contains helper functions for:
- Matchup canonicalization
- Wins/points aggregation
- Seeding/ranking
- Schedule parsing
- History snapshot generation
"""

from functools import wraps

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

from core.data_normalization import normalize_numeric_columns, ensure_league_id
import numpy as np
import pandas as pd
from functools import cmp_to_key
from multi_league.core.identity import get_manager_col, get_opponent_col

# Import optimized SQL aggregation functions
# Use relative import since we're in the modules directory
try:
    from .sql_aggregations import (
        get_wins_points_to_date_fast,
        get_history_snapshots_fast,  # noqa: F401 — re-exported for backwards compatibility
    )
except ImportError:
    from sql_aggregations import (
        get_wins_points_to_date_fast,
    )

# -------------------------
# Tiebreaker System
# -------------------------
DEFAULT_TIEBREAKER_ORDER = ["total_points", "head_to_head"]
RECORD_FIRST_SEEDING = "record"
POINTS_FIRST_SEEDING = "points_for"

POINTS_FIRST_SEEDING_ALIASES = {
    "best_score",
    "highest_score",
    "highest_points",
    "pf",
    "point",
    "points",
    "points_for",
    "score",
    "total_points",
    "total_points_for",
    "total_points_scored",
}
RECORD_FIRST_SEEDING_ALIASES = {
    "h2h",
    "head_to_head",
    "head_to_head_record",
    "overall_record",
    "record",
    "standings",
    "win_loss",
    "wins",
}


def _normalize_seeding_token(value) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except (TypeError, ValueError):
        pass
    token = str(value).strip().lower()
    for ch in (" ", "-", "/", ".", ":"):
        token = token.replace(ch, "_")
    while "__" in token:
        token = token.replace("__", "_")
    return token.strip("_")


def _is_points_first_seed_token(token: str) -> bool:
    return (
        token in POINTS_FIRST_SEEDING_ALIASES
        or "points_for" in token
        or "points_scored" in token
        or "best_score" in token
        or "highest_score" in token
    )


def _is_record_first_seed_token(token: str) -> bool:
    return token in RECORD_FIRST_SEEDING_ALIASES or "record" in token or "win_loss" in token or token == "total_wins"


def normalize_playoff_seeding_rule(
    rule=None,
    rule_by=None,
) -> str:
    """Normalize flat league_settings seeding metadata to a cross-platform rule.

    Default league standings remain record-first. When a platform flattens a
    best-score / total-points setting into the canonical DDL, downstream playoff
    odds should seed by PF first and use wins as the secondary tie-breaker.
    """
    for value in (rule, rule_by):
        token = _normalize_seeding_token(value)
        if not token:
            continue
        if _is_points_first_seed_token(token):
            return POINTS_FIRST_SEEDING
        if _is_record_first_seed_token(token):
            return RECORD_FIRST_SEEDING

    return RECORD_FIRST_SEEDING


def seed_indices_from_totals(wins_matrix: np.ndarray, points_matrix: np.ndarray, seeding_rule=None) -> np.ndarray:
    """Return team indices ordered by simulated final seed for each row.

    The output shape matches ``wins_matrix``. Each row contains the team indices
    from seed 1 through seed N.
    """
    wins_matrix = np.asarray(wins_matrix)
    points_matrix = np.asarray(points_matrix)
    if wins_matrix.shape != points_matrix.shape:
        raise ValueError("wins_matrix and points_matrix must have the same shape")

    rule = normalize_playoff_seeding_rule(seeding_rule)
    seeds_idx = np.empty_like(np.argsort(-wins_matrix, axis=1, kind="stable"))
    identity_order = np.arange(wins_matrix.shape[1])

    for sim_idx in range(wins_matrix.shape[0]):
        if rule == POINTS_FIRST_SEEDING:
            seeds_idx[sim_idx] = np.lexsort((identity_order, -wins_matrix[sim_idx], -points_matrix[sim_idx]))
        else:
            seeds_idx[sim_idx] = np.lexsort((identity_order, -points_matrix[sim_idx], -wins_matrix[sim_idx]))

    return seeds_idx


def _identity_columns(df: pd.DataFrame) -> tuple[str, str]:
    """Return (franchise_id, opponent_franchise_id). Raises if df is None/empty or columns are missing."""
    if df is None or df.empty:
        raise KeyError("franchise_id is required for manager identity")
    return get_manager_col(df), get_opponent_col(df)


def _filter_to_played_rows(df: pd.DataFrame, opp_col: str) -> pd.DataFrame:
    """Return rows where the team actually competed.

    Identity-safe predicate: team_points NOT NULL AND <opp_col> NOT NULL.
    Excludes phantom/bye rows of every category. Falls back to opp_col-only
    filtering for legacy fixtures without team_points.
    """
    if "team_points" in df.columns:
        return df[df["team_points"].notna() & df[opp_col].notna()].copy()
    return df[df[opp_col].notna()].copy()


def _display_name_lookup(df: pd.DataFrame | None) -> dict:
    """Build franchise_id -> latest display name mapping.

    Callers (rank_and_seed, history_snapshots) validate franchise_id via
    _identity_columns upstream, so this trusts the precondition and lets
    pandas raise KeyError loudly if either identity column is absent.
    """
    if df is None or df.empty:
        return {}

    display = df.loc[df["franchise_id"].notna() & df["manager"].notna()].copy()
    if display.empty:
        return {}

    cols = ["franchise_id", "manager"]
    if {"year", "week"}.issubset(display.columns):
        cols.extend(["year", "week"])
        display = display[cols].sort_values(["year", "week"]).drop_duplicates(subset=["franchise_id"], keep="last")
    else:
        display = display[cols].drop_duplicates(subset=["franchise_id"], keep="last")

    return dict(zip(display["franchise_id"], display["manager"]))


def _games_played_by_identity(played_df: pd.DataFrame | None) -> pd.Series:
    """Count distinct (franchise_id, year, week) tuples where the team actually competed.

    "Actually played" predicate uses team_points NOT NULL AND opponent_franchise_id
    NOT NULL — the identity-safe row-level "team competed" semantic from the
    2026-04-27 phantom-row policy. Excludes ALL bye rows (championship-eligible
    or not — that distinction lives in clinched_bye / eliminated_from_* and is
    covered separately by Bug #1.5).
    """
    if played_df is None or played_df.empty:
        return pd.Series(dtype=int)

    id_col, opp_col = _identity_columns(played_df)  # raises if franchise_id missing

    # Filter to rows where the team actually competed
    played = _filter_to_played_rows(played_df, opp_col)

    if played.empty:
        return pd.Series(dtype=int)

    frames = [
        played[[id_col, "year", "week"]].rename(columns={id_col: "name"}),
        played[[opp_col, "year", "week"]].rename(columns={opp_col: "name"}),
    ]
    mgr_weeks = pd.concat(frames, ignore_index=True).dropna(subset=["name", "year", "week"])
    if mgr_weeks.empty:
        return pd.Series(dtype=int)
    return mgr_weeks.drop_duplicates().groupby("name").size()


def get_head_to_head_record(team_a, team_b, played_df):
    """
    Calculate head-to-head record between two teams.
    Returns (wins_a, wins_b, ties).

    Optimized: Uses vectorized operations instead of iterrows.
    """
    if played_df is None or played_df.empty:
        return 0, 0, 0

    id_col, opp_col = _identity_columns(played_df)

    # Filter to games from team_a's perspective only (where team_a is manager)
    # This avoids double-counting since each game appears twice in the data.
    team_a_rows = played_df[(played_df[id_col] == team_a) & (played_df[opp_col] == team_b)].copy()

    if team_a_rows.empty:
        return 0, 0, 0

    # Ensure 'win' column exists with proper default
    if "win" not in team_a_rows.columns:
        team_a_rows["win"] = np.nan

    # Count from team_a's perspective
    wins_a = int((team_a_rows["win"] == 1).sum())
    wins_b = int((team_a_rows["win"] == 0).sum())

    # Ties: games where win is neither 0 nor 1 (or NaN)
    total_games = len(team_a_rows)
    ties = total_games - wins_a - wins_b
    ties = max(0, ties)  # Safety check

    return wins_a, wins_b, ties


def compare_by_tiebreaker(team_a, team_b, played_df, stats_df, method):
    """
    Compare two teams by a specific tiebreaker method.
    Returns: 1 if team_a wins, -1 if team_b wins, 0 if still tied.
    """
    if method == "total_points":
        pf_a = stats_df.loc[team_a, "PF"] if team_a in stats_df.index else 0
        pf_b = stats_df.loc[team_b, "PF"] if team_b in stats_df.index else 0
        if pf_a > pf_b:
            return 1
        elif pf_b > pf_a:
            return -1
        return 0

    elif method == "head_to_head":
        wins_a, wins_b, _ = get_head_to_head_record(team_a, team_b, played_df)
        if wins_a > wins_b:
            return 1
        elif wins_b > wins_a:
            return -1
        return 0

    elif method == "points_against":
        # Lower points against is better
        pa_a = stats_df.loc[team_a, "PA"] if team_a in stats_df.index else 0
        pa_b = stats_df.loc[team_b, "PA"] if team_b in stats_df.index else 0
        if pa_a < pa_b:
            return 1
        elif pa_b < pa_a:
            return -1
        return 0

    elif method == "victory_margin":
        margin_a = stats_df.loc[team_a, "avg_margin"] if team_a in stats_df.index else 0
        margin_b = stats_df.loc[team_b, "avg_margin"] if team_b in stats_df.index else 0
        if margin_a > margin_b:
            return 1
        elif margin_b > margin_a:
            return -1
        return 0

    return 0  # Unknown method, no decision


def resolve_tiebreaker(tied_teams, played_df, stats_df, tiebreaker_order=None):
    """
    Resolve ties between multiple teams using ordered tiebreaker methods.

    Args:
        tied_teams: List of team names with same record
        played_df: DataFrame of played games (for H2H calculation)
        stats_df: DataFrame with columns PF, PA, avg_margin indexed by manager
        tiebreaker_order: List of tiebreaker methods in priority order

    Returns:
        List of teams sorted by tiebreaker (best first)
    """
    if tiebreaker_order is None:
        tiebreaker_order = DEFAULT_TIEBREAKER_ORDER

    if len(tied_teams) <= 1:
        return list(tied_teams)

    def compare_teams(a, b):
        for method in tiebreaker_order:
            result = compare_by_tiebreaker(a, b, played_df, stats_df, method)
            if result != 0:
                return -result  # Negative because sorted() is ascending
        return 0  # Still tied after all methods

    return sorted(tied_teams, key=cmp_to_key(compare_teams))


def calculate_standings_stats(played_df):
    """
    Calculate stats needed for tiebreakers from played games.
    Returns DataFrame with PF, PA, avg_margin indexed by franchise_id.

    Filters to "actually played" rows (team_points NOT NULL AND opponent_franchise_id
    NOT NULL) — excludes phantom/bye rows of every category so PF/PA/avg_margin
    tiebreakers are not contaminated by non-games.
    """
    if played_df is None or played_df.empty:
        return pd.DataFrame(columns=["PF", "PA", "avg_margin"])

    _id_col, _opp_col = _identity_columns(played_df)

    # Filter to rows where the team actually competed
    played_df = _filter_to_played_rows(played_df, _opp_col)

    if played_df.empty:
        return pd.DataFrame(columns=["PF", "PA", "avg_margin"])

    # Points For
    pf = played_df.groupby(_id_col)["team_points"].sum()

    # Points Against - try to get from opponent_points column first
    if "opponent_points" in played_df.columns and not played_df["opponent_points"].isna().all():
        pa = played_df.groupby(_id_col)["opponent_points"].sum()
    else:
        # Calculate from opponent's scores by joining
        played_df = played_df.copy()
        try:
            opp_points = played_df.set_index(["year", "week", _id_col])["team_points"].to_dict()
            played_df["opponent_points"] = played_df.apply(
                lambda row: opp_points.get((row["year"], row["week"], row[_opp_col]), np.nan), axis=1
            )
            pa = played_df.groupby(_id_col)["opponent_points"].sum()
        except Exception:
            pa = pd.Series(0.0, index=pf.index)

    # Average margin
    played_df = played_df.copy()
    if "opponent_points" in played_df.columns:
        played_df["margin"] = played_df["team_points"] - played_df["opponent_points"]
        avg_margin = played_df.groupby(_id_col)["margin"].mean()
    else:
        avg_margin = pd.Series(0.0, index=pf.index)

    stats = pd.DataFrame({"PF": pf, "PA": pa, "avg_margin": avg_margin})

    return stats.fillna(0)


@ensure_normalized
def canonicalize(df: pd.DataFrame) -> pd.DataFrame:
    """Reduce to one row per matchup (canonical perspective).

    Requires franchise_id and opponent_franchise_id. Schedule data path
    empirically always has both post-Bug-#1.
    """
    df = df.copy()
    # Filter out bye rows — they have no opponent and aren't real matchups.
    # They are also the source of NaN cells in the id columns in
    # format-changing leagues (pigskin, etc.), which crash min/max below.
    if "opponent" in df.columns:
        df = df[df["opponent"].notna() & (df["opponent"].astype(str).str.strip() != "")]
    if df.empty:
        return df
    id_col, opp_col = _identity_columns(df)
    # Defense-in-depth: coerce id columns to string dtype to protect against
    # any residual NaN that slipped past the bye filter. pandas mixed-dtype
    # .min(axis=1) crashes with "str vs float" on a single NaN cell.
    df[id_col] = df[id_col].fillna("").astype(str)
    df[opp_col] = df[opp_col].fillna("").astype(str)
    df["mA"] = df[[id_col, opp_col]].min(axis=1)
    df["mB"] = df[[id_col, opp_col]].max(axis=1)
    df["match_key"] = list(zip(df["year"], df["week"], df["mA"], df["mB"]))
    return df[df[id_col] == df["mA"]]


def wins_points_to_date(played_raw: pd.DataFrame, use_median: bool = False) -> tuple[pd.Series, pd.Series]:
    """
    Calculate cumulative wins and points for each manager.

    For H2H+Median leagues (use_median=True), also adds median wins from the
    above_league_median column if available.

    Optimized: Uses vectorized operations instead of groupby transform with lambda.

    Args:
        played_raw: DataFrame with matchup data (manager, opponent, team_points, year, week)
        use_median: If True, include above_league_median wins (H2H+Median scoring)
    Returns:
        wins: Series of cumulative wins per manager
        points: Series of cumulative points per manager
    """
    # Use the optimized version from sql_aggregations
    return get_wins_points_to_date_fast(played_raw, use_median=use_median)


def rank_and_seed(
    wins: pd.Series,
    points: pd.Series,
    playoff_slots: int,
    bye_slots: int,
    played_raw: pd.DataFrame,
    tiebreaker_order: list[str] | None = None,
    seeding_rule: str | None = None,
) -> pd.DataFrame:
    """
    Rank managers and determine playoff seeding.

    Seeding is determined by the normalized league setting:
    - default: total wins, then tiebreaker methods
    - points_for: total points, then wins

    Args:
        wins: Series of wins per manager
        points: Series of points per manager
        playoff_slots: Number of playoff spots
        bye_slots: Number of first-round byes
        played_raw: Matchup data for calculating games played and tiebreakers (required, non-empty)
        tiebreaker_order: List of tiebreaker methods (default: ["total_points", "head_to_head"])
        seeding_rule: Normalized setting from league_settings; defaults to record-first

    Returns:
        DataFrame with columns: seed, manager, franchise_id, W, L, PF, made_playoffs, bye

    Raises:
        ValueError: if played_raw is None or empty.
        KeyError: if played_raw lacks franchise_id or opponent_franchise_id (raised by _identity_columns).
    """
    if tiebreaker_order is None:
        tiebreaker_order = DEFAULT_TIEBREAKER_ORDER
    seeding_rule = normalize_playoff_seeding_rule(seeding_rule)

    if played_raw is None or played_raw.empty:
        raise ValueError("rank_and_seed requires non-empty played_raw")

    managers = [m for m in sorted(set(wins.index) | set(points.index)) if pd.notna(m)]
    w = wins.reindex(managers).fillna(0.0)
    pf = points.reindex(managers).fillna(0.0)

    _identity_columns(played_raw)  # raises KeyError if franchise_id absent
    games_played = _games_played_by_identity(played_raw).reindex(managers).fillna(0).astype(int)
    stats_df = calculate_standings_stats(played_raw)
    display_lookup = _display_name_lookup(played_raw)

    l = (games_played - w).clip(lower=0)  # noqa: E741 — domain convention for losses

    if seeding_rule == POINTS_FIRST_SEEDING:
        sorted_managers = sorted(managers, key=lambda m: (-pf.loc[m], -w.loc[m], str(m)))
    else:
        # Group managers by wins
        win_groups = {}
        for m in managers:
            win_count = w.loc[m]
            if win_count not in win_groups:
                win_groups[win_count] = []
            win_groups[win_count].append(m)

        # Sort each win group by tiebreakers
        sorted_managers = []
        for win_count in sorted(win_groups.keys(), reverse=True):
            tied_teams = win_groups[win_count]
            if len(tied_teams) == 1:
                sorted_managers.extend(tied_teams)
            else:
                # Resolve ties using tiebreaker system
                resolved = resolve_tiebreaker(tied_teams, played_raw, stats_df, tiebreaker_order)
                sorted_managers.extend(resolved)

    table = pd.DataFrame(
        {
            "franchise_id": sorted_managers,
            "manager": [display_lookup.get(m, m) for m in sorted_managers],
            "W": [w.loc[m] for m in sorted_managers],
            "L": [l.loc[m] for m in sorted_managers],
            "PF": [pf.loc[m] for m in sorted_managers],
        }
    )
    table["seed"] = np.arange(1, len(table) + 1)
    table["made_playoffs"] = table["seed"] <= playoff_slots
    table["bye"] = table["seed"] <= bye_slots
    return table[["seed", "manager", "franchise_id", "W", "L", "PF", "made_playoffs", "bye"]]


@ensure_normalized
def enforce_playoff_monotonicity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure once playoffs start, all subsequent weeks are marked as playoffs.
    This prevents data inconsistencies where later regular season weeks
    accidentally get marked as non-playoff.
    """
    df = df.copy()
    if "is_playoffs" not in df.columns:
        return df
    for yr in sorted(df["year"].dropna().unique().astype(int)):
        season_mask = df["year"] == yr
        po_weeks = df.loc[season_mask & (df["is_playoffs"] == 1), "week"].dropna()
        if po_weeks.empty:
            continue
        start_po = int(po_weeks.min())
        df.loc[season_mask & (df["week"] >= start_po), "is_playoffs"] = 1
    return df


def schedules_last_regular_week(df_sched: pd.DataFrame, season: int) -> int | None:
    """Find the last regular season week for a given season."""
    # Handle empty schedule or missing required columns
    if df_sched.empty or "year" not in df_sched.columns:
        return None
    s = df_sched[(df_sched["year"] == season)]
    if s.empty:
        return None
    po = s.loc[s["is_playoffs"] == 1, "week"].dropna()
    if not po.empty:
        return int(po.min()) - 1
    return int(s["week"].max())


@ensure_normalized
def future_regular_from_schedule(df_sched: pd.DataFrame, season: int, current_week: int) -> pd.DataFrame:
    """
    Extract future regular season games from schedule.
    Args:
        df_sched: Schedule DataFrame
        season: Current season
        current_week: Current week
    Returns:
        DataFrame with columns: year, week, manager, opponent
    """
    # Handle empty schedule or missing required columns
    if df_sched.empty or "year" not in df_sched.columns:
        return pd.DataFrame(columns=["year", "week", "manager", "opponent"])

    # Guard: schedule must have manager/opponent columns for canonicalization
    if "manager" not in df_sched.columns or "opponent" not in df_sched.columns:
        return pd.DataFrame(columns=["year", "week", "manager", "opponent"])
    s = df_sched[(df_sched["year"] == season) & (df_sched["is_playoffs"] == 0)]
    if s.empty:
        return pd.DataFrame(columns=["year", "week", "manager", "opponent"])
    s = s[s["week"] > current_week].copy()
    if s.empty:
        return s[["year", "week", "manager", "opponent"]]
    # Canonicalize to avoid double-counting matchups
    s = canonicalize(s)
    return s[["year", "week", "manager", "opponent"]].drop_duplicates()


def history_snapshots(all_games: pd.DataFrame, playoff_slots: int, use_median: bool = False) -> pd.DataFrame:
    """
    Generate historical snapshots for kernel-based seed prediction.
    For each season and week, captures:
    - Current record (W, L)
    - Points percentile
    - Final playoff outcome (made_playoffs, final_seed)
    This data is used for empirical seed distribution estimation.
    Args:
        all_games: All historical matchup data
        playoff_slots: Number of playoff spots
        use_median: If True, include above_league_median wins (H2H+Median scoring)
    Returns:
        DataFrame with columns: year, week, manager, W, L, PF_pct, made_playoffs, final_seed
    """
    rows = []
    seasons = sorted(all_games["year"].dropna().unique().astype(int))
    for yr in seasons:
        reg = all_games[(all_games["year"] == yr) & (all_games["is_playoffs"] == 0)]
        if reg.empty:
            continue
        _identity_columns(reg)  # validates franchise_id is present, raises otherwise
        display_lookup = _display_name_lookup(reg)
        # Final standings for this season
        wins_f, pts_f = wins_points_to_date(reg, use_median=use_median)
        final_table = rank_and_seed(wins_f, pts_f, playoff_slots, 2, played_raw=reg)
        final_id_col = "franchise_id"
        final_seed_map = dict(zip(final_table[final_id_col], final_table["seed"]))
        made = set(final_table.loc[final_table["made_playoffs"], final_id_col])
        # Snapshot at each week
        weeks = sorted(reg["week"].dropna().unique().astype(int))
        for w in weeks:
            played = reg[reg["week"] <= w]
            wins_w, pts_w = wins_points_to_date(played, use_median=use_median)
            # Games played
            gp = _games_played_by_identity(played)
            mgrs = sorted(set(wins_w.index) | set(gp.index) | set(pts_w.index))
            if not mgrs:
                continue
            W = wins_w.reindex(mgrs).fillna(0.0)
            PF = pts_w.reindex(mgrs).fillna(0.0)
            GP = gp.reindex(mgrs).fillna(0).astype(int)
            L = (GP - W).clip(lower=0).astype(int)
            pf_pct = 100.0 * PF.rank(pct=True)
            for m in mgrs:
                row = {
                    "year": yr,
                    "week": int(w),
                    "manager": display_lookup.get(m, m),
                    "W": float(W.loc[m]),
                    "L": float(L.loc[m]),
                    "PF_pct": float(pf_pct.loc[m]),
                    "made_playoffs": 1.0 if m in made else 0.0,
                    "final_seed": final_seed_map.get(m, np.nan),
                }
                row["franchise_id"] = m
                rows.append(row)
    return pd.DataFrame(rows)


def gaussian_kernel(d2: np.ndarray) -> np.ndarray:
    """Gaussian kernel for distance weighting."""
    return np.exp(-0.5 * d2)


@ensure_normalized
def empirical_kernel_seed_dist(
    played_raw: pd.DataFrame,
    week: int,
    history_snapshots_df: pd.DataFrame,
    n_teams: int,
    h_W: float = 0.9,
    h_L: float = 0.9,
    h_PF: float = 15.0,
    h_week: float = 0.9,
    prior_strength: float = 3.0,
    use_median: bool = False,
) -> pd.DataFrame:
    """
    Empirical kernel-based seed distribution estimation.
    Uses historical snapshots with similar records to predict seed probabilities.
    Args:
        played_raw: Current season games played
        week: Current week
        history_snapshots_df: Historical snapshot data
        n_teams: Number of teams in league
        h_W: Bandwidth for wins
        h_L: Bandwidth for losses
        h_PF: Bandwidth for points percentile
        h_week: Bandwidth for week
        prior_strength: Dirichlet prior strength
        use_median: If True, include above_league_median wins (H2H+Median scoring)
    Returns:
        DataFrame with seed probabilities (managers x seeds)
    """
    if played_raw.empty or history_snapshots_df.empty:
        return pd.DataFrame()
    wins_w, pts_w = wins_points_to_date(played_raw, use_median=use_median)
    gp = _games_played_by_identity(played_raw)
    mgrs = sorted(set(wins_w.index) | set(gp.index) | set(pts_w.index))
    if not mgrs:
        return pd.DataFrame()
    W = wins_w.reindex(mgrs).fillna(0.0)
    PF = pts_w.reindex(mgrs).fillna(0.0)
    GP = gp.reindex(mgrs).fillna(0).astype(int)
    L = (GP - W).clip(lower=0).astype(int)
    pf_pct = 100.0 * PF.rank(pct=True)
    H = history_snapshots_df.dropna(subset=["final_seed"]).copy()
    seeds = np.arange(1, n_teams + 1)
    cols = list(seeds)
    out = pd.DataFrame(0.0, index=mgrs, columns=cols)
    alpha0 = np.ones(n_teams) * (prior_strength / n_teams)
    H_seed = H["final_seed"].to_numpy()
    for m in mgrs:
        xW, xL, xPF, xwk = float(W.loc[m]), float(L.loc[m]), float(pf_pct.loc[m]), float(week)
        # Calculate squared distances
        dW = (H["W"].to_numpy() - xW) / max(1e-6, h_W)
        dL = (H["L"].to_numpy() - xL) / max(1e-6, h_L)
        dPF = (H["PF_pct"].to_numpy() - xPF) / max(1e-6, h_PF)
        dWK = (H["week"].to_numpy() - xwk) / max(1e-6, h_week)
        d2 = dW * dW + dL * dL + dPF * dPF + dWK * dWK
        # Kernel weights
        w = gaussian_kernel(d2)
        # Count weighted occurrences of each seed
        counts = np.zeros(n_teams)
        for k in range(1, n_teams + 1):
            counts[k - 1] = float(w[(H_seed == k)].sum())
        # Bayesian smoothing with Dirichlet prior
        probs = (counts + alpha0) / (counts.sum() + alpha0.sum() + 1e-12)
        out.loc[m, cols] = probs
    return out


@ensure_normalized
def normalize_seed_matrix_to_100(seed_df: pd.DataFrame) -> pd.DataFrame:
    """Normalize seed probability matrix so each seed sums to 100%."""
    if seed_df is None or seed_df.empty:
        return seed_df
    seed_df = seed_df.copy()
    for col in seed_df.columns:
        col_sum = float(seed_df[col].sum())
        seed_df[col] = seed_df[col] * (100.0 / col_sum) if col_sum > 0 else 0.0
    return seed_df


def p_playoffs_from_seeds(seed_df: pd.DataFrame, slots: int) -> pd.Series:
    """Calculate playoff probability from seed distribution."""
    if seed_df is None or seed_df.empty:
        return pd.Series(dtype=float)
    cols = [c for c in seed_df.columns if isinstance(c, int) and 1 <= c <= slots]
    return seed_df[cols].sum(axis=1)


def p_bye_from_seeds(seed_df: pd.DataFrame, bye_slots: int) -> pd.Series:
    """Calculate bye probability from seed distribution."""
    if seed_df is None or seed_df.empty:
        return pd.Series(dtype=float)
    cols = [c for c in seed_df.columns if isinstance(c, int) and 1 <= c <= bye_slots]
    return seed_df[cols].sum(axis=1)
