import argparse
import sys
import numpy as np
import pandas as pd
from pathlib import Path
import time

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

from core.league_context import LeagueContext
from multi_league.core.identity import get_manager_col
from transformations.matchup.modules.playoff_helpers import (
    canonicalize,
    wins_points_to_date,
    rank_and_seed,
    history_snapshots,
    normalize_seed_matrix_to_100,
    p_playoffs_from_seeds,
    p_bye_from_seeds,
    normalize_playoff_seeding_rule,
    seed_indices_from_totals,
)
from transformations.matchup.modules.playoff_bracket import (
    load_league_settings,
    create_round_matchups,
)
from transformations.matchup.modules.playoff_config import PlayoffConfig, DEFAULT_TIEBREAKER_ORDER, round_weeks
from transformations.matchup.modules.playoff_holdover import (
    ensure_all_managers_have_playoff_rows,
    hold_playoff_values_for_eliminated,
)
from transformations.matchup.modules.probability_normalization import (
    bye_advances_to_semis,
    compress_and_normalize_probabilities,
    enforce_hierarchy,
)
from transformations.matchup.modules.team_model import (
    # Constants
    HALF_LIFE_WEEKS,
    SHRINK_K,
    N_SIMS,
    RNG_SEED,
    safe_mean,
    build_team_models,
    ensure_params_for_future,
    compute_power_ratings,
    _sim_game,
    _sim_game_score,
)
from multi_league.core.logging_config import get_logger
from multi_league.core.sql_utils import execute_scoped
from transformations.matchup.expected_record_v2 import detect_median_scoring
from transformations.matchup.modules.sql_aggregations import (
    simulate_playoff_bracket_vectorized,
    apply_completed_round_overrides,
    build_playoff_week_mask,
)
from multi_league.core.canonical_matchup import COLUMN_TYPES as MATCHUP_COLUMN_TYPES

# Initialize logger for this module
logger = get_logger(__name__)

CENTRAL_DB_NAME = "___leagues"
ACTIVE_TABLE_CATALOG = CENTRAL_DB_NAME


def current_catalog(conn) -> str:
    return conn.execute("SELECT current_database()").fetchone()[0]


def configure_table_catalog(conn) -> None:
    global ACTIVE_TABLE_CATALOG
    catalog = current_catalog(conn)
    if catalog == "memory" and ACTIVE_TABLE_CATALOG not in {CENTRAL_DB_NAME, "memory"}:
        return
    ACTIVE_TABLE_CATALOG = catalog


def _ensure_active_catalog(conn) -> None:
    if ACTIVE_TABLE_CATALOG != current_catalog(conn):
        configure_table_catalog(conn)


def matchup_table(conn) -> str:
    _ensure_active_catalog(conn)
    return f"{ACTIVE_TABLE_CATALOG}.public.matchup"


def schedule_table(conn) -> str:
    _ensure_active_catalog(conn)
    return f"{ACTIVE_TABLE_CATALOG}.public.schedule"


def league_db_filter(db_name: str, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}db_name = '{db_name}'"


def _ensure_matchup_output_columns(conn, db_name: str, output_cols: list[str]):
    unexpected = sorted(col for col in output_cols if col not in MATCHUP_COLUMN_TYPES)
    if unexpected:
        raise ValueError(f"Refusing to add non-canonical matchup columns for {db_name}: {', '.join(unexpected)}")

    matchup_sql = matchup_table(conn)
    existing = {r[0] for r in conn.execute(f"DESCRIBE {matchup_sql}").fetchall()}
    for col in output_cols:
        if col in existing:
            continue
        conn.execute(f'ALTER TABLE {matchup_sql} ADD COLUMN IF NOT EXISTS "{col}" {MATCHUP_COLUMN_TYPES[col]}')


def _normalize_x_win_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Move impossible x-win mass below current wins into the current bucket."""
    if "wins_to_date" not in df.columns:
        return df

    x_cols = []
    for col in df.columns:
        if not col.startswith("x") or not col.endswith("_win"):
            continue
        try:
            bucket = int(col[1 : col.index("_win")])
        except (TypeError, ValueError):
            continue
        x_cols.append((bucket, col))
    if not x_cols:
        return df

    x_cols.sort()
    bucket_to_col = dict(x_cols)
    max_bucket = x_cols[-1][0]
    rows_adjusted = 0

    wins = pd.to_numeric(df["wins_to_date"], errors="coerce")
    for idx, current_wins in wins.dropna().items():
        if pd.isna(df.at[idx, x_cols[0][1]]):
            continue
        min_bucket = int(np.ceil(float(current_wins) - 1e-9))
        if min_bucket <= 0:
            continue
        min_bucket = min(min_bucket, max_bucket)
        lower_cols = [col for bucket, col in x_cols if bucket < min_bucket]
        if not lower_cols:
            continue

        lower_values = pd.to_numeric(df.loc[idx, lower_cols], errors="coerce").fillna(0.0)
        lower_mass = float(lower_values.sum())
        if lower_mass <= 1e-9:
            continue

        target_col = bucket_to_col.get(min_bucket)
        if target_col:
            current_target = pd.to_numeric(pd.Series([df.at[idx, target_col]]), errors="coerce").fillna(0.0).iloc[0]
            df.at[idx, target_col] = float(current_target) + lower_mass
        df.loc[idx, lower_cols] = 0.0
        rows_adjusted += 1

    if rows_adjusted:
        logger.info("Normalized x-win distributions on %s rows", rows_adjusted)
    return df


def fill_missing_title_odds(df: pd.DataFrame) -> pd.DataFrame:
    """Carry title odds through a team's bye-week row and leave no null output cells.

    Playoff bye rows do not have a played matchup to simulate.  Their title odds
    are therefore the most recent (or, for an opening bye, next) team-week odds.
    A league with no calculable title odds receives the neutral zero value rather
    than a missing output field.
    """
    required = {"year", "week", "p_champ"}
    if not required.issubset(df.columns):
        return df

    team_col = "franchise_id" if "franchise_id" in df.columns else "manager"
    if team_col not in df.columns:
        return df

    result = df.copy()
    original_missing = pd.to_numeric(result["p_champ"], errors="coerce").isna()
    if not original_missing.any():
        return result

    result["p_champ"] = pd.to_numeric(result["p_champ"], errors="coerce")
    ordered = result.sort_values(["year", team_col, "week"], kind="stable")
    grouped = ordered.groupby(["year", team_col], dropna=False)["p_champ"]
    ordered["p_champ"] = grouped.ffill().groupby(
        [ordered["year"], ordered[team_col]], dropna=False
    ).bfill()
    result.loc[ordered.index, "p_champ"] = ordered["p_champ"]
    result["p_champ"] = result["p_champ"].fillna(0.0)

    logger.info("Filled missing title odds on %s matchup rows", int(original_missing.sum()))
    return result


# =========================================================
# Playoff Odds Engine – Multi-League Generic Version
# =========================================================

# Tiebreaker configuration
TIEBREAKER_METHODS = {
    "total_points": "total_points",  # Most common - total points for
    "head_to_head": "head_to_head",  # H2H record between tied teams
    "points_against": "points_against",  # Least points against (defense)
    "victory_margin": "victory_margin",  # Average margin of victory
}
# Global variables (set from league settings - NO DEFAULTS, must be loaded)
PLAYOFF_SLOTS = None
BYE_SLOTS = None
BRACKET_RESEED = None
TIEBREAKER_ORDER = DEFAULT_TIEBREAKER_ORDER
NUM_TEAMS = None
REGULAR_SEASON_WEEKS = None

# Target columns placeholder (populated dynamically after settings load)
TARGET_COLS = None


# -------------------------
# Round column names
# -------------------------
ROUND_COLS = {
    "qf": "quarterfinal",
    "sf": "semifinal",
    "fn": "championship",
}


# -------------------------
# RNG helper
# -------------------------
def get_rng(season: int, week: int, base: int = RNG_SEED) -> np.random.Generator:
    mix = int(base) ^ ((int(season) * 0x9E3779B1) & 0xFFFFFFFF) ^ ((int(week) * 0x85EBCA77) & 0xFFFFFFFF)
    return np.random.default_rng(mix & 0xFFFFFFFF)


def _simulate_bracket_milestones_generic(
    seed_orders: np.ndarray,
    mu_arr: np.ndarray,
    sd_arr: np.ndarray,
    rng: np.random.Generator,
    *,
    bye_slots: int,
    use_reseeding: bool,
    num_playoff_teams: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Simulate bracket milestones generically for any playoff field shape."""
    n_sims = seed_orders.shape[0]
    n_teams = len(mu_arr)
    reached_semis = np.zeros((n_sims, n_teams), dtype=np.int8)
    reached_final = np.zeros((n_sims, n_teams), dtype=np.int8)
    won_champ = np.zeros((n_sims, n_teams), dtype=np.int8)

    for sim_idx in range(n_sims):
        alive = [int(team_idx) for team_idx in seed_orders[sim_idx].tolist()]
        seeds_map = {team_idx: seed_num + 1 for seed_num, team_idx in enumerate(alive)}
        first_round_byes = set(alive[:bye_slots]) if bye_slots > 0 else set()
        round_num = 0

        while len(alive) > 1:
            if len(alive) <= 4:
                reached_semis[sim_idx, alive] = 1
            if len(alive) <= 2:
                reached_final[sim_idx, alive] = 1

            teams_playing = [team for team in alive if team not in first_round_byes] if round_num == 0 else list(alive)
            carry = []
            if len(teams_playing) % 2 != 0:
                carry = [min(teams_playing, key=lambda team: seeds_map[team])]
                teams_playing = [team for team in teams_playing if team not in carry]

            winners = []
            if round_num == 0 and first_round_byes:
                winners.extend(sorted(first_round_byes, key=lambda team: seeds_map[team]))
                first_round_byes = set()
            winners.extend(carry)

            matchups = create_round_matchups(
                teams_playing,
                seeds_map,
                use_reseeding,
                round_num,
                num_playoff_teams,
            )
            for team_a, team_b in matchups:
                score_a = rng.normal(mu_arr[team_a], sd_arr[team_a])
                score_b = rng.normal(mu_arr[team_b], sd_arr[team_b])
                if score_a > score_b:
                    winners.append(team_a)
                elif score_b > score_a:
                    winners.append(team_b)
                else:
                    winners.append(team_a if rng.integers(0, 2) == 0 else team_b)

            alive = winners
            round_num += 1

        if alive:
            won_champ[sim_idx, alive[0]] = 1

    return (
        100.0 * reached_semis.sum(axis=0) / float(n_sims),
        100.0 * reached_final.sum(axis=0) / float(n_sims),
        100.0 * won_champ.sum(axis=0) / float(n_sims),
    )


def enforce_playoff_monotonicity(df):
    df = df.copy()
    if "is_playoffs" not in df.columns:
        return df
    for yr in sorted(df["year"].dropna().unique().astype(int)):
        season_mask = df["year"] == yr
        po_weeks = df.loc[season_mask & (df["is_playoffs"] == 1), "week"].dropna()
        if po_weeks.empty:
            continue
        # Ensure week is numeric before calling min()
        po_weeks = pd.to_numeric(po_weeks, errors="coerce").dropna()
        if po_weeks.empty:
            continue
        start_po = int(po_weeks.min())
        df.loc[season_mask & (df["week"] >= start_po), "is_playoffs"] = 1
    return df


# -------------------------
# Schedule helpers
# -------------------------
def schedules_last_regular_week(df_sched, season):
    s = df_sched[(df_sched["year"] == season)]
    if s.empty:
        return None
    po = s.loc[s["is_playoffs"] == 1, "week"].dropna()
    if not po.empty:
        # Ensure week is numeric before calling min()
        po = pd.to_numeric(po, errors="coerce").dropna()
        if po.empty:
            return None
        return int(po.min()) - 1

    weeks = pd.to_numeric(s["week"], errors="coerce").dropna()
    if weeks.empty:
        return None
    return int(weeks.max())


def _derive_schedule_opponent_fids(schedule_df: pd.DataFrame) -> pd.DataFrame:
    """Fill opponent_franchise_id without collapsing duplicate display names."""
    if schedule_df.empty or "franchise_id" not in schedule_df.columns:
        return schedule_df

    s = schedule_df.copy()
    if "opponent_franchise_id" not in s.columns:
        s["opponent_franchise_id"] = pd.Series(pd.NA, index=s.index, dtype="object")
    elif not pd.api.types.is_object_dtype(s["opponent_franchise_id"]):
        s["opponent_franchise_id"] = s["opponent_franchise_id"].astype("object")

    unresolved_mask = s["opponent_franchise_id"].isna()
    if not unresolved_mask.any():
        return s

    base = s.reset_index().rename(columns={"index": "_orig_index"})
    reciprocal = base[["_orig_index", "year", "week", "manager", "opponent", "franchise_id"]].rename(
        columns={
            "_orig_index": "_reciprocal_row",
            "manager": "_reciprocal_manager",
            "opponent": "_reciprocal_opponent",
            "franchise_id": "_candidate_opp_fid",
        }
    )
    reciprocal_matches = base.merge(
        reciprocal,
        left_on=["year", "week", "manager", "opponent"],
        right_on=["year", "week", "_reciprocal_opponent", "_reciprocal_manager"],
        how="left",
    )
    reciprocal_matches = reciprocal_matches[
        (reciprocal_matches["_orig_index"] != reciprocal_matches["_reciprocal_row"])
        & reciprocal_matches["_candidate_opp_fid"].notna()
        & (reciprocal_matches["_candidate_opp_fid"] != reciprocal_matches["franchise_id"])
    ]

    if not reciprocal_matches.empty:
        reciprocal_counts = reciprocal_matches.groupby("_orig_index")["_candidate_opp_fid"].nunique()
        uniquely_resolved = reciprocal_counts[reciprocal_counts == 1].index
        if len(uniquely_resolved) > 0:
            reciprocal_map = (
                reciprocal_matches[reciprocal_matches["_orig_index"].isin(uniquely_resolved)]
                .drop_duplicates(subset=["_orig_index"])
                .set_index("_orig_index")["_candidate_opp_fid"]
            )
            s.loc[reciprocal_map.index, "opponent_franchise_id"] = reciprocal_map.values

    unresolved_mask = s["opponent_franchise_id"].isna()
    if not unresolved_mask.any():
        return s

    unique_name_rows = s[["manager", "franchise_id"]].dropna().drop_duplicates()
    if unique_name_rows.empty:
        return s

    unique_name_counts = unique_name_rows.groupby("manager")["franchise_id"].nunique()
    safe_names = unique_name_counts[unique_name_counts == 1].index
    if len(safe_names) == 0:
        return s

    safe_name_map = (
        unique_name_rows[unique_name_rows["manager"].isin(safe_names)]
        .drop_duplicates(subset=["manager"])
        .set_index("manager")["franchise_id"]
    )
    s.loc[unresolved_mask, "opponent_franchise_id"] = s.loc[unresolved_mask, "opponent"].map(safe_name_map)

    return s


def future_regular_from_schedule(df_sched, season, current_week):
    # Core columns + identity columns (opponent_franchise_id may be derived below)
    _core = ["year", "week", "manager", "opponent"]
    _id_cols = [c for c in ["franchise_id", "opponent_franchise_id"] if c in df_sched.columns]
    # Always include franchise_id + opponent_franchise_id in output if franchise_id exists
    # (opponent_franchise_id will be derived from manager→franchise_id mapping if missing)
    if "franchise_id" in df_sched.columns and "opponent_franchise_id" not in _id_cols:
        _id_cols.append("opponent_franchise_id")
    _out_cols = _core + _id_cols

    # Guard: schedule must have manager/opponent columns for canonicalization
    if df_sched.empty or "manager" not in df_sched.columns or "opponent" not in df_sched.columns:
        return pd.DataFrame(columns=_out_cols)

    # Get last regular season week to exclude consolation bracket games
    last_reg_week = schedules_last_regular_week(df_sched, season)

    s = df_sched[(df_sched["year"] == season) & (df_sched["is_playoffs"] == 0)]
    if s.empty:
        return pd.DataFrame(columns=_out_cols)

    # Only include games after current_week AND up to last_reg_week
    # This excludes consolation games that occur after playoffs start
    s = s[s["week"] > current_week].copy()
    if last_reg_week is not None:
        s = s[s["week"] <= last_reg_week].copy()

    if s.empty:
        return s[[c for c in _out_cols if c in s.columns]]

    # Derive opponent_franchise_id if schedule only has franchise_id
    # The schedule has both sides of each matchup (A-vs-B and B-vs-A),
    # so we can build the mapping from manager → franchise_id within the schedule itself.
    if "franchise_id" in s.columns and "opponent_franchise_id" not in s.columns:
        s = _derive_schedule_opponent_fids(s)
        if "opponent_franchise_id" not in _out_cols:
            _out_cols.append("opponent_franchise_id")

    s = canonicalize(s.rename(columns={"Opponent Week": "opponent_week", "OpponentYear": "opponent_year"}))
    keep = [c for c in _out_cols if c in s.columns]
    return s[keep].drop_duplicates()


# -------------------------
# Vectorized regular and bracket sim
# -------------------------
def _vectorized_regular_and_bracket(
    reg_to_date,
    future_canon,
    mu_hat,
    sigma_hat,
    season,
    week,
    playoff_slots=None,
    bye_slots=None,
    bracket_reseed=None,
    regular_season_weeks=None,
    num_teams=None,
    use_median=False,
    seeding_rule=None,
):
    t0 = time.time()
    rng = get_rng(season, week)

    # Use provided playoff configuration or fallback to globals
    _playoff_slots = playoff_slots if playoff_slots is not None else PLAYOFF_SLOTS
    _bye_slots = bye_slots if bye_slots is not None else BYE_SLOTS
    _bracket_reseed = bracket_reseed if bracket_reseed is not None else BRACKET_RESEED
    _regular_season_weeks = regular_season_weeks if regular_season_weeks is not None else REGULAR_SEASON_WEEKS
    _num_teams = num_teams if num_teams is not None else NUM_TEAMS
    _seeding_rule = normalize_playoff_seeding_rule(seeding_rule)

    # Validate required parameters
    if _regular_season_weeks is None:
        raise ValueError("regular_season_weeks must be provided (load from league settings)")
    if _num_teams is None:
        raise ValueError("num_teams must be provided (load from league settings)")

    # Build manager list from BOTH reg_to_date AND future_canon to ensure all managers are indexed
    # This prevents IndexError when future schedule contains managers not yet in reg_to_date
    # IMPORTANT: Use the same identity column that wins_points_to_date() will use
    _mgr_col = get_manager_col(reg_to_date)
    _opp_col = (
        "opponent_franchise_id"
        if _mgr_col == "franchise_id" and "opponent_franchise_id" in reg_to_date.columns
        else "opponent"
    )
    manager_set = set(reg_to_date[_mgr_col].dropna().unique()) | set(reg_to_date[_opp_col].dropna().unique())
    if future_canon is not None and not future_canon.empty:
        _fc_mgr = get_manager_col(future_canon)
        _fc_opp = _opp_col if _opp_col in future_canon.columns else "opponent"
        manager_set |= set(future_canon[_fc_mgr].dropna().unique())
        manager_set |= set(future_canon[_fc_opp].dropna().unique())
    # Remove any NaN values that may have slipped through
    manager_set = {m for m in manager_set if pd.notna(m) and m != ""}
    managers = sorted(manager_set)
    mgr2idx = {m: i for i, m in enumerate(managers)}
    idx2mgr = np.array(managers)
    n = len(managers)

    # Cap playoff slots to actual number of teams (league may have fewer managers in early years)
    if _playoff_slots > n:
        logger.warning(
            f"[PLAYOFF_ODDS] playoff_slots ({_playoff_slots}) > actual teams ({n}) for season {season} - capping to {n}"
        )
        _playoff_slots = n
        # Recalculate byes: need power-of-2 bracket
        _next_p2 = 1
        while _next_p2 < _playoff_slots:
            _next_p2 *= 2
        _bye_slots = min(_bye_slots, _next_p2 - _playoff_slots)

    # Guard against empty data - no managers found means no teams to simulate
    if n == 0:
        logger.warning(f"[PLAYOFF_ODDS] No managers found for season {season} week {week} - returning empty results")
        empty_index = pd.Index([], name="manager")
        empty_series = pd.Series([], dtype=float, index=empty_index)
        series_pack = {
            "Exp_Final_Wins": empty_series.copy(),
            "Exp_Final_PF": empty_series.copy(),
            "Avg_Seed": empty_series.copy(),
            "P_Playoffs_SIM": empty_series.copy(),
            "P_Bye_SIM": empty_series.copy(),
            "P_Semis_SIM": empty_series.copy(),
            "P_Final": empty_series.copy(),
            "P_Champ": empty_series.copy(),
            "P_QFWin_Given_NonBye_SIM": empty_series.copy(),
            "P_SFWin_Given_NonBye_SIM": empty_series.copy(),
            "P_WinFinal_Given_Final_SIM": empty_series.copy(),
        }
        seed_df = pd.DataFrame()
        win_df = pd.DataFrame()
        return series_pack, seed_df, win_df

    wins_to_date, pf_to_date = wins_points_to_date(reg_to_date, use_median=use_median)
    WTD = np.zeros(n, dtype=float)
    PFTD = np.zeros(n, dtype=float)
    for m, v in wins_to_date.items():
        WTD[mgr2idx[m]] = v
    for m, v in pf_to_date.items():
        PFTD[mgr2idx[m]] = v

    # Use safe fallbacks for missing managers
    mu_default = safe_mean(mu_hat.values(), default=100.0)
    sd_default = safe_mean(sigma_hat.values(), default=15.0)
    mu_arr = np.array([mu_hat.get(m, mu_default) for m in managers])
    sd_arr = np.array([sigma_hat.get(m, sd_default) for m in managers])

    if future_canon is None or future_canon.empty:
        PF_all = np.tile(PFTD, (N_SIMS, 1))
        W_all = np.tile(WTD, (N_SIMS, 1))
    else:
        A = future_canon[_fc_mgr].map(mgr2idx).to_numpy()
        B = future_canon[_fc_opp].map(mgr2idx).to_numpy()

        # Safety check: Filter out any rows where manager or opponent couldn't be mapped
        # This handles edge cases where schedule has managers not in the matchup data
        valid_mask = ~(np.isnan(A.astype(float)) | np.isnan(B.astype(float)))
        if not valid_mask.all():
            unmapped_count = (~valid_mask).sum()
            logger.warning(f" Filtering out {unmapped_count} future games with unmapped managers")
            A = A[valid_mask].astype(int)
            B = B[valid_mask].astype(int)
        else:
            A = A.astype(int)
            B = B.astype(int)

        G = A.size

        if G == 0:
            # No valid future games - treat as if no future schedule
            PF_all = np.tile(PFTD, (N_SIMS, 1))
            W_all = np.tile(WTD, (N_SIMS, 1))
        else:
            SA = rng.normal(mu_arr[A][None, :], sd_arr[A][None, :], size=(N_SIMS, G))
            SB = rng.normal(mu_arr[B][None, :], sd_arr[B][None, :], size=(N_SIMS, G))

            PF_add = np.zeros((N_SIMS, n), dtype=float)
            rows = np.arange(N_SIMS)[:, None]
            np.add.at(PF_add, (rows, A[None, :]), SA)
            np.add.at(PF_add, (rows, B[None, :]), SB)
            PF_all = PF_add + PFTD

            a_wins = (SA > SB).astype(float)
            ties = SA == SB
            if ties.any():
                coin = rng.integers(0, 2, size=ties.shape)
                a_wins = np.where(ties, coin, a_wins)
            b_wins = 1.0 - a_wins

            W_add = np.zeros((N_SIMS, n), dtype=float)
            np.add.at(W_add, (rows, A[None, :]), a_wins)
            np.add.at(W_add, (rows, B[None, :]), b_wins)

            # H2H+Median: add median wins per simulated week
            if use_median:
                future_weeks = future_canon["week"].to_numpy()
                unique_weeks = np.unique(future_weeks)
                # Build a per-team score matrix for median calculation
                # For each future week, collect all simulated scores and compute median
                for fw in unique_weeks:
                    week_game_mask = future_weeks == fw
                    week_SA = SA[:, week_game_mask]  # (N_SIMS, games_in_week)
                    week_SB = SB[:, week_game_mask]
                    week_A = A[week_game_mask]
                    week_B = B[week_game_mask]
                    # Collect all scores for this week into a (N_SIMS, n_teams_playing) array
                    all_scores = np.concatenate([week_SA, week_SB], axis=1)
                    # Median per sim
                    week_median = np.median(all_scores, axis=1, keepdims=True)  # (N_SIMS, 1)
                    # Check each team's score against median
                    a_above = (week_SA > week_median).astype(float)
                    b_above = (week_SB > week_median).astype(float)
                    np.add.at(W_add, (rows, week_A[None, :]), a_above)
                    np.add.at(W_add, (rows, week_B[None, :]), b_above)

            W_all = W_add + WTD

    seeds_idx = seed_indices_from_totals(W_all, PF_all, _seeding_rule)

    inv_seed = np.full((N_SIMS, n), n - 1, dtype=int)
    row_idx = np.arange(N_SIMS)[:, None]
    inv_seed[row_idx, seeds_idx] = np.arange(n)[None, :]
    seed_numbers = inv_seed + 1

    if _playoff_slots <= 1:
        logger.info(f"[PLAYOFF_ODDS] playoffs disabled for season {season}; " "using seed 1 as championship odds")

        exp_final_wins = W_all.mean(axis=0)
        exp_final_pf = PF_all.mean(axis=0)
        avg_seed = seed_numbers.mean(axis=0)

        max_wins = min(36, _regular_season_weeks * (2 if use_median else 1))
        W_int = np.rint(W_all).astype(int)
        W_int = np.clip(W_int, 0, max_wins)
        win_prob_mat = np.zeros((n, max_wins + 1), dtype=float)
        for k in range(0, max_wins + 1):
            mask = (W_int == k).astype(np.int32)
            counts_k = mask.sum(axis=0).astype(float)
            win_prob_mat[:, k] = 100.0 * counts_k / float(N_SIMS)

        seed_mat = np.zeros((n, n), dtype=float)
        for pos in range(n):
            teams_at_pos = seeds_idx[:, pos]
            seed_mat[:, pos] = np.bincount(teams_at_pos, minlength=n) / N_SIMS * 100.0
        seed_df = pd.DataFrame(seed_mat, index=idx2mgr, columns=list(range(1, n + 1)))

        s_index = pd.Index(idx2mgr, name="manager")
        zero_series = pd.Series(0.0, index=s_index)
        p_champ_top_seed = pd.Series(seed_mat[:, 0], index=s_index)
        series_pack = {
            "Avg_Seed": pd.Series(avg_seed, index=s_index),
            "Exp_Final_Wins": pd.Series(exp_final_wins, index=s_index),
            "Exp_Final_PF": pd.Series(exp_final_pf, index=s_index),
            "P_Playoffs_SIM": zero_series.copy(),
            "P_Bye_SIM": zero_series.copy(),
            "P_Semis_SIM": zero_series.copy(),
            "P_Final": zero_series.copy(),
            "P_Champ": p_champ_top_seed,
            "P_QFWin_Given_NoBye_SIM": zero_series.copy(),
            "P_SFWin_Given_Bye_SIM": zero_series.copy(),
            "P_SFWin_Given_NonBye_SIM": zero_series.copy(),
            "P_WinFinal_Given_Final_SIM": zero_series.copy(),
        }

        win_cols = [f"x{k}_win" for k in range(max_wins + 1)]
        win_df = pd.DataFrame(win_prob_mat, index=idx2mgr, columns=win_cols)
        return series_pack, seed_df, win_df

    # DYNAMIC BRACKET STRUCTURE - Uses _playoff_slots and _bye_slots from parameters

    # Handle odd-number brackets by adding implicit byes
    # This ensures all teams can be paired in round 1
    # e.g., 7 teams with 0 byes -> add 1 implicit bye -> seed 1 gets bye, seeds 2-7 play round 1
    teams_playing_round1 = _playoff_slots - _bye_slots
    if teams_playing_round1 % 2 != 0:
        implicit_byes = 1  # Add 1 bye to make round 1 teams even
        logger.warning(
            f"Odd number of teams ({teams_playing_round1}) in round 1 - "
            f"adding {implicit_byes} implicit bye(s) for top seed(s)"
        )
        _bye_slots += implicit_byes

    # Extract bye teams (top _bye_slots seeds)
    bye_teams = [seeds_idx[:, i] for i in range(_bye_slots)]

    # Extract first round teams (seeds _bye_slots+1 through _playoff_slots)
    first_round_teams = [seeds_idx[:, i] for i in range(_bye_slots, _playoff_slots)]

    # Determine first round matchups (high seed vs low seed)
    # For 6 teams with 2 byes: 3v6, 4v5
    # For 8 teams with 4 byes: 5v8, 6v7
    num_first_round_games = len(first_round_teams) // 2
    first_round_winners = []

    for game_idx in range(num_first_round_games):
        # Pair highest remaining seed with lowest remaining seed
        higher_seed = first_round_teams[game_idx]
        lower_seed = first_round_teams[-(game_idx + 1)]

        # Simulate game
        ScoreA = rng.normal(mu_arr[higher_seed], sd_arr[higher_seed])
        ScoreB = rng.normal(mu_arr[lower_seed], sd_arr[lower_seed])
        ties = ScoreA == ScoreB
        coin = rng.integers(0, 2, size=ties.shape[0])
        winner = np.where(
            ScoreA > ScoreB,
            higher_seed,
            np.where(ScoreB > ScoreA, lower_seed, np.where(coin == 0, higher_seed, lower_seed)),
        )
        first_round_winners.append(winner)

    # Semifinals: bye teams face first round winners
    # Standard bracket (no reseeding): 1 vs winner of lower game, 2 vs winner of higher game
    # With reseeding: 1 vs lowest seed, 2 vs next lowest seed
    #
    # CRITICAL: semi_teams tracks who is IN the semifinal round, not who WINS it.
    # - For 6-team bracket with 2 byes: bye_teams (2) + first_round_winners (2) = 4 semifinalists
    # - For 4-team bracket with 0 byes: ALL first_round_teams (4) ARE the semifinalists
    #   (the first round IS the semifinal round)
    # - For 8-team bracket with 0 byes: first_round is QUARTERFINALS, only 4 winners make semis
    if _bye_slots == 0:
        if _playoff_slots <= 4:
            # 4-team or smaller: first round IS the semifinals
            semi_teams = first_round_teams
        else:
            # 8+ team bracket with no byes: first round is quarterfinals
            # Only the winners advance to semifinals
            semi_teams = first_round_winners
    else:
        # With byes: bye teams + first round winners make up the semifinalists
        semi_teams = bye_teams + first_round_winners

    # Handle bracket matchups for the next round
    semi_matchups = []

    if _bye_slots == 0:
        # No bye teams: first_round_winners play in the next round
        # - 4-team bracket: 2 first_round_winners play in finals (first round WAS semis)
        # - 8-team bracket: 4 first_round_winners play in semifinals (first round was quarters)
        num_semi_games = len(first_round_winners) // 2

        if not _bracket_reseed:
            # Fixed bracket: pair high seed winner vs low seed winner
            # For 4-team: winner of 1v4 plays winner of 2v3
            for i in range(num_semi_games):
                team_a = first_round_winners[i]
                team_b = first_round_winners[-(i + 1)]
                semi_matchups.append((team_a, team_b))
        else:
            # Reseeding: pair best remaining seed with worst remaining seed
            sorted_winners = []
            for sim_idx in range(N_SIMS):
                winners_seeds = [
                    (first_round_winners[j][sim_idx], inv_seed[sim_idx, first_round_winners[j][sim_idx]])
                    for j in range(len(first_round_winners))
                ]
                winners_seeds.sort(key=lambda x: x[1])
                sorted_winners.append([w[0] for w in winners_seeds])

            for i in range(num_semi_games):
                team_a = np.array([sorted_winners[s][i] for s in range(N_SIMS)])
                team_b = np.array([sorted_winners[s][-(i + 1)] for s in range(N_SIMS)])
                semi_matchups.append((team_a, team_b))

    elif not _bracket_reseed:
        # Fixed bracket: each bye team faces predetermined opponent
        # For 6-team, 2-bye: seed1 faces winner of 4v5, seed2 faces winner of 3v6
        # Generically: bye team i faces first_round_winners[num_games - 1 - i]
        for i in range(_bye_slots):
            bye_team = bye_teams[i]
            opponent = (
                first_round_winners[num_first_round_games - 1 - i]
                if i < num_first_round_games
                else first_round_winners[0]
            )
            semi_matchups.append((bye_team, opponent))
    else:
        # Reseeding: pair best remaining seed with worst remaining seed
        # Sort all remaining teams by seed, then pair 1vworst, 2vsecond-worst
        all_remaining = bye_teams + first_round_winners
        # Sort by seed (lower seed number = better seed)
        sorted_teams = []
        for sim_idx in range(N_SIMS):
            teams_seeds = [(team[sim_idx], inv_seed[sim_idx, team[sim_idx]]) for team in all_remaining]
            teams_seeds.sort(key=lambda x: x[1])
            sorted_teams.append([t[0] for t in teams_seeds])

        # Pair best with worst
        for i in range(_bye_slots):
            team_a = np.array([sorted_teams[s][i] for s in range(N_SIMS)])
            team_b = np.array([sorted_teams[s][-(i + 1)] for s in range(N_SIMS)])
            semi_matchups.append((team_a, team_b))

    # Simulate semifinals
    semi_winners = []
    for team_a, team_b in semi_matchups:
        ScoreA = rng.normal(mu_arr[team_a], sd_arr[team_a])
        ScoreB = rng.normal(mu_arr[team_b], sd_arr[team_b])
        ties = ScoreA == ScoreB
        coin = rng.integers(0, 2, size=ties.shape[0])
        winner = np.where(
            ScoreA > ScoreB, team_a, np.where(ScoreB > ScoreA, team_b, np.where(coin == 0, team_a, team_b))
        )
        semi_winners.append(winner)

    # Championship game
    # In a two-team bracket the first playoff matchup is the championship;
    # there is no semifinal layer to populate semi_winners.
    if len(semi_winners) == 0 and _playoff_slots == 2 and len(first_round_winners) == 1:
        s1_w = first_round_teams[0]
        s2_w = first_round_teams[1]
        champ = first_round_winners[0]
    elif len(semi_winners) == 1:
        # Only 1 semifinal winner = champion (no separate championship game needed)
        # The semifinal winner IS the finalist AND champion
        s1_w = semi_winners[0]
        s2_w = semi_winners[0]  # Same as s1_w since there's only one finalist
        champ = semi_winners[0]
    elif len(semi_winners) >= 2:
        # Standard case: 2+ semifinal winners play for championship
        s1_w = semi_winners[0]
        s2_w = semi_winners[1]

        FA = rng.normal(mu_arr[s1_w], sd_arr[s1_w])
        FB = rng.normal(mu_arr[s2_w], sd_arr[s2_w])
        ties = FA == FB
        coin = rng.integers(0, 2, size=ties.shape[0])
        champ = np.where(FA > FB, s1_w, np.where(FB > FA, s2_w, np.where(coin == 0, s1_w, s2_w)))
    else:
        # No semifinal winners - should not happen, but handle gracefully
        raise ValueError(f"No semifinal winners found! semi_winners={semi_winners}")

    def pct_counts(idx_arr, nteams, n_sims=None):
        """Calculate percentage for each team appearing in the given array.

        Args:
            idx_arr: Array of team indices
            nteams: Total number of teams
            n_sims: Number of simulations to divide by. If None, uses arr.size.
                    Use N_SIMS for probabilities that can exceed 100% per team
                    (e.g., semifinals can have 4 teams, so sum should be 400%).
        """
        arr = np.asarray(idx_arr).reshape(-1).astype(np.int64, copy=False)
        if arr.size == 0:
            return np.zeros(nteams, dtype=float)
        c = np.bincount(arr, minlength=nteams).astype(float)
        divisor = n_sims if n_sims is not None else arr.size
        return 100.0 * c / divisor

    # Use the generic round simulator for milestone probabilities so rare field
    # sizes and bye shapes follow the same bracket math as the tracer.
    p_semis_sim, p_final_sim, p_champ_sim = _simulate_bracket_milestones_generic(
        seeds_idx[:, :_playoff_slots],
        mu_arr,
        sd_arr,
        rng,
        bye_slots=_bye_slots,
        use_reseeding=_bracket_reseed,
        num_playoff_teams=_playoff_slots,
    )

    exp_final_wins = W_all.mean(axis=0)
    exp_final_pf = PF_all.mean(axis=0)
    avg_seed = seed_numbers.mean(axis=0)

    # Dynamic win probability array based on league format.
    # H2H+median can award two wins per week, but we cap the canonical schema at 36 buckets.
    max_wins = min(36, _regular_season_weeks * (2 if use_median else 1))
    W_int = np.rint(W_all).astype(int)
    W_int = np.clip(W_int, 0, max_wins)
    win_prob_mat = np.zeros((n, max_wins + 1), dtype=float)
    for k in range(0, max_wins + 1):
        mask = (W_int == k).astype(np.int32)
        counts_k = mask.sum(axis=0).astype(float)
        win_prob_mat[:, k] = 100.0 * counts_k / float(N_SIMS)

    seed_mat = np.zeros((n, n), dtype=float)
    for pos in range(n):
        teams_at_pos = seeds_idx[:, pos]
        seed_mat[:, pos] = np.bincount(teams_at_pos, minlength=n) / N_SIMS * 100.0
    seed_df = pd.DataFrame(seed_mat, index=idx2mgr, columns=list(range(1, n + 1)))

    s_index = pd.Index(idx2mgr, name="manager")
    series_pack = {
        "Avg_Seed": pd.Series(avg_seed, index=s_index),
        "Exp_Final_Wins": pd.Series(exp_final_wins, index=s_index),
        "Exp_Final_PF": pd.Series(exp_final_pf, index=s_index),
        "P_Semis_SIM": pd.Series(p_semis_sim, index=s_index),
        "P_Final": pd.Series(p_final_sim, index=s_index),
        "P_Champ": pd.Series(p_champ_sim, index=s_index),
    }

    win_cols = [f"x{k}_win" for k in range(max_wins + 1)]
    win_df = pd.DataFrame(win_prob_mat, index=idx2mgr, columns=win_cols)

    # Build bye and first round winner matrices dynamically
    bye_mat = np.zeros((N_SIMS, n), dtype=np.int8)
    for bye_team in bye_teams:
        bye_mat[np.arange(N_SIMS), bye_team] = 1
    no_bye_mat = 1 - bye_mat

    qfwin_mat = np.zeros((N_SIMS, n), dtype=np.int8)
    for winner in first_round_winners:
        qfwin_mat[np.arange(N_SIMS), winner] = 1

    sfwin_mat = np.zeros((N_SIMS, n), dtype=np.int8)
    sfwin_mat[np.arange(N_SIMS), s1_w] = 1
    sfwin_mat[np.arange(N_SIMS), s2_w] = 1

    champ_mat = np.zeros((N_SIMS, n), dtype=np.int8)
    champ_mat[np.arange(N_SIMS), champ] = 1

    no_bye_counts = no_bye_mat.sum(axis=0).astype(float)
    qfwin_and_no_bye = (qfwin_mat & no_bye_mat).sum(axis=0).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        qf_win_given_no_bye = np.where(no_bye_counts > 0, 100.0 * qfwin_and_no_bye / no_bye_counts, 0.0)
    series_pack["P_QFWin_Given_NoBye_SIM"] = pd.Series(qf_win_given_no_bye, index=s_index)

    bye_counts = bye_mat.sum(axis=0).astype(float)
    sfwin_and_bye = (sfwin_mat & bye_mat).sum(axis=0).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        sf_win_given_bye = np.where(bye_counts > 0, 100.0 * sfwin_and_bye / bye_counts, 0.0)
    series_pack["P_SFWin_Given_Bye_SIM"] = pd.Series(sf_win_given_bye, index=s_index)

    qfwin_counts = qfwin_mat.sum(axis=0).astype(float)
    sfwin_and_qf = (sfwin_mat & qfwin_mat).sum(axis=0).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        sf_win_given_nonbye = np.where(qfwin_counts > 0, 100.0 * sfwin_and_qf / qfwin_counts, 0.0)
    series_pack["P_SFWin_Given_NonBye_SIM"] = pd.Series(sf_win_given_nonbye, index=s_index)

    final_counts = sfwin_mat.sum(axis=0).astype(float)
    champ_and_final = (champ_mat & sfwin_mat).sum(axis=0).astype(float)
    with np.errstate(divide="ignore", invalid="ignore"):
        win_final_given_final = np.where(final_counts > 0, 100.0 * champ_and_final / final_counts, 0.0)
    series_pack["P_WinFinal_Given_Final_SIM"] = pd.Series(win_final_given_final, index=s_index)

    return series_pack, seed_df, win_df


# -------------------------
# Core calculators
# -------------------------
def standings_for_projection(
    df_to_date: pd.DataFrame, reg: pd.DataFrame, use_median: bool = False
) -> tuple[dict, dict]:
    """Return standings for playoff projection, including playoff-only feeds.

    Some MFL exports contain playoff and bye rows but no regular-season
    matchup rows.  In that shape the feed still carries the cumulative
    standings columns, so refusing to seed is unnecessary and leaves every
    downstream title/clutch value NULL.
    """
    if reg is not None and not reg.empty:
        return wins_points_to_date(reg, use_median=use_median)

    source = df_to_date.copy()
    if source.empty:
        return {}, {}
    if "is_consolation" in source.columns:
        source = source[pd.to_numeric(source["is_consolation"], errors="coerce").fillna(0).ne(1)]
    if source.empty:
        return {}, {}

    identity = get_manager_col(source)
    source = source[source[identity].notna()].copy()
    source[identity] = source[identity].astype(str).str.strip()
    source = source[source[identity] != ""]
    if source.empty:
        return {}, {}

    wins_col = next((c for c in ("wins_to_date", "wins") if c in source.columns), None)
    points_col = next(
        (c for c in ("points_scored_to_date", "points_to_date", "team_points") if c in source.columns),
        None,
    )
    if wins_col is None:
        source["_projection_wins"] = 0.0
        wins_col = "_projection_wins"
    else:
        source["_projection_wins"] = pd.to_numeric(source[wins_col], errors="coerce").fillna(0.0)
        wins_col = "_projection_wins"
    if points_col is None:
        source["_projection_points"] = 0.0
        points_col = "_projection_points"
    else:
        source["_projection_points"] = pd.to_numeric(source[points_col], errors="coerce").fillna(0.0)
        points_col = "_projection_points"

    # Cumulative fields repeat on every row; max is stable for both repeated
    # snapshots and partially populated playoff exports.
    grouped = source.groupby(identity, sort=False)[[wins_col, points_col]].max()
    return grouped[wins_col].astype(float).to_dict(), grouped[points_col].astype(float).to_dict()


def seed_input_for_projection(df_to_date: pd.DataFrame, reg: pd.DataFrame) -> pd.DataFrame:
    """Choose rows rank-and-seed can use when regular rows are absent."""
    if reg is not None and not reg.empty:
        return reg
    source = df_to_date.copy()
    if "is_consolation" in source.columns:
        source = source[pd.to_numeric(source["is_consolation"], errors="coerce").fillna(0).ne(1)]
    if "is_bye_week" in source.columns:
        source = source[pd.to_numeric(source["is_bye_week"], errors="coerce").fillna(0).ne(1)]
    return source


def calc_regular_week_outputs(
    df_season,
    df_sched,
    season,
    week,
    history_df,
    season_stats=None,
    data_directory=None,
    settings_by_year=None,
):
    df_to_date = df_season[df_season["week"] <= week].copy()
    # Filter to regular season only (exclude both playoffs AND consolation bracket)
    consolation_filter = (df_to_date["is_consolation"] == 0) if "is_consolation" in df_to_date.columns else True
    reg_to_date = df_to_date[(df_to_date["is_playoffs"] == 0) & consolation_filter].copy()
    played_raw = reg_to_date.copy()

    # Load year-specific playoff configuration
    settings = load_league_settings(
        season,
        data_directory=str(data_directory) if data_directory else None,
        df=df_to_date,
        settings_by_year=settings_by_year,
    )

    num_playoff_teams = int(settings["num_playoff_teams"])
    bye_teams = int(settings["bye_teams"])
    uses_reseeding = bool(settings.get("uses_playoff_reseeding", 0))
    num_teams = int(settings["num_teams"])
    playoff_start_week = int(settings["playoff_start_week"])
    regular_season_weeks = playoff_start_week - 1
    seeding_rule = normalize_playoff_seeding_rule(
        settings.get("playoff_seeding_rule"),
        settings.get("playoff_seeding_rule_by"),
    )

    # Detect H2H + Median scoring for proper win calculations (year-specific)
    use_median = (
        detect_median_scoring(
            data_directory,
            year=season,
            settings_by_year=settings_by_year,
        )
        if (data_directory or settings_by_year)
        else False
    )

    future_canon = future_regular_from_schedule(df_sched, season, week)
    simulate_future_reg = not future_canon.empty

    mu_hat, sigma_hat, samples_by_team, league_mu, sigma_floor = build_team_models(
        df_to_date,
        season,
        week,
        HALF_LIFE_WEEKS,
        SHRINK_K,
        boundary_penalty=0.05,
        prior_w_cap=2.0,
        season_stats=season_stats,
    )
    ensure_params_for_future(mu_hat, sigma_hat, samples_by_team, future_canon, league_mu, sigma_floor)

    power_s = compute_power_ratings(mu_hat, samples_by_team, bootstrap_min=3)

    series_pack, seed_dist_sim, win_df = _vectorized_regular_and_bracket(
        reg_to_date,
        future_canon,
        mu_hat,
        sigma_hat,
        season,
        week,
        playoff_slots=num_playoff_teams,
        bye_slots=bye_teams,
        bracket_reseed=uses_reseeding,
        regular_season_weeks=regular_season_weeks,
        num_teams=num_teams,
        use_median=use_median,
        seeding_rule=seeding_rule,
    )

    # Handle empty results (no teams found for simulation)
    if seed_dist_sim.empty:
        logger.warning(f"[PLAYOFF_ODDS] Empty simulation results for season {season} week {week}")
        empty_odds = pd.DataFrame()
        return empty_odds, seed_dist_sim, win_df

    n_teams_all = seed_dist_sim.shape[1]
    if simulate_future_reg:
        # Check if there is historical data for years before the current season
        # If this is the first year in dataset, skip blending with historical data
        has_prior_history = not history_df.empty and len(history_df[history_df["year"] < season]) > 0

        # DISABLED historical blending - use 100% Monte Carlo simulations
        blended_seed_norm = normalize_seed_matrix_to_100(seed_dist_sim)
    else:
        blended_seed_norm = normalize_seed_matrix_to_100(seed_dist_sim)

    odds = pd.concat(
        [
            series_pack["Exp_Final_Wins"].rename("Exp_Final_Wins"),
            series_pack["Exp_Final_PF"].rename("Exp_Final_PF"),
            series_pack["Avg_Seed"].rename("Avg_Seed"),
        ],
        axis=1,
    )

    odds["P_Playoffs"] = p_playoffs_from_seeds(blended_seed_norm, num_playoff_teams)
    odds["P_Bye"] = p_bye_from_seeds(blended_seed_norm, bye_teams)

    # Use direct simulation results for P_Semis, P_Final, P_Champ
    # The conditional probability formulas don't preserve the correct sums:
    # - P_Semis should sum to ~400% (4 semifinalists)
    # - P_Final should sum to 200% (2 finalists)
    # - P_Champ should sum to 100% (1 champion)
    # Using direct simulation results ensures these constraints are satisfied.
    odds["P_Semis"] = series_pack["P_Semis_SIM"]
    odds["P_Final"] = series_pack["P_Final"]
    odds["P_Champ"] = series_pack["P_Champ"]

    # Compress, renormalize, and enforce hierarchy in a single pass
    odds = compress_and_normalize_probabilities(
        odds,
        week=week,
        num_teams=num_teams,
        num_playoff_teams=num_playoff_teams,
        bye_teams=bye_teams,
    )

    if not power_s.empty:
        odds["Power_Rating"] = power_s.reindex(odds.index)

    return odds, blended_seed_norm, win_df


def get_actual_playoff_matchups(
    df_playoff,
    week,
    is_championship_bracket=True,
    playoff_qualifiers=None,
    playoff_round_type=0,
    playoff_start_week=None,
    end_week=None,
    num_playoff_teams=None,
):
    """
    Extract actual playoff matchups from data for a given week.

    Uses is_playoffs flag to identify championship bracket games.
    Returns actual game results and teams still alive.

    CRITICAL FIX: Includes teams on bye weeks by using playoff_qualifiers.
    Teams on bye (e.g., seeds 1-2) haven't played yet, but should still be "alive"
    for championship odds calculations.

    Args:
        df_playoff: DataFrame with playoff game data
        week: Current week to analyze
        is_championship_bracket: If True, filter for is_playoffs=1, else is_consolation=1
        playoff_qualifiers: Set of all teams that qualified for playoffs (includes bye teams)
        playoff_round_type: 0=all single-week, 1=all two-week, 2=championship-only two-week
        playoff_start_week: First week of the playoffs (required when playoff_round_type > 0)
        end_week: Last valid week (required when playoff_round_type > 0)
        num_playoff_teams: Number of playoff teams (required when playoff_round_type > 0)

    Returns:
        dict with:
            - 'games': List of (manager, opponent, winner) tuples for completed games
            - 'alive': Set of teams still alive in bracket
            - 'completed_weeks': Set of weeks with completed games
    """
    flag_col = "is_playoffs" if is_championship_bracket else "is_consolation"
    bracket_df = df_playoff[df_playoff[flag_col] == 1].copy()

    if bracket_df.empty:
        return {"games": [], "alive": set(), "completed_weeks": set()}

    # Build week -> round boundary lookup for 2-week round awareness
    week_to_round = {}
    if (
        playoff_round_type > 0
        and playoff_start_week is not None
        and end_week is not None
        and num_playoff_teams is not None
    ):
        from transformations.matchup.modules.playoff_config import round_weeks as _round_weeks

        rounds = _round_weeks(num_playoff_teams, playoff_start_week, end_week, playoff_round_type)
        for round_idx, (wk_start, wk_end) in enumerate(rounds):
            for w in range(wk_start, wk_end + 1):
                week_to_round[w] = (round_idx, wk_start, wk_end)

    # CRITICAL FIX: Prioritize actual bracket participants over standings-based qualifiers
    # The actual bracket data reflects who actually made playoffs, which may differ from
    # standings due to divisional winners, tiebreakers, or other factors not in our data
    actual_bracket_teams = set(bracket_df["franchise_id"].unique())

    if actual_bracket_teams:
        # Use actual bracket participants as the source of truth
        all_teams = actual_bracket_teams
        # If playoff_qualifiers provided, add teams that haven't appeared in bracket yet
        # This is CRITICAL for bye teams who don't play until the semifinals
        if playoff_qualifiers is not None:
            # Add any qualified teams not yet in bracket (bye teams, etc.)
            missing_qualifiers = playoff_qualifiers - actual_bracket_teams
            if missing_qualifiers:
                logger.debug(f"Adding {len(missing_qualifiers)} bye/missing teams to all_teams: {missing_qualifiers}")
                all_teams = all_teams | missing_qualifiers
    elif playoff_qualifiers is not None:
        # No actual bracket data yet - use standings-based qualifiers
        all_teams = playoff_qualifiers
    else:
        all_teams = set()

    if "franchise_id" not in bracket_df.columns or "opponent_franchise_id" not in bracket_df.columns:
        logger.warning(
            "[playoff_matchups] franchise_id + opponent_franchise_id required; skipping actual matchup tracing"
        )
        return {
            "games": [],
            "alive": all_teams,
            "alive_at_week_start": all_teams,
            "completed_weeks": set(),
            "all_games": {},
        }

    # Track completed games by week
    games_by_week = {}
    for wk in sorted(bracket_df["week"].dropna().unique()):
        wk = int(wk)
        if wk > week:
            continue

        week_games = bracket_df[bracket_df["week"] == wk]
        games = []

        # Process each unique matchup using franchise_id identities only.
        processed_pairs = set()
        for _, row in week_games.iterrows():
            fid = row["franchise_id"]
            opp_fid = row["opponent_franchise_id"]
            pair = tuple(sorted([str(fid), str(opp_fid)]))

            if pair in processed_pairs:
                continue
            processed_pairs.add(pair)

            id_col = "franchise_id"
            mgr_rows = week_games[week_games[id_col] == fid]
            opp_rows = week_games[week_games[id_col] == opp_fid]

            # Skip if opponent isn't in this bracket
            if mgr_rows.empty or opp_rows.empty:
                logger.debug(f" Skipping {fid} vs {opp_fid}: mgr_rows={len(mgr_rows)}, opp_rows={len(opp_rows)}")
                continue

            mgr_row = mgr_rows.iloc[0]
            opp_row = opp_rows.iloc[0]

            round_info = week_to_round.get(wk)
            if round_info is not None:
                _, wk_start, wk_end = round_info
                if wk_start != wk_end and wk < wk_end:
                    # Week 1 of a 2-week round: round is not decided yet
                    winner = None
                elif wk_start != wk_end and wk == wk_end:
                    # Week 2 of a 2-week round: determine winner
                    # API trust rule: check win/loss flags on week 2 first
                    if (
                        pd.notna(mgr_row.get("win"))
                        and pd.notna(opp_row.get("win"))
                        and (mgr_row["win"] == 1 or opp_row["win"] == 1)
                    ):
                        winner = fid if mgr_row["win"] == 1 else opp_fid
                    else:
                        # Fall back to combined score across both weeks
                        both_weeks = bracket_df[
                            (bracket_df["week"].isin(range(wk_start, wk_end + 1)))
                            & (bracket_df["franchise_id"].isin([fid, opp_fid]))
                        ]
                        combined = both_weeks.groupby("franchise_id")["team_points"].sum()
                        if fid in combined.index and opp_fid in combined.index:
                            if pd.notna(combined[fid]) and pd.notna(combined[opp_fid]):
                                winner = fid if combined[fid] > combined[opp_fid] else opp_fid
                            else:
                                winner = None
                        else:
                            winner = None
                else:
                    # 1-week round: use existing win/loss logic
                    if pd.notna(mgr_row.get("win")) and pd.notna(opp_row.get("win")):
                        if mgr_row["win"] == 1:
                            winner = fid
                        elif opp_row["win"] == 1:
                            winner = opp_fid
                        else:
                            mgr_pts = mgr_row.get("team_points", 0)
                            opp_pts = opp_row.get("team_points", 0)
                            winner = fid if mgr_pts > opp_pts else opp_fid
                    else:
                        winner = None
            else:
                # week_to_round is empty (prt=0) or week not in map: use existing logic
                if pd.notna(mgr_row.get("win")) and pd.notna(opp_row.get("win")):
                    if mgr_row["win"] == 1:
                        winner = fid
                    elif opp_row["win"] == 1:
                        winner = opp_fid
                    else:
                        mgr_pts = mgr_row.get("team_points", 0)
                        opp_pts = opp_row.get("team_points", 0)
                        winner = fid if mgr_pts > opp_pts else opp_fid
                else:
                    winner = None

            games.append((fid, opp_fid, winner))

        games_by_week[wk] = games

    # Determine who's still alive (haven't lost yet)
    # Track TWO states:
    # 1. alive_at_week_start: Teams alive BEFORE current week games (for simulation)
    # 2. alive: Teams alive AFTER current week games (for holdover/elimination tracking)
    losers_before_week = set()  # Losers from weeks BEFORE current week
    losers_after_week = set()  # Losers including current week
    completed_weeks = set()

    for wk in sorted(games_by_week.keys()):
        if wk > week:  # Only exclude FUTURE weeks
            break
        # Check if this week's games have winners (are complete)
        week_games = games_by_week.get(wk, [])
        games_complete = all(winner is not None for _, _, winner in week_games) if week_games else False

        # Track losers from weeks BEFORE current week (for alive_at_week_start)
        if wk < week:
            completed_weeks.add(wk)
            for mgr, opp, winner in week_games:
                if winner:
                    loser = opp if winner == mgr else mgr
                    losers_before_week.add(loser)
                    losers_after_week.add(loser)
        # Track losers from current week (only for alive, not alive_at_week_start)
        elif wk == week and games_complete:
            completed_weeks.add(wk)
            for mgr, opp, winner in week_games:
                if winner:
                    loser = opp if winner == mgr else mgr
                    losers_after_week.add(loser)

    alive = all_teams - losers_after_week
    alive_at_week_start = all_teams - losers_before_week

    return {
        "games": games_by_week.get(week, []),
        "alive": alive,
        "alive_at_week_start": alive_at_week_start,  # Teams alive BEFORE this week's games
        "completed_weeks": completed_weeks,
        "all_games": games_by_week,
    }


def apply_authoritative_playoff_seeds(
    seeds: pd.DataFrame,
    df_to_date: pd.DataFrame,
    num_playoff_teams: int,
    bye_teams: int,
) -> tuple[pd.DataFrame, str | None]:
    """
    Replace standings-derived playoff qualification/seeding with authoritative
    bracket seeds when the matchup data already carries them.

    `final_playoff_seed` is populated by the bracket tracer / platform API and
    is the source of truth whenever it contains a complete 1..N playoff field.
    We keep the standings-derived W/L/PF columns, but override:
    - who made the playoffs
    - each playoff team's seed
    - bye status

    Returns:
        (seed_table, source_column_used)
    """
    if seeds is None or seeds.empty or df_to_date is None or df_to_date.empty:
        return seeds, None

    if "franchise_id" not in seeds.columns or not seeds["franchise_id"].notna().any():
        return seeds, None
    if "franchise_id" not in df_to_date.columns:
        return seeds, None
    id_col = "franchise_id"

    seed_map: dict[str, int] | None = None
    source_col = None
    for candidate in ("final_playoff_seed", "playoff_seed"):
        if candidate not in df_to_date.columns:
            continue

        candidate_df = df_to_date[[id_col, candidate]].copy()
        candidate_df[candidate] = pd.to_numeric(candidate_df[candidate], errors="coerce")
        candidate_df = candidate_df[
            candidate_df[id_col].notna() & candidate_df[candidate].between(1, num_playoff_teams, inclusive="both")
        ]
        if candidate_df.empty:
            continue

        grouped = candidate_df.sort_values([id_col, candidate]).drop_duplicates(subset=[id_col], keep="first")
        candidate_map = {str(row[id_col]): int(row[candidate]) for _, row in grouped.iterrows()}
        candidate_values = sorted(set(candidate_map.values()))
        if len(candidate_map) >= num_playoff_teams and candidate_values == list(range(1, num_playoff_teams + 1)):
            seed_map = candidate_map
            source_col = candidate
            break

    if not seed_map:
        return seeds, None

    seeded = seeds.copy()
    keyed_identity = seeded[id_col].astype(str)
    authoritative_seed = keyed_identity.map(seed_map)
    sort_seed = authoritative_seed.where(authoritative_seed.notna(), seeded["seed"])

    seeded["seed"] = pd.to_numeric(sort_seed, errors="coerce").astype("Int64")
    seeded["made_playoffs"] = authoritative_seed.notna()
    seeded["bye"] = authoritative_seed.notna() & (authoritative_seed <= bye_teams)
    seeded = seeded.sort_values(
        by=["made_playoffs", "seed", "W", "PF"],
        ascending=[False, True, False, False],
    ).reset_index(drop=True)

    return seeded, source_col


def calc_playoff_week_outputs(
    df_season,
    df_sched,
    season,
    week,
    season_stats=None,
    data_directory=None,
    settings_by_year=None,
):
    """
    Calculate playoff odds using DYNAMIC, GENERIC, SCALABLE simulation.

    NO HARD-CODING: All playoff structure comes from actual data and league settings.
    GUARANTEED COMPLETION: All simulations run to completion (no skips).
    PROBABILITIES SUM TO 100%: Proper normalization ensures valid probabilities.

    Architecture:
    - Part 1: Setup & State Extraction
    - Part 2: Dynamic Simulation Logic
    - Part 3: Calculate & Normalize Probabilities
    """
    t0 = time.time()
    rng = get_rng(season, week)
    df_to_date = df_season[df_season["week"] <= week].copy()

    # ========================================================================
    # PART 1: SETUP & STATE EXTRACTION
    # ========================================================================

    # [1.1] Load year-specific playoff configuration from league settings
    # CRITICAL FIX: Load settings per year instead of using globals
    settings = load_league_settings(
        season,
        data_directory=str(data_directory) if data_directory else None,
        df=df_to_date,
        settings_by_year=settings_by_year,
    )

    num_playoff_teams = int(settings["num_playoff_teams"])
    bye_teams = int(settings["bye_teams"])
    uses_reseeding = bool(settings.get("uses_playoff_reseeding", 0))
    playoff_start_week = int(settings["playoff_start_week"])
    seeding_rule = normalize_playoff_seeding_rule(
        settings.get("playoff_seeding_rule"),
        settings.get("playoff_seeding_rule_by"),
    )

    # Read multi-week playoff round settings
    playoff_round_type = int(settings.get("playoff_round_type", 0))

    # Derive end_week for round_weeks()
    import math as _math

    _consol_filter = (df_season["is_consolation"] == 0) if "is_consolation" in df_season.columns else True
    _po_wks = (
        sorted(df_season[(df_season["is_playoffs"] == 1) & _consol_filter]["week"].dropna().unique().astype(int))
        if not df_season.empty
        else []
    )
    if _po_wks:
        end_week = max(_po_wks)
    else:
        _num_rnds = _math.ceil(_math.log2(max(num_playoff_teams, 2)))
        _total_wks = sum(
            2 if (playoff_round_type == 1 or (playoff_round_type == 2 and r == _num_rnds)) else 1
            for r in range(1, _num_rnds + 1)
        )
        end_week = playoff_start_week + _total_wks - 1

    logger.info(
        f"[Dynamic Settings] Year={season}, Playoff Teams={num_playoff_teams}, Byes={bye_teams}, "
        f"Reseeding={uses_reseeding}, PlayoffRoundType={playoff_round_type}"
    )

    # Detect H2H + Median scoring for proper win calculations (year-specific)
    use_median = (
        detect_median_scoring(
            data_directory,
            year=season,
            settings_by_year=settings_by_year,
        )
        if (data_directory or settings_by_year)
        else False
    )

    # [1.2] Calculate seeds from regular season performance
    # CRITICAL: Use only regular season games (is_playoffs=0 AND is_consolation=0) for seeding
    # Also exclude rows with no opponent (bye/eliminated team placeholders)
    consolation_filter = (df_to_date["is_consolation"] == 0) if "is_consolation" in df_to_date.columns else True
    if "opponent_franchise_id" in df_to_date.columns:
        opponent_tokens = df_to_date["opponent_franchise_id"].astype("string").str.strip().str.lower()
        has_opponent = opponent_tokens.notna() & ~opponent_tokens.isin({"", "none"})
    elif "is_bye_week" in df_to_date.columns:
        has_opponent = pd.to_numeric(df_to_date["is_bye_week"], errors="coerce").fillna(0).ne(1)
    else:
        has_opponent = pd.Series(True, index=df_to_date.index, dtype=bool)
    reg = df_to_date[(df_to_date["is_playoffs"] == 0) & consolation_filter & has_opponent].copy()
    wins_to_date, pts_to_date = standings_for_projection(df_to_date, reg, use_median=use_median)

    seed_input = seed_input_for_projection(df_to_date, reg)
    seeds = rank_and_seed(
        pd.Series(wins_to_date, dtype=float),
        pd.Series(pts_to_date, dtype=float),
        num_playoff_teams,
        bye_teams,
        played_raw=seed_input,
        seeding_rule=seeding_rule,
    )
    seeds, authoritative_seed_source = apply_authoritative_playoff_seeds(
        seeds,
        df_to_date,
        num_playoff_teams=num_playoff_teams,
        bye_teams=bye_teams,
    )
    seeds["seed"] = pd.to_numeric(seeds["seed"], errors="coerce").astype("Int64")
    if "franchise_id" not in seeds.columns:
        raise ValueError("franchise_id is required for playoff week outputs")
    seed_id_col = "franchise_id"
    playoff_teams = seeds.loc[seeds["made_playoffs"], seed_id_col].dropna().tolist()

    if authoritative_seed_source:
        logger.info(f" Playoff teams from {authoritative_seed_source}: {playoff_teams}")
    else:
        logger.info(f" Playoff teams from standings ({seeding_rule}): {playoff_teams}")

    # [1.3] Get current bracket state from ACTUAL playoff data
    # CRITICAL FIX: Trust the API bracket data over standings-based seeding.
    # Platforms (Sleeper, ESPN, Yahoo) may use different tiebreakers, divisional
    # seeding, or commissioner overrides that produce a bracket different from
    # what rank_and_seed() computes. Build playoff_qualifiers from actual bracket
    # participants (is_playoffs=1) plus bye team placeholders.
    actual_bracket_managers = {
        team
        for team in df_to_date[(df_to_date["is_playoffs"] == 1) & (df_to_date["is_consolation"] == 0)][
            "franchise_id"
        ].unique()
        if pd.notna(team)
    }
    # Add bye-team rows even when the source marks them as False instead of NULL.
    # The flat DDL should not require a platform-specific sentinel here:
    # `is_bye_week=1` is the authoritative signal that this manager is in the
    # championship bracket but does not have a game this week.
    if "is_bye_week" in df_to_date.columns:
        bye_placeholder_managers = set(
            df_to_date[
                (df_to_date["is_bye_week"] == 1)
                & (df_to_date["is_consolation"].isna() | (df_to_date["is_consolation"] == 0))
                & (df_to_date["week"] >= playoff_start_week)
                & (df_to_date["franchise_id"].isin(playoff_teams))
            ]["franchise_id"]
            .dropna()
            .unique()
        )
        actual_bracket_managers |= bye_placeholder_managers

    if actual_bracket_managers:
        # Use actual bracket participants as source of truth, but always union
        # in authoritative playoff qualifiers from seeded standings so first-round
        # bye teams are still included before they log an actual playoff matchup.
        # Without this, quarterfinal winners can incorrectly look like they have
        # already clinched the championship game because seeds 1-N were omitted.
        seeded_playoff_teams = set(playoff_teams)
        playoff_qualifiers_set = actual_bracket_managers | seeded_playoff_teams
        if playoff_qualifiers_set != seeded_playoff_teams or actual_bracket_managers != seeded_playoff_teams:
            logger.info(
                f" [API Override] Bracket participants differ from standings: "
                f"API={sorted(actual_bracket_managers)}, "
                f"Standings={sorted(seeded_playoff_teams)}, "
                f"Combined={sorted(playoff_qualifiers_set)}"
            )
    else:
        # Fallback to standings if no bracket data yet (pre-playoff simulation)
        playoff_qualifiers_set = set(playoff_teams)

    bracket_info = get_actual_playoff_matchups(
        df_to_date,
        week,
        is_championship_bracket=True,
        playoff_qualifiers=playoff_qualifiers_set,
        playoff_round_type=playoff_round_type,
        playoff_start_week=playoff_start_week,
        end_week=end_week,
        num_playoff_teams=num_playoff_teams,
    )
    teams_alive = bracket_info["alive"]  # Teams alive AFTER current week games
    teams_alive_for_sim = bracket_info["alive_at_week_start"]  # Teams alive BEFORE current week games (for simulation)
    all_playoff_games = bracket_info["all_games"]

    logger.debug(
        f"[Bracket State] Week {week}: {len(teams_alive)} teams alive (end of week), {len(teams_alive_for_sim)} for sim (start of week)"
    )
    logger.debug(f"[Timing] Bracket state extracted in {time.time() - t0:.2f}s")

    # [1.4] Build team power models
    # Strategy: Frozen power ratings for eliminated teams, refreshed for teams still alive
    last_reg_week = schedules_last_regular_week(df_sched, season)
    if last_reg_week is None:
        if not reg.empty:
            weeks = pd.to_numeric(reg["week"], errors="coerce").dropna()
            last_reg_week = int(weeks.max()) if len(weeks) > 0 else week
        else:
            last_reg_week = week

    # BASE MODEL: Regular season only (frozen for eliminated teams)
    history_for_base = df_to_date[df_to_date["week"] <= last_reg_week].copy()

    mu_hat_base, sigma_hat_base, samples_base, league_mu, sigma_floor = build_team_models(
        history_for_base,
        season,
        last_reg_week,
        HALF_LIFE_WEEKS,
        SHRINK_K,
        boundary_penalty=0.05,
        prior_w_cap=2.0,
        season_stats=season_stats,
    )

    power_s_base = compute_power_ratings(mu_hat_base, samples_base, bootstrap_min=3)

    # REFRESHED MODEL: Include playoff games for teams still in championship contention
    # Filter: actual games only (exclude placeholder/bye rows where team_points is NaN)
    # Use teams_alive_for_sim to include teams that lost in current week (for simulation)
    if teams_alive_for_sim:
        # Include regular season + playoff games, but exclude placeholder rows
        history_with_playoffs = df_to_date[
            (df_to_date["team_points"].notna())  # Exclude placeholder/bye rows
            & (df_to_date["opponent"].notna())  # Must have an opponent (actual game)
        ].copy()

        # Build fresh model with playoff data
        mu_hat_fresh, sigma_hat_fresh, samples_fresh, _, _ = build_team_models(
            history_with_playoffs,
            season,
            week,
            HALF_LIFE_WEEKS,
            SHRINK_K,
            boundary_penalty=0.05,
            prior_w_cap=2.0,
            season_stats=season_stats,
        )

        power_s_fresh = compute_power_ratings(mu_hat_fresh, samples_fresh, bootstrap_min=3)

        # Merge: alive teams get refreshed power ratings, others keep frozen
        power_s = power_s_base.copy()
        for team in teams_alive_for_sim:
            if team in power_s_fresh.index:
                power_s[team] = power_s_fresh[team]
                logger.debug(f"  [Power Refresh] {team}: {power_s_base.get(team, 0):.1f} -> {power_s_fresh[team]:.1f}")

        # Also update mu_hat and sigma_hat for alive teams (used in simulations)
        mu_hat = mu_hat_base.copy()
        sigma_hat = sigma_hat_base.copy()
        samples_by_team = samples_base.copy()
        for team in teams_alive_for_sim:
            if team in mu_hat_fresh:
                mu_hat[team] = mu_hat_fresh[team]
            if team in sigma_hat_fresh:
                sigma_hat[team] = sigma_hat_fresh[team]
            if team in samples_fresh:
                samples_by_team[team] = samples_fresh[team]
    else:
        # No teams alive (shouldn't happen, but handle gracefully)
        mu_hat = mu_hat_base
        sigma_hat = sigma_hat_base
        samples_by_team = samples_base
        power_s = power_s_base

    logger.debug(f"[Timing] Power models built in {time.time() - t0:.2f}s")
    logger.debug(
        f"[Power Rating] Refreshed for {len(teams_alive_for_sim)} alive teams (for sim): {sorted(teams_alive_for_sim)}"
    )
    # ========================================================================
    # PART 2: DYNAMIC SIMULATION LOGIC
    # ========================================================================

    def get_actual_winner(teamA, teamB, week_limit):
        """Get actual winner from playoff games, if the matchup has been played.

        For 2-week rounds (playoff_round_type=1 or 2), sums points across both
        weeks of the round to determine the winner.
        """
        if teamA is None or teamB is None:
            return None

        playoff_df = df_to_date[df_to_date["is_playoffs"] == 1].copy()

        # Find games between these two teams
        matchup_games = playoff_df[
            (playoff_df["franchise_id"].isin([teamA, teamB]))
            & (playoff_df["opponent_franchise_id"].isin([teamA, teamB]))
            & (playoff_df["week"] <= week_limit)
        ]

        if matchup_games.empty:
            return None

        weeks = pd.to_numeric(matchup_games["week"], errors="coerce").dropna()
        if weeks.empty:
            return None

        recent_week = int(weeks.max())

        if playoff_round_type == 1:
            # All rounds are 2 weeks: find the 2-week window for this round
            round_offset = (recent_week - playoff_start_week) // 2
            round_start = playoff_start_week + round_offset * 2
            round_end = round_start + 1
            recent_games = matchup_games[(matchup_games["week"] >= round_start) & (matchup_games["week"] <= round_end)]
            # Sum points across both weeks
            pts = recent_games.groupby("franchise_id")["team_points"].sum()
            # Only declare winner if both weeks are complete
            if recent_games["week"].nunique() < 2 and recent_week < round_end:
                return None
        elif playoff_round_type == 2:
            # Only championship is 2 weeks
            num_rounds = max(1, int(np.ceil(np.log2(num_playoff_teams))))
            champ_start_week = playoff_start_week + (num_rounds - 1)
            if recent_week >= champ_start_week:
                recent_games = matchup_games[
                    (matchup_games["week"] >= champ_start_week) & (matchup_games["week"] <= champ_start_week + 1)
                ]
                pts = recent_games.groupby("franchise_id")["team_points"].sum()
                if recent_games["week"].nunique() < 2 and recent_week < champ_start_week + 1:
                    return None
            else:
                recent_games = matchup_games[matchup_games["week"] == recent_week]
                pts = recent_games.groupby("franchise_id")["team_points"].mean()
        else:
            recent_games = matchup_games[matchup_games["week"] == recent_week]
            pts = recent_games.groupby("franchise_id")["team_points"].mean()

        if len(pts) != 2 or teamA not in pts.index or teamB not in pts.index:
            return None

        if pd.isna(pts[teamA]) or pd.isna(pts[teamB]):
            return None

        if pts[teamA] > pts[teamB]:
            return teamA
        elif pts[teamB] > pts[teamA]:
            return teamB
        else:
            # Tie - use alphabetical
            return min(teamA, teamB)

    def simulate_round(
        alive_teams,
        current_week,
        seeds_map,
        use_reseeding,
        bye_team_set=None,
        round_num=0,
        playoff_round_type=0,
        round_week_ranges=None,
        round_actuals=None,
        current_round_idx=0,
    ):
        """
        Simulate one round of playoffs.

        Args:
            alive_teams: Set of teams still in contention
            current_week: Current week for looking up actual results
            seeds_map: Dict mapping manager -> seed number
            use_reseeding: Whether to use reseeding bracket
            bye_team_set: Set of teams with first-round byes (they don't play in round 1)
            round_num: Which round of the simulation (0 = first round)
            playoff_round_type: 0=all 1-week, 1=all 2-week, 2=championship-only 2-week
            round_week_ranges: List of (wk_start, wk_end) tuples per round
            round_actuals: Dict mapping (wk_start, wk_end) -> {team: week1_score} for mid-round actuals
            current_round_idx: Index into round_week_ranges for the current round

        Returns: (winners_list, matchups_played, bye_teams_advancing)
        """
        if len(alive_teams) <= 1:
            return list(alive_teams), [], []

        # Pair teams by seed
        alive_sorted = sorted(alive_teams, key=lambda m: seeds_map.get(m, 999))

        # Identify which teams are on bye THIS round
        # Bye teams are highest seeds that don't play when there are more teams than a power of 2
        # For 6-team playoff: seeds 1-2 have byes in round 1
        # For 5-team playoff: seed 1 has bye in round 1
        teams_playing = []
        teams_on_bye = []

        if bye_team_set:
            for team in alive_sorted:
                if team in bye_team_set:
                    teams_on_bye.append(team)
                else:
                    teams_playing.append(team)
        else:
            teams_playing = alive_sorted

        # If no teams playing (all on bye), just return byes as "winners"
        if not teams_playing:
            return teams_on_bye, [], teams_on_bye

        # Use shared module for bracket matchup creation
        # This handles both reseeding and fixed bracket logic correctly
        matchups = create_round_matchups(teams_playing, seeds_map, use_reseeding, round_num, num_playoff_teams)

        # Simulate each matchup
        winners = []
        for teamA, teamB in matchups:
            # Check if actual result exists
            actual_winner = get_actual_winner(teamA, teamB, current_week)

            if actual_winner:
                winner = actual_winner
            else:
                # Check if current round is 2-week
                is_two_week = False
                current_range = None
                if round_week_ranges and current_round_idx < len(round_week_ranges):
                    wk_start, wk_end = round_week_ranges[current_round_idx]
                    is_two_week = wk_start != wk_end
                    current_range = (wk_start, wk_end)

                if is_two_week:
                    actuals = round_actuals.get(current_range, {}) if round_actuals else {}
                    has_both = teamA in actuals and teamB in actuals

                    if has_both:
                        # Mid-round: actual week 1 + sim week 2
                        score_a = actuals[teamA] + _sim_game_score(teamA, rng, mu_hat, sigma_hat)
                        score_b = actuals[teamB] + _sim_game_score(teamB, rng, mu_hat, sigma_hat)
                    else:
                        # Pre-game: sim both weeks
                        score_a = _sim_game_score(teamA, rng, mu_hat, sigma_hat) + _sim_game_score(
                            teamA, rng, mu_hat, sigma_hat
                        )
                        score_b = _sim_game_score(teamB, rng, mu_hat, sigma_hat) + _sim_game_score(
                            teamB, rng, mu_hat, sigma_hat
                        )

                    # Deterministic tie-break: higher seed wins
                    if score_a > score_b:
                        winner = teamA
                    elif score_b > score_a:
                        winner = teamB
                    else:
                        winner = teamA if seeds_map.get(teamA, 999) < seeds_map.get(teamB, 999) else teamB
                else:
                    # 1-week round: existing behavior
                    winner = _sim_game(teamA, teamB, rng, mu_hat, sigma_hat, samples_by_team)

            winners.append(winner)

        return winners, matchups, teams_on_bye

    # Track seed distribution for simulation results
    sims_for_seed_dist = []

    # Vectorized probabilities will be stored here for playoff weeks
    vectorized_p_semis = {}
    vectorized_p_final = {}
    vectorized_p_champ = {}

    # Determine if we're in playoffs yet
    in_playoffs = week >= playoff_start_week

    if not in_playoffs:
        # Regular season - no playoff simulation needed, just track seeds
        t_sim = time.time()
        for _ in range(N_SIMS):
            sims_for_seed_dist.append(seeds)
        logger.debug(f"[Timing] Simulations completed in {time.time() - t_sim:.2f}s")
    else:
        # Playoff simulation
        # Build seeds_map keyed by franchise_id for stable identity
        seeds_map = dict(zip(seeds["franchise_id"], seeds["seed"]))
        _fid_to_mgr = dict(zip(seeds["franchise_id"], seeds["manager"]))
        _mgr_to_fid = dict(zip(seeds["manager"], seeds["franchise_id"]))
        # Build fid mapping from df_to_date for teams not in seeds (bracket overrides)
        if "franchise_id" in df_to_date.columns:
            for _, _row in df_to_date[["manager", "franchise_id"]].drop_duplicates().iterrows():
                if pd.notna(_row["franchise_id"]) and pd.notna(_row["manager"]):
                    _mgr_to_fid.setdefault(_row["manager"], _row["franchise_id"])
                    _fid_to_mgr.setdefault(_row["franchise_id"], _row["manager"])
        # teams_alive_for_sim/teams_alive/playoff_teams are already franchise_ids
        _teams_alive_fid = teams_alive_for_sim
        _teams_alive_end_fid = teams_alive
        _playoff_teams_fid = playoff_teams

        # Identify bye teams (top N seeds where N = bye_teams setting)
        # These teams don't play in round 1 (wild card) but enter in round 2 (semis)
        # Use teams_alive_for_sim to include bye teams that may have lost in current week
        bye_team_set = set()
        if bye_teams > 0:
            for key, seed in seeds_map.items():
                if seed <= bye_teams and key in _teams_alive_fid:
                    bye_team_set.add(key)
            sorted_bye_teams = sorted(bye_team_set, key=lambda m: seeds_map.get(m, 999))
            # For logging, show human-readable manager names
            _display = [_fid_to_mgr.get(k, k) for k in sorted_bye_teams]
            if len(bye_team_set) == bye_teams:
                logger.info(f" Bye teams (seeds 1-{bye_teams}): {_display}")
            else:
                logger.info(f" Surviving bye teams: {_display} ({len(bye_team_set)} of {bye_teams} original)")

        # Determine which playoff round we're in
        # Round 0 = wild card (first playoff week), Round 1 = semis, etc.
        # For 2-week rounds (playoff_round_type=1: all rounds, =2: championship only),
        # consecutive week pairs map to the same round.
        weeks_elapsed = week - playoff_start_week
        if playoff_round_type == 1:
            # All rounds are 2 weeks: week 0-1 = round 0, week 2-3 = round 1, etc.
            current_playoff_round = weeks_elapsed // 2
        elif playoff_round_type == 2:
            # Only championship is 2 weeks: detect based on bracket structure
            # Standard rounds first, then the last 2 weeks are the championship
            num_rounds = max(1, int(np.ceil(np.log2(num_playoff_teams))))
            # Championship starts at round (num_rounds - 1), which spans 2 weeks
            championship_start_week_offset = num_rounds - 1  # 0-indexed offset from playoff_start
            if weeks_elapsed >= championship_start_week_offset:
                current_playoff_round = championship_start_week_offset
            else:
                current_playoff_round = weeks_elapsed
        else:
            # Standard: 1 week per round
            current_playoff_round = weeks_elapsed

        # Check if current week's games are complete (teams have been eliminated)
        # If so, we need to simulate starting from the NEXT round
        expected_teams_at_round_start = num_playoff_teams
        if current_playoff_round == 0 and bye_teams > 0:
            # In wild card, only non-bye teams play
            expected_teams_playing = num_playoff_teams - bye_teams
        else:
            expected_teams_playing = expected_teams_at_round_start

        # If teams have been eliminated this round, the round is complete
        # Advance the simulation starting round accordingly
        games_this_round_complete = len(teams_alive) < num_playoff_teams and week in bracket_info.get(
            "completed_weeks", set()
        )
        starting_sim_round = 1 if games_this_round_complete and current_playoff_round == 0 else 0

        if games_this_round_complete:
            logger.info(f" Week {week} games complete, starting simulation from round {starting_sim_round}")

        # Determine locked champion if championship has been played
        if len(teams_alive) == 1:
            champ_locked = list(teams_alive)[0]
        else:
            champ_locked = None

        t_sim = time.time()

        # Build actual_results dict from completed playoff games
        # Include current week's games if they are complete (for POST-GAME odds)
        # Odds represent: "what are the chances going forward, given games played so far"
        actual_results = {}
        if bracket_info and "all_games" in bracket_info:
            for wk, games in bracket_info["all_games"].items():
                # Include games from previous weeks always
                # Include current week's games ONLY if the round is complete
                include_game = (wk < week) or (wk == week and games_this_round_complete)
                if include_game:
                    for mgr, opp, winner in games:
                        if winner:
                            # Convert to franchise_id keys
                            _m = _mgr_to_fid.get(mgr, mgr)
                            _o = _mgr_to_fid.get(opp, opp)
                            _w = _mgr_to_fid.get(winner, winner)
                            actual_results[(_m, _o)] = _w
                            actual_results[(_o, _m)] = _w

        # Use vectorized bracket simulation for massive speedup
        # When games are complete, use POST-GAME teams (teams_alive) for accurate odds
        # When games are pending, use PRE-GAME teams (teams_alive_for_sim)
        # sim_teams uses franchise_id keys (consistent with seeds_map)
        if games_this_round_complete:
            # POST-GAME: use teams that survived this round
            sim_teams = list(_teams_alive_end_fid) if _teams_alive_end_fid else _playoff_teams_fid
        else:
            # PRE-GAME: use teams alive at start of week (all could still win)
            sim_teams = list(_teams_alive_fid) if _teams_alive_fid else _playoff_teams_fid

        # Calculate effective bye count for simulation
        # Byes only matter in round 0 (wild card) when games haven't completed
        # After games complete, remaining teams don't have byes anymore
        if games_this_round_complete:
            effective_byes = 0  # No byes after round completes
        else:
            effective_byes = bye_teams if current_playoff_round == 0 else 0

        # Generate unique RNG seed for this year/week combination
        rng_seed = RNG_SEED + season * 100 + week

        # Log simulation setup
        odds_type = "POST-GAME" if games_this_round_complete else "PRE-GAME"
        logger.debug(f"[Simulation] {odds_type} odds with {len(sim_teams)} teams, {len(actual_results)} known results")

        # Use actual bracket size if larger than settings (handles data/settings mismatch)
        actual_bracket_size = max(num_playoff_teams, len(sim_teams))

        # Convert mu_hat/sigma_hat to franchise_id keys for simulation consistency
        _mu_fid = {_mgr_to_fid.get(m, m): v for m, v in mu_hat.items()}
        _sigma_fid = {_mgr_to_fid.get(m, m): v for m, v in sigma_hat.items()}

        _round_ranges = round_weeks(num_playoff_teams, playoff_start_week, end_week, playoff_round_type)

        # Build round_actuals with matchup-pair symmetry check
        _round_actuals = {}
        if playoff_round_type > 0:
            for _ri, (_ws, _we) in enumerate(_round_ranges):
                if _ws == _we:
                    continue
                _consol_filt = (df_to_date["is_consolation"] == 0) if "is_consolation" in df_to_date.columns else True
                _wk1 = df_to_date[(df_to_date["week"] == _ws) & (df_to_date["is_playoffs"] == 1) & _consol_filt]
                if _wk1.empty or not _wk1["team_points"].notna().any():
                    continue
                _matchup_scores = {}
                for _, _r in _wk1.iterrows():
                    _fid = _r.get("franchise_id")
                    _opp = _r.get("opponent_franchise_id")
                    _pts = _r.get("team_points")
                    if pd.notna(_fid) and pd.notna(_opp) and pd.notna(_pts):
                        _pk = frozenset([_fid, _opp])
                        _matchup_scores.setdefault(_pk, {})[_fid] = float(_pts)
                _valid = {}
                for _pk, _pp in _matchup_scores.items():
                    if len(_pp) == 2:
                        _valid.update(_pp)
                if _valid:
                    _round_actuals[(_ws, _we)] = _valid

        vectorized_results = simulate_playoff_bracket_vectorized(
            teams_alive=sim_teams,
            seeds_map=seeds_map,
            mu_hat=_mu_fid,
            sigma_hat=_sigma_fid,
            n_sims=N_SIMS,
            bye_teams=effective_byes,
            uses_reseeding=uses_reseeding,
            rng_seed=rng_seed,
            actual_results=actual_results,
            original_playoff_teams=actual_bracket_size,  # Use actual bracket size for side assignment
            playoff_round_type=playoff_round_type,
            round_week_ranges=_round_ranges,
            round_actuals=_round_actuals,
        )

        # Store vectorized probabilities for use in Part 3.
        # Keep the simulation identity untouched so the odds table and the
        # writeback path can stay keyed by franchise_id during playoffs.
        vectorized_p_semis = vectorized_results.get("p_semis", {})
        vectorized_p_final = vectorized_results.get("p_final", {})
        vectorized_p_champ = vectorized_results.get("p_champ", {})

        # Seed distribution still needs N_SIMS copies (this is fast)
        for _ in range(N_SIMS):
            sims_for_seed_dist.append(seeds)

        logger.debug(f"[Timing] Vectorized simulations completed in {time.time() - t_sim:.2f}s")

    # ========================================================================
    # PART 3: CALCULATE & NORMALIZE PROBABILITIES
    # ========================================================================

    if not sims_for_seed_dist:
        return pd.DataFrame(), pd.DataFrame(), seeds

    # Build output DataFrame
    tall = pd.concat(sims_for_seed_dist, ignore_index=True)

    odds = tall.groupby("franchise_id").agg(Exp_Final_Wins=("W", "mean"), Exp_Final_PF=("PF", "mean"))
    odds["Avg_Seed"] = tall.groupby("franchise_id")["seed"].mean()

    idx_mgrs = odds.index.tolist()

    # Calculate probabilities
    if in_playoffs:
        # CRITICAL FIX: Ensure all simulated teams are in odds DataFrame
        # Some actual playoff participants may not be in standings-based 'seeds'
        # (e.g., division winners with worse records, or data inconsistencies)
        sim_teams_set = set(teams_alive_for_sim)
        missing_teams = sim_teams_set - set(idx_mgrs)
        if missing_teams:
            logger.debug(
                f"[Playoff Fix] Adding {len(missing_teams)} actual playoff teams not in standings: {sorted(missing_teams)}"
            )
            # Add missing teams to odds with default values
            for mgr in missing_teams:
                odds.loc[mgr] = [0.0] * len(odds.columns)
            idx_mgrs = odds.index.tolist()  # Refresh after adding

        # Playoff probabilities - use vectorized results directly (already in %)
        odds["P_Semis"] = [vectorized_p_semis.get(m, 0.0) for m in idx_mgrs]
        odds["P_Final"] = [vectorized_p_final.get(m, 0.0) for m in idx_mgrs]
        odds["P_Champ"] = [vectorized_p_champ.get(m, 0.0) for m in idx_mgrs]

        # Apply overrides for completed rounds using helper function
        odds = apply_completed_round_overrides(
            odds=odds,
            teams_alive=teams_alive,
            playoff_teams=playoff_teams,
            bracket_info=bracket_info,
            current_playoff_round=current_playoff_round,
            games_this_round_complete=games_this_round_complete,
        )
    else:
        # Regular season - no playoff probabilities yet
        odds["P_Semis"] = 0.0
        odds["P_Final"] = 0.0
        odds["P_Champ"] = 0.0

    # Playoff/Bye probabilities (always deterministic at this week)
    # Use API bracket participants for P_Playoffs (trust the API over standings)
    odds["P_Playoffs"] = [100.0 if m in playoff_qualifiers_set else 0.0 for m in idx_mgrs]
    odds["P_Bye"] = [
        100.0
        if (
            m in playoff_qualifiers_set and bool(seeds.loc[seeds["franchise_id"] == m, "bye"].iloc[0])
            if m in seeds["franchise_id"].values
            else False
        )
        else 0.0
        for m in idx_mgrs
    ]

    # Add power ratings
    if "Power_Rating" not in odds.columns:
        odds["Power_Rating"] = power_s.reindex(odds.index)

    # Seed distribution
    team_count = seeds.shape[0]
    seed_dist = (
        tall.pivot_table(index="franchise_id", columns="seed", values="W", aggfunc="size", fill_value=0).div(
            len(sims_for_seed_dist)
        )
        * 100.0
    )
    all_cols = list(range(1, team_count + 1))
    seed_dist = seed_dist.reindex(columns=all_cols, fill_value=0.0)
    seed_dist_norm = normalize_seed_matrix_to_100(seed_dist)

    odds = enforce_hierarchy(odds, enforce_bye_semis=bye_advances_to_semis(num_playoff_teams, bye_teams))

    # SAFETY NET: Normalize P_Champ to sum to 100% during playoffs
    # Even with correct bracket participants, edge cases (rounding, hierarchy
    # clipping) can cause slight drift. Same pattern as compress_and_normalize.
    if in_playoffs and "P_Champ" in odds.columns:
        champ_sum = odds["P_Champ"].sum()
        if champ_sum > 0 and abs(champ_sum - 100.0) > 0.5:
            logger.info(f" [P_Champ Normalization] Week {week}: sum was {champ_sum:.2f}%, normalizing to 100%")
            odds["P_Champ"] = odds["P_Champ"] * 100.0 / champ_sum
            # Re-enforce hierarchy after normalization
            odds = enforce_hierarchy(odds, enforce_bye_semis=bye_advances_to_semis(num_playoff_teams, bye_teams))

    # Diagnostic output
    logger.debug(f"[PO diag] season={season} week={week}")
    if in_playoffs and len(teams_alive) == 2:
        champ_probs = odds.loc[odds.index.isin(teams_alive), "P_Champ"]
        logger.info(f"[Championship Odds] {dict(champ_probs)} - Sum: {champ_probs.sum():.2f}%")

    return odds, seed_dist_norm, seeds


# -------------------------
# League Settings Loading
# -------------------------
def infer_projection_settings(df: pd.DataFrame, year: int) -> dict:
    """Build a conservative settings fallback for odds-only projection.

    This is intentionally used only when canonical per-year settings cannot be
    resolved.  It is not a replacement for source settings: actual playoff
    seed fields win when present, otherwise the bracket size follows the
    standard 4/6/8-team convention from the observed league size.  The
    ``settings_source`` marker makes the fallback auditable downstream.
    """
    year_df = df[df["year"] == year].copy() if "year" in df.columns else df.copy()
    if year_df.empty:
        raise ValueError(f"Cannot infer playoff settings for empty year {year}")

    identity = "franchise_id" if "franchise_id" in year_df.columns else "manager"
    teams = year_df[identity].dropna().astype(str).str.strip()
    teams = teams[teams != ""]
    num_teams = int(teams.nunique())
    if num_teams <= 0:
        raise ValueError(f"Cannot infer number of teams for year {year}")

    is_playoffs = pd.to_numeric(year_df.get("is_playoffs", 0), errors="coerce").fillna(0).eq(1)
    is_consolation = pd.to_numeric(year_df.get("is_consolation", 0), errors="coerce").fillna(0).eq(1)
    championship = year_df[is_playoffs & ~is_consolation].copy()
    playoff_start = pd.to_numeric(championship.get("week"), errors="coerce").dropna()
    if playoff_start.empty:
        regular = year_df[~is_playoffs & ~is_consolation]
        regular_weeks = pd.to_numeric(regular.get("week"), errors="coerce").dropna()
        playoff_start_week = int(regular_weeks.max()) + 1 if not regular_weeks.empty else 14
    else:
        playoff_start_week = int(playoff_start.min())

    # A complete 1..N final/playoff seed field is the strongest data-derived
    # bracket evidence available when the canonical setting is missing.
    playoff_slots = None
    for seed_col in ("final_playoff_seed", "playoff_seed"):
        if seed_col not in championship.columns:
            continue
        seeds = pd.to_numeric(championship[seed_col], errors="coerce").dropna()
        seeds = seeds[seeds >= 1].astype(int)
        if not seeds.empty:
            candidate = sorted(seeds.unique().tolist())
            if candidate == list(range(1, max(candidate) + 1)):
                playoff_slots = max(candidate)
                break

    if playoff_slots is None and not championship.empty:
        playoff_ids = championship[identity].dropna().astype(str).str.strip()
        playoff_ids = playoff_ids[playoff_ids != ""]
        if playoff_ids.nunique() >= 2:
            playoff_slots = int(playoff_ids.nunique())

    if playoff_slots is None:
        # Standard fallback for seasons with no actual playoff rows (including
        # best-ball-like data): retain a projected championship bracket so
        # title odds remain computable rather than being forced to zero.
        playoff_slots = 4 if num_teams <= 8 else 6 if num_teams <= 12 else 8
    playoff_slots = max(2, min(int(playoff_slots), num_teams))

    if playoff_slots in (6, 12):
        bye_teams = playoff_slots // 3
    elif playoff_slots == 4 or playoff_slots == 8:
        bye_teams = 0
    else:
        next_power = 1
        while next_power < playoff_slots:
            next_power *= 2
        bye_teams = next_power - playoff_slots

    weeks = pd.to_numeric(year_df.get("week"), errors="coerce").dropna()
    end_week = int(weeks.max()) if not weeks.empty else playoff_start_week
    return {
        "playoff_start_week": playoff_start_week,
        "num_playoff_teams": playoff_slots,
        "bye_teams": bye_teams,
        "has_multiweek_championship": 0,
        "playoff_round_type": 0,
        "uses_playoff_reseeding": 0,
        "num_teams": num_teams,
        "end_week": end_week,
        "uses_median": False,
        "settings_source": "data_projection_fallback",
    }


def resolve_settings_for_projection(
    df: pd.DataFrame,
    data_directory: str | None = None,
    settings_by_year: dict[int, dict] | None = None,
) -> dict[int, dict]:
    """Resolve every observed season, falling back only for unresolved years."""
    effective = dict(settings_by_year or {})
    years = sorted(pd.to_numeric(df["year"], errors="coerce").dropna().astype(int).unique())
    for year in years:
        try:
            load_league_settings(
                int(year),
                data_directory=data_directory,
                df=df[df["year"] == year],
                settings_by_year=effective,
            )
            continue
        except ValueError as exc:
            fallback = infer_projection_settings(df, int(year))
            # Preserve any valid fields supplied by a partial canonical row,
            # but replace NULL/blank values with the data-derived projection.
            row = effective.get(year, effective.get(str(year), {}))
            if not isinstance(row, dict):
                row = {}
            merged = dict(fallback)
            for key, value in row.items():
                if value is None or (isinstance(value, str) and not value.strip()):
                    continue
                try:
                    if bool(pd.isna(value)):
                        continue
                except (TypeError, ValueError):
                    pass
                merged[key] = value
            effective[year] = merged
            logger.warning(
                "Using data-derived playoff projection for year %s after canonical settings failed: %s",
                year,
                exc,
            )
    return effective


def load_playoff_config_from_settings(
    ctx: LeagueContext,
    df: pd.DataFrame = None,
    settings_by_year: dict[int, dict] | None = None,
):
    """
    Load playoff configuration from canonical league settings.

    Prefers the flat ``league_settings`` rows already loaded from DuckDB /
    MotherDuck. Falls back to the shared playoff settings loader only when
    canonical rows are unavailable.

    Args:
        ctx: LeagueContext object
        df: Optional DataFrame for settings inference if files not found

    Returns:
        Tuple of (playoff_slots, bye_slots, bracket_reseed, config)

    Raises:
        ValueError: If settings cannot be loaded and num_teams/playoff_start cannot be determined
    """
    global PLAYOFF_SLOTS, BYE_SLOTS, BRACKET_RESEED, TIEBREAKER_ORDER, NUM_TEAMS, REGULAR_SEASON_WEEKS, TARGET_COLS

    most_recent_year = None

    if settings_by_year:
        years = []
        for key in settings_by_year:
            try:
                years.append(int(key))
            except (TypeError, ValueError):
                continue
        if years:
            most_recent_year = max(years)

    # Fallback: get year from DataFrame
    if most_recent_year is None and df is not None and not df.empty:
        most_recent_year = int(df["year"].max())

    if most_recent_year is None:
        raise ValueError("Cannot determine league year - no settings files found and no data provided")

    logger.info(f" Loading league settings for year {most_recent_year}")

    # Use the unified settings loader
    settings = load_league_settings(
        year=most_recent_year,
        data_directory=str(ctx.data_directory),
        df=df,
        settings_by_year=settings_by_year,
    )

    # Extract values from loaded settings
    playoff_slots = int(settings["num_playoff_teams"])
    bye_slots = int(settings["bye_teams"])
    bracket_reseed = bool(settings.get("uses_playoff_reseeding", 0))
    use_median = bool(settings.get("uses_median", 0))
    num_teams = int(settings["num_teams"])
    playoff_start_week = int(settings["playoff_start_week"])
    seeding_rule = normalize_playoff_seeding_rule(
        settings.get("playoff_seeding_rule"),
        settings.get("playoff_seeding_rule_by"),
    )

    regular_season_weeks = playoff_start_week - 1

    tiebreaker_order = DEFAULT_TIEBREAKER_ORDER
    if settings_by_year:
        row = settings_by_year.get(most_recent_year) or settings_by_year.get(str(most_recent_year)) or {}
        if isinstance(row, dict):
            explicit_order = row.get("tiebreaker_order")
            if isinstance(explicit_order, list) and explicit_order:
                tiebreaker_order = explicit_order

    logger.info(" League Configuration (from settings):")
    logger.debug(f"  -Number of Teams: {num_teams}")
    logger.debug(f"  -Playoff Teams: {playoff_slots}")
    logger.debug(f"  -Bye Slots: {bye_slots}")
    logger.debug(f"  -Bracket Reseeding: {bracket_reseed}")
    logger.debug(f"  -Playoff Start Week: {playoff_start_week}")
    logger.debug(f"  -Regular Season Weeks: {regular_season_weeks}")
    logger.debug(f"  -Seeding Rule: {seeding_rule}")
    logger.debug(f"  -Tiebreaker Order: {tiebreaker_order}")

    # Build PlayoffConfig dataclass (new canonical config object)
    config = PlayoffConfig(
        playoff_slots=playoff_slots,
        bye_slots=bye_slots,
        num_teams=num_teams,
        regular_season_weeks=regular_season_weeks,
        use_median=use_median,
        bracket_reseed=bracket_reseed,
        tiebreaker_order=tiebreaker_order,
    )

    # Set global variables (compatibility shim — will be removed in later tasks)
    PLAYOFF_SLOTS = playoff_slots
    BYE_SLOTS = bye_slots
    BRACKET_RESEED = bracket_reseed
    TIEBREAKER_ORDER = tiebreaker_order
    NUM_TEAMS = num_teams
    REGULAR_SEASON_WEEKS = regular_season_weeks

    # Update TARGET_COLS dynamically based on league settings
    TARGET_COLS = config.target_cols

    return playoff_slots, bye_slots, bracket_reseed, config


# -------------------------
# Odds-writing helper
# -------------------------
def write_odds_to_row(df, idx, manager, odds_df, seed_df, win_df):
    """Write simulation results to a single row of the output DataFrame."""
    if manager not in odds_df.index:
        return

    def _set(col, val):
        if col in df.columns and pd.notna(val):
            df.at[idx, col] = round(float(val), 2) if isinstance(val, (float, np.floating, int)) else val

    col_map = {
        "avg_seed": "Avg_Seed",
        "exp_final_wins": "Exp_Final_Wins",
        "exp_final_pf": "Exp_Final_PF",
        "p_semis": "P_Semis",
        "p_final": "P_Final",
        "p_champ": "P_Champ",
        "p_playoffs": "P_Playoffs",
        "p_bye": "P_Bye",
    }
    for target, source in col_map.items():
        if source in odds_df.columns:
            _set(target, odds_df.at[manager, source])

    if "Power_Rating" in odds_df.columns:
        if "_power_rating_raw" in df.columns:
            _set("_power_rating_raw", odds_df.at[manager, "Power_Rating"])
        else:
            _set("power_rating", odds_df.at[manager, "Power_Rating"])

    if seed_df is not None and not seed_df.empty and manager in seed_df.index:
        for k in seed_df.columns:
            if isinstance(k, int) and k >= 1:
                _set(f"x{k}_seed", seed_df.loc[manager, k])

    if win_df is not None and manager in win_df.index:
        for col in win_df.columns:
            if col.startswith("x") and col.endswith("_win"):
                _set(col, win_df.at[manager, col])


def normalize_power_rating_by_season(df: pd.DataFrame, raw_col: str = "_power_rating_raw") -> pd.DataFrame:
    """Rebase raw power ratings to a league-season median 100 index.

    The weekly sim emits raw expected scoring strength. Normalizing once after
    all weeks are processed makes the stored ``power_rating`` comparable within
    each league season and keeps reruns idempotent by using this run's raw temp
    column when present.
    """
    if "power_rating" not in df.columns or "year" not in df.columns:
        return df

    source_col = raw_col if raw_col in df.columns else "power_rating"
    raw_values = pd.to_numeric(df[source_col], errors="coerce")
    if raw_values.notna().sum() == 0:
        return df.drop(columns=[raw_col], errors="ignore")

    for season, idx in df.loc[raw_values.notna()].groupby("year").groups.items():
        season_raw = raw_values.loc[idx]
        baseline = season_raw.dropna().median()
        if pd.isna(baseline) or baseline == 0:
            normalized = season_raw
        else:
            normalized = (season_raw / float(baseline)) * 100.0
        df.loc[idx, "power_rating"] = normalized.round(2)

    return df.drop(columns=[raw_col], errors="ignore")


def resolve_sim_key(row, odds_index, fid_to_manager=None):
    """Resolve the lookup key used by simulated odds tables.

    The simulation stack now prefers stable team identity, so many odds tables
    are indexed by ``franchise_id``. Older callers and some test fixtures still
    use manager display names, so we try both in a safe order.
    """
    fid_to_manager = fid_to_manager or {}

    fid = row.get("franchise_id")
    if pd.notna(fid) and fid in odds_index:
        return fid

    mgr = row.get("manager")
    if mgr in odds_index:
        return mgr

    if pd.notna(fid):
        sim_mgr = fid_to_manager.get(fid)
        if sim_mgr in odds_index:
            return sim_mgr

    return None


# -------------------------
# Main processing function
# -------------------------
def process_parquet_files(
    ctx: LeagueContext = None,
    *,
    conn=None,
    db_name: str = None,
    data_directory: str = None,
    settings_by_year: dict[int, dict] | None = None,
    data_dir: str | None = None,
    target_year: int | None = None,
):
    """Process playoff odds using the supplied local DuckDB connection.

    Args:
        ctx: LeagueContext (legacy, used for data_directory only)
        conn: DuckDB connection
        db_name: League database name
        data_directory: Path to data directory (for settings detection)
        data_dir: Path to local DuckDB directory (runs locally instead of MotherDuck)
    """
    from multi_league.core.db_utils import get_pipeline_connection

    # Get or create connection
    if conn is None:
        if db_name:
            conn = get_pipeline_connection(db_name, data_dir=data_dir, qualified=True)
        elif ctx is not None:
            from multi_league.core.db_utils import get_db_name

            db_name = get_db_name(ctx)
            conn = get_pipeline_connection(db_name, data_dir=data_dir, qualified=True)
        else:
            raise ValueError("Either conn+db_name or ctx must be provided")

    # Build a shim ctx for internal functions that need ctx.data_directory
    if ctx is None:
        _data_dir = data_directory or "."

        class _ShimCtx:
            def __init__(self, dd):
                self.data_directory = Path(dd)

        ctx = _ShimCtx(_data_dir)

    logger.info(f"Loading data from local DuckDB: {db_name}")

    # Surgical read — only columns the sim needs (no SELECT *)
    _needed_matchup = [
        "year",
        "week",
        "manager",
        "team_points",
        "opponent",
        "opponent_points",
        "win",
        "loss",
        "tie",
        "franchise_id",
        "opponent_franchise_id",
        "team_name",
        "is_playoffs",
        "is_consolation",
        "is_bye_week",
        "wins_to_date",
        "losses_to_date",
        "ties_to_date",
        "playoff_seed",
        "final_playoff_seed",
        "points_to_date",
        "inflation_rate",
        "above_league_median",
        "num_playoff_teams",
        "num_bye_teams",
        "playoff_round",
        "consolation_round",
        "season_result",
    ]
    matchup_sql = matchup_table(conn)
    _avail = {r[0] for r in conn.execute(f"DESCRIBE {matchup_sql}").fetchall()}
    # Local DuckDB files don't have db_name — only filter when it exists
    _is_local = "db_name" not in _avail
    _db_where = "" if _is_local else f"WHERE {league_db_filter(db_name)}"
    _matchup_select = [c for c in _needed_matchup if c in _avail]
    df_matches = conn.execute(f"SELECT {', '.join(_matchup_select)} FROM {matchup_sql} {_db_where}").fetchdf()
    if df_matches.empty:
        logger.warning(f"No matchup records found for {db_name}; skipping playoff odds processing")
        return 0

    # Resolve settings before the per-season loop.  A malformed/incomplete
    # settings row for one historical year must not suppress odds for the rest
    # of the league; unresolved years receive an auditable data-derived
    # projection so title odds remain computable when no playoff games exist.
    settings_by_year = resolve_settings_for_projection(
        df_matches,
        data_directory=str(ctx.data_directory),
        settings_by_year=settings_by_year,
    )

    # Surgical schedule read
    try:
        _sched_need = [
            "year",
            "week",
            "manager",
            "opponent",
            "franchise_id",
            "opponent_franchise_id",
            "is_playoffs",
            "is_consolation",
        ]
        schedule_sql = schedule_table(conn)
        _sched_avail = {r[0] for r in conn.execute(f"DESCRIBE {schedule_sql}").fetchall()}
        _sched_db_where = "" if "db_name" not in _sched_avail else f"WHERE {league_db_filter(db_name)}"
        _sched_sel = [c for c in _sched_need if c in _sched_avail]
        df_sched = conn.execute(f"SELECT {', '.join(_sched_sel)} FROM {schedule_sql} {_sched_db_where}").fetchdf()
    except Exception:
        logger.error(f" Schedule table not found in {db_name}")
        return 1

    logger.info(f"Loaded {len(df_matches)} matchup records (including bye weeks)")
    logger.info(f"Loaded {len(df_sched)} schedule records")

    # Fix DuckDB nullable boolean columns before any filtering
    for _bcol in ["is_bye_week", "is_playoffs", "is_consolation"]:
        if _bcol in df_matches.columns:
            df_matches[_bcol] = (
                df_matches[_bcol]
                .astype(str)
                .str.strip()
                .str.lower()
                .isin({"true", "1", "1.0"})
                .astype(int)
            )

    # NOTE: Bye/placeholder rows (is_bye_week=1, opponent=NULL) are KEPT in df_all.
    # They were created by enforce_postseason_flags and must persist through the
    # pipeline so that holdover can write frozen probability values to them.
    # The sim engine naturally skips them: calc_regular_week_outputs only processes
    # is_playoffs=0 weeks, and calc_playoff_week_outputs uses build_playoff_week_mask
    # which explicitly handles is_bye_week=1 rows.
    if "is_bye_week" in df_matches.columns:
        bye_week_count = int(df_matches["is_bye_week"].sum())
        if bye_week_count > 0:
            logger.debug(f"Preserving {bye_week_count} bye/placeholder rows in pipeline")

    # Clean blanks
    for df in (df_matches, df_sched):
        df.replace("", np.nan, inplace=True)

    # Coerce numeric columns (bye/placeholder rows may have NaN/mixed types)
    numeric_cols_matches = [
        "week",
        "year",
        "is_playoffs",
        "is_consolation",
        "inflation_rate",
        "team_points",
        "opponent_points",
        "win",
        "loss",
        "tie",
        "wins_to_date",
        "losses_to_date",
        "ties_to_date",
        "points_to_date",
        "final_playoff_seed",
        "playoff_seed",
        "is_bye_week",
    ]
    for col in numeric_cols_matches:
        if col in df_matches.columns:
            df_matches[col] = pd.to_numeric(df_matches[col], errors="coerce")

    for col in ["week", "year", "is_playoffs", "is_consolation"]:
        if col in df_sched.columns:
            df_sched[col] = pd.to_numeric(df_sched[col], errors="coerce")

    # Ensure target columns exist
    missing_targets = [col for col in TARGET_COLS if col not in df_matches.columns]
    if missing_targets:
        df_matches = pd.concat(
            [
                df_matches,
                pd.DataFrame(
                    np.nan,
                    index=df_matches.index,
                    columns=missing_targets,
                ),
            ],
            axis=1,
        )

    # Generate historical snapshots for kernel-based seed prediction
    # Note: For the first season in dataset, hist_df will be empty - this is handled
    # in calc_regular_week_outputs() by checking for prior history before blending
    # Exclude bye/placeholder rows — they have no game data for snapshot calculation.
    _games_for_hist = df_matches
    if "is_bye_week" in df_matches.columns:
        _games_for_hist = df_matches[df_matches["is_bye_week"] != 1]
    hist_df = history_snapshots(_games_for_hist, PLAYOFF_SLOTS)

    seasons = sorted(df_matches["year"].dropna().unique().astype(int))
    if target_year is not None:
        seasons = [season for season in seasons if season == int(target_year)]
        if not seasons:
            raise ValueError(f"No matchup rows found for target year {target_year}")

    # A season whose playoff settings cannot be resolved by ANY source (e.g. an in-progress
    # successor year with zero playoff games, or a historical year the platform never exposed
    # playoff config for) is unsimulatable -- skip IT, not the league. One 2025 shell season
    # was killing 15 good years of MFL history (crawl run 29626415574).
    simulatable = []
    for season in seasons:
        try:
            load_league_settings(
                int(season),
                data_directory=str(ctx.data_directory),
                df=df_matches[df_matches["year"] == season],
                settings_by_year=settings_by_year,
            )
            simulatable.append(season)
        except ValueError as exc:
            logger.warning(
                f" Skipping season {season}: playoff settings unresolvable ({exc}). "
                "Sim outputs (odds/clutch) will be NULL for this season."
            )
    seasons = simulatable

    df_all = (
        df_matches.loc[pd.to_numeric(df_matches["year"], errors="coerce").eq(int(target_year))].copy()
        if target_year is not None
        else df_matches.copy()
    )

    # Create placeholder rows for bye teams BEFORE processing playoff weeks
    # This ensures bye team rows exist when simulation results are written
    # Otherwise, bye teams get stale p_champ values from their last regular season week
    logger.info("Ensuring all managers have rows for all playoff weeks (including bye weeks)...")
    try:
        df_all = ensure_all_managers_have_playoff_rows(df_all, data_directory=str(ctx.data_directory))
    except Exception as e:
        logger.warning(f" Could not add missing playoff rows: {e}")

    # Keep raw expected scoring strength separate until every week is processed.
    # Final stored power_rating is normalized once by league-season median.
    df_all["_power_rating_raw"] = np.nan

    # Pre-calculate season-level statistics for all seasons
    # This allows us to use full-season variance estimates even for early-week calculations
    # without introducing lookahead bias (we're using league-level variance, not individual outcomes)
    logger.info("Pre-calculating season-level statistics...")
    season_stats = {}
    for season in seasons:
        df_season_all = df_matches[df_matches["year"] == season].copy()
        # Exclude bye/placeholder rows from season stats (they have NaN team_points)
        if "is_bye_week" in df_season_all.columns:
            df_season_all = df_season_all[df_season_all["is_bye_week"] != 1]
        if not df_season_all.empty:
            season_mean = df_season_all["team_points"].mean()
            season_sd = df_season_all["team_points"].std(ddof=1)
            season_stats[season] = {"mean": float(season_mean), "sd": float(season_sd), "n_games": len(df_season_all)}
            logger.debug(f"{season}: Mean={season_mean:.1f}, SD={season_sd:.1f}, Games={len(df_season_all)}")

    for season in seasons:
        logger.info(f"Processing season {season}...")
        df_season_raw = df_matches[df_matches["year"] == season].copy()
        if df_season_raw.empty:
            continue
        # Filter out bye/placeholder rows for sim input — they have no game data.
        # df_all retains them for holdover writes.
        if "is_bye_week" in df_season_raw.columns:
            df_season = df_season_raw[df_season_raw["is_bye_week"] != 1].copy()
        else:
            df_season = df_season_raw

        # Get pre-calculated season statistics
        season_stat = season_stats.get(season, {})

        reg_weeks = sorted(df_season[df_season["is_playoffs"] == 0]["week"].dropna().unique().astype(int))
        po_weeks = sorted(
            df_season[(df_season["is_playoffs"] == 1) & (df_season["is_consolation"] == 0)]["week"]
            .dropna()
            .unique()
            .astype(int)
        )

        # Build franchise_id -> manager name lookup for robust matching
        # This handles cases where manager names are disambiguated but franchise_id stays stable.
        fid_to_manager = {}
        for _, row in df_season[["franchise_id", "manager"]].drop_duplicates().iterrows():
            if pd.notna(row["franchise_id"]) and pd.notna(row["manager"]):
                fid_to_manager[row["franchise_id"]] = row["manager"]
        logger.debug(f"Built franchise_id lookup with {len(fid_to_manager)} entries")

        # Process regular season weeks
        for w in reg_weeks:
            logger.debug(f"Processing regular season week {w}...")
            mask_week = (df_all["year"] == season) & (df_all["week"] == w) & (df_all["is_consolation"] == 0)
            if not mask_week.any():
                continue

            odds_df, seed_df, win_df = calc_regular_week_outputs(
                df_season,
                df_sched,
                season,
                w,
                hist_df,
                season_stat,
                data_directory=ctx.data_directory,
                settings_by_year=settings_by_year,
            )

            odds_index = set(odds_df.index) if not odds_df.empty else set()

            for idx in df_all[mask_week].index:
                row = df_all.loc[idx]
                m = resolve_sim_key(row, odds_index, fid_to_manager)
                if m is not None:
                    write_odds_to_row(df_all, idx, m, odds_df, seed_df, win_df)

        # Process playoff weeks
        for w in po_weeks:
            logger.debug(f"Processing playoff week {w}...")

            # Get playoff teams for this week to include bye team rows
            odds_df, seed_df, seeds = calc_playoff_week_outputs(
                df_season,
                df_sched,
                season,
                w,
                season_stat,
                data_directory=ctx.data_directory,
                settings_by_year=settings_by_year,
            )

            # Build mask for playoff rows including bye teams using helper function
            if "made_playoffs" in seeds.columns and "franchise_id" in seeds.columns:
                playoff_team_set = set(seeds.loc[seeds["made_playoffs"], "franchise_id"].dropna().tolist())
            elif "made_playoffs" in seeds.columns:
                raise ValueError("calc_playoff_week_outputs must return franchise_id for playoff week masking")
            else:
                playoff_team_set = set(odds_df.index)
            mask_week = build_playoff_week_mask(df_all, season, w, playoff_team_set)

            if not mask_week.any():
                continue

            odds_index = set(odds_df.index) if not odds_df.empty else set()

            for idx in df_all[mask_week].index:
                row = df_all.loc[idx]
                m = resolve_sim_key(row, odds_index, fid_to_manager)
                if m is not None:
                    write_odds_to_row(df_all, idx, m, odds_df, seed_df, None)

    logger.info("Normalizing power ratings to league-season median index...")
    df_all = normalize_power_rating_by_season(df_all)

    # Add playoff scenario columns (p_playoffs_change, p_champ_change, is_critical_matchup, etc.)
    # These require p_playoffs column to exist, which was just calculated above
    logger.info("Adding playoff scenario columns (changes, critical matchups)...")
    try:
        from transformations.matchup.modules.playoff_scenarios import add_playoff_scenario_columns

        df_all = add_playoff_scenario_columns(df_all, data_directory=str(ctx.data_directory))
        logger.info(" Playoff scenario columns added")
    except Exception as e:
        logger.warning(f" Could not add playoff scenario columns: {e}")

    # Note: ensure_all_managers_have_playoff_rows() is called earlier (before playoff week processing)
    # to ensure bye team rows exist when simulation results are written

    # Hold playoff values for eliminated teams
    # This freezes values from final regular season and sets eliminated odds to 0%
    logger.info("Holding playoff values for eliminated teams...")
    try:
        # Read playoff_round_type from most recent year settings
        import math as _m

        _most_recent = max(df_all["year"].dropna().unique().astype(int))
        _prt_settings = (
            settings_by_year.get(_most_recent, settings_by_year.get(str(_most_recent), {})) if settings_by_year else {}
        )
        _prt = int(_prt_settings.get("playoff_round_type", 0)) if isinstance(_prt_settings, dict) else 0
        _ps_week = int(_prt_settings.get("playoff_start_week", 14)) if isinstance(_prt_settings, dict) else 14
        _np_teams = int(_prt_settings.get("num_playoff_teams", 6)) if isinstance(_prt_settings, dict) else 6

        # Derive end_week for the most recent season
        if _prt > 0:
            _po = (
                sorted(
                    df_all[(df_all["year"] == _most_recent) & (df_all["is_playoffs"] == 1)]["week"]
                    .dropna()
                    .unique()
                    .astype(int)
                )
                if not df_all.empty
                else []
            )
            if _po:
                _ew = max(_po)
            else:
                _nr = _m.ceil(_m.log2(max(_np_teams, 2)))
                _tw = sum(2 if (_prt == 1 or (_prt == 2 and r == _nr)) else 1 for r in range(1, _nr + 1))
                _ew = _ps_week + _tw - 1
        else:
            _ew = None

        df_all = hold_playoff_values_for_eliminated(
            df_all,
            data_directory=str(ctx.data_directory),
            playoff_round_type=_prt,
            playoff_start_week=_ps_week,
            end_week=_ew,
            num_playoff_teams=_np_teams,
        )
    except Exception as e:
        logger.warning(f" Could not hold playoff values: {e}")

    df_all = _normalize_x_win_distribution(df_all)
    df_all = fill_missing_title_odds(df_all)

    # DEFENSIVE: Ensure string columns are never null (use empty string instead)
    # Cast to object first to handle Int32/nullable integer columns that can't accept ""
    string_cols_to_clean = ["playoff_round", "consolation_round", "season_result"]
    for col in string_cols_to_clean:
        if col in df_all.columns:
            df_all[col] = df_all[col].astype(object).fillna("").astype(str)

    # Surgical write — UPDATE only produced columns (no DROP TABLE)
    logger.info("Saving results to local DuckDB (surgical UPDATE)...")

    # Some older league tables still carry pre-DDL numeric types for round/result
    # columns. Normalize them to canonical VARCHAR before writing string labels.
    matchup_sql = matchup_table(conn)
    _target_schema = {r[0]: str(r[1]).upper() for r in conn.execute(f"DESCRIBE {matchup_sql}").fetchall()}
    _varchar_cols = {
        "playoff_round",
        "consolation_round",
        "season_result",
    }
    for col in _varchar_cols:
        col_type = _target_schema.get(col, "")
        if col_type and not any(token in col_type for token in ("CHAR", "TEXT", "STRING", "VARCHAR")):
            logger.info(f'Normalizing matchup."{col}" from {col_type} to VARCHAR before playoff update')
            conn.execute(
                f"""
                ALTER TABLE {matchup_sql}
                ALTER COLUMN "{col}" SET DATA TYPE VARCHAR
                USING CASE
                    WHEN "{col}" IS NULL THEN NULL
                    ELSE CAST("{col}" AS VARCHAR)
                END
                """
            )

    # Columns that are pure inputs (never modified by sim)
    _read_only = {
        "year",
        "week",
        "manager",
        "team_points",
        "opponent",
        "opponent_points",
        "win",
        "loss",
        "tie",
        "franchise_id",
        "team_name",
        "is_playoffs",
        "is_consolation",
        "is_bye_week",
        "wins_to_date",
        "losses_to_date",
        "ties_to_date",
        "playoff_seed",
        "final_playoff_seed",
        "points_to_date",
        "inflation_rate",
        "above_league_median",
        "num_playoff_teams",
        "num_bye_teams",
        "playoff_round",
        "consolation_round",
        "season_result",
    }
    _output_cols = [c for c in df_all.columns if c not in _read_only]

    if "matchup_id" in df_all.columns:
        df_all["matchup_id"] = pd.to_numeric(df_all["matchup_id"], errors="coerce")

    _ensure_matchup_output_columns(conn, db_name, _output_cols)

    # UPDATE existing rows via register + join
    # Use franchise_id for the join (stable across name changes)
    conn.register("_playoff_result", df_all)
    _set_clause = ", ".join(f'"{c}" = u."{c}"' for c in _output_cols)
    if "franchise_id" not in _avail:
        raise ValueError("playoff_odds_import requires matchup.franchise_id for stable writeback joins")
    _join_clause = "t.year = u.year AND t.week = u.week AND t.franchise_id = u.franchise_id"
    _update_db_filter = f"AND {league_db_filter(db_name, 't')}" if not _is_local else ""
    execute_scoped(
        conn,
        f"""
        UPDATE {matchup_sql} t
        SET {_set_clause}
        FROM _playoff_result u
        WHERE {_join_clause}
          {_update_db_filter}
        """,
        db_name,
        label="playoff_odds:update",
    )
    conn.unregister("_playoff_result")

    logger.info("Processing complete!")
    logger.info(f"Updated {len(_output_cols)} columns on {len(df_all)} rows in {matchup_sql}")


# -------------------------
# Main
# -------------------------
def main(argv: list[str] | None = None):
    """Main entry point with argument parsing."""
    global N_SIMS  # Declare at top before any reference to N_SIMS

    parser = argparse.ArgumentParser(
        description="Calculate playoff odds and probabilities for fantasy football matchups",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run with league context (RECOMMENDED)
    python playoff_odds_import.py --context /path/to/league_context.json

Note:
    This script reads playoff configuration (num_playoff_teams, bye_slots, etc.)
    from the canonical league_settings table. If settings are not available,
    it falls back to inferred/default playoff structure.

    The script takes >10 minutes to run due to Monte Carlo simulations.
        """,
    )
    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument(
        "--db", type=str, help="MotherDuck database name (alternative to --context, reads/writes directly)"
    )
    parser.add_argument(
        "--n-sims", type=int, default=None, help=f"Number of Monte Carlo simulations (default: {N_SIMS})"
    )
    parser.add_argument("--target-year", type=int, help="Only recalculate and write this league season")
    parser.add_argument(
        "--data-dir",
        type=str,
        default=None,
        help="Path to local DuckDB directory (runs locally instead of MotherDuck)",
    )

    args = parser.parse_args(argv)

    if not args.context and not args.db:
        parser.error("Either --context or --db is required")

    # Override N_SIMS if provided
    if args.n_sims is not None:
        N_SIMS = args.n_sims
        logger.info(f"Using {N_SIMS} simulations (from --n-sims argument)")

    logger.info("Fantasy Football Playoff Odds Calculator")

    # ── Resolve database connection ──
    if args.db:
        from multi_league.core.db_utils import sanitize_database_name as _sanitize_database_name

        _db_name = _sanitize_database_name(args.db)
    elif args.context:
        # Auto-convert --context to --db
        if not args.context.exists():
            logger.error(f" League context file not found: {args.context}")
            sys.exit(1)
        try:
            ctx = LeagueContext.load_readonly(str(args.context))
            logger.info(f"Loaded context for league: {ctx.league_name}")
        except Exception as e:
            logger.error(f" Failed to load league context: {e}")
            import traceback

            traceback.print_exc()
            sys.exit(1)
        from multi_league.core.db_utils import get_db_name

        _db_name = get_db_name(ctx)
        logger.info(f"Auto-converting --context to --db {_db_name}")
        args.db = _db_name
        args.context = None

    # Need a data_directory for settings detection (create temp if needed)
    if args.context:
        from multi_league.core.db_utils import get_pipeline_connection as _get_conn

        _conn = _get_conn(_db_name, data_dir=args.data_dir, qualified=True)
        _data_dir = Path(ctx.data_directory)
        _settings_by_year = {}
    else:
        from multi_league.core.db_context import DbContext

        _db = DbContext(_db_name, data_dir=args.data_dir)
        _db.ensure_settings_dir()
        _conn = _db.conn  # Reuse DbContext connection
        _data_dir = _db.data_directory
        _settings_by_year = _db.get_settings()

    logger.info(f"Connected to database: {_db_name}")

    # Store connection info for later use
    args._db_name = _db_name
    args._conn = _conn
    args._data_dir = _data_dir
    args._settings_by_year = _settings_by_year

    # Load matchup data (needed for settings inference)
    logger.info("LOADING MATCHUP DATA")

    # Surgical read — only columns needed for config inference
    _cfg_cols = "year, week, manager, is_playoffs, is_consolation, is_bye_week, opponent, team_points"
    matchup_sql = matchup_table(_conn)
    # Local DuckDB files don't have db_name column — only filter when it exists
    _matchup_cols = {r[0] for r in _conn.execute(f"DESCRIBE {matchup_sql}").fetchall()}
    _where = f"WHERE {league_db_filter(_db_name)}" if "db_name" in _matchup_cols else ""
    df_matches = _conn.execute(f"SELECT {_cfg_cols} FROM {matchup_sql} {_where}").fetchdf()
    if df_matches.empty:
        logger.warning(f"No matchup records found for {_db_name}; skipping playoff odds and clutch calculations")
        _conn.close()
        return
    logger.info(f"Loaded {len(df_matches)} matchup records")

    args._settings_by_year = resolve_settings_for_projection(
        df_matches,
        data_directory=str(_data_dir),
        settings_by_year=args._settings_by_year,
    )

    # Load playoff configuration from league settings
    logger.info("LOADING PLAYOFF CONFIGURATION")

    # Create a minimal ctx-like object for load_playoff_config_from_settings
    class _MinCtx:
        def __init__(self, data_dir):
            self.data_directory = Path(data_dir)

    _ctx = _MinCtx(_data_dir)

    try:
        playoff_slots, bye_slots, bracket_reseed, config = load_playoff_config_from_settings(
            _ctx,
            df=df_matches,
            settings_by_year=args._settings_by_year,
        )
    except Exception as e:
        logger.error(f" Failed to load playoff configuration: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    # Process playoff odds
    logger.info("CALCULATING PLAYOFF ODDS")

    try:
        process_parquet_files(
            conn=args._conn,
            db_name=args._db_name,
            data_directory=str(args._data_dir),
            settings_by_year=args._settings_by_year,
            data_dir=args.data_dir,
            target_year=args.target_year,
        )
        logger.info("SUCCESS - Playoff odds calculation complete!")
    except Exception as e:
        logger.error(f"Playoff odds calculation failed: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)

    # DuckDB is an in-process database — only one process can hold a write
    # lock at a time. Close the parent connection and force-release the file
    # lock before spawning clutch/aggregation subprocesses.
    if getattr(args, "_conn", None) is not None:
        try:
            args._conn.close()
        except Exception as close_exc:
            logger.warning(f"Failed to close parent connection before subprocesses: {close_exc}")
        finally:
            args._conn = None

    # =========================================================================
    # POST-SIM: Populate matchup columns that depend on team_mu/team_sigma
    # =========================================================================
    # expected_spread, expected_odds, win_probability, underdog_wins,
    # favorite_losses all derive from team_mu / team_sigma (written above by
    # process_parquet_files). They live in MatchupEnrichmentsMixin and were
    # historically invoked from compute_derived_matchup_columns, but that
    # enrichment runs in SQLEnrichments.run_all() BEFORE playoff_odds_import —
    # so team_mu wasn't populated yet and the sim-derived block always skipped,
    # leaving those columns NULL and tripping sim_expected_odds_populated on
    # every row. We now run the sim-derived block as its own step here, after
    # sims have written team_mu/team_sigma.
    logger.info("POPULATING SIM-DERIVED MATCHUP COLUMNS (expected_odds etc.)")
    try:
        from multi_league.transformations.sql_enrichments import SQLEnrichments

        with SQLEnrichments(db_name=args._db_name, data_dir=args.data_dir) as _sim_eng:
            _sim_eng.populate_sim_derived_matchup_columns()
        # Force Python to release the connection object and DuckDB file lock
        del _sim_eng
        import gc

        gc.collect()
        logger.info("SUCCESS - Sim-derived matchup columns populated")
    except Exception as sim_exc:
        logger.warning(f"Failed to populate sim-derived matchup columns (non-fatal): {sim_exc}")

    # =========================================================================
    # POST-PLAYOFF-ODDS: Clutch Equity
    # =========================================================================
    # Clutch needs p_champ values from sims above, so it runs here.
    # Import in-process to avoid DuckDB file-lock conflicts that occur
    # when spawning a subprocess while the parent still holds a lock.
    logger.info("CALCULATING CLUTCH EQUITY")
    try:
        from multi_league.transformations.player.clutch_to_player import main as clutch_main

        _clutch_args = argparse.Namespace(
            db=args._db_name,
            data_dir=args.data_dir,
            context=None,
            dry_run=False,
            backup=False,
            target_year=args.target_year,
        )
        clutch_main(_clutch_args)
        logger.info("SUCCESS - Clutch equity calculation complete!")
    except Exception as e:
        logger.warning(f"Clutch calculation failed: {e}")
        import traceback

        traceback.print_exc()

    # Results written to local DuckDB by process_parquet_files() and clutch_to_player.py
    # Upload to MotherDuck is handled by the workflow's upload step.
    # Close the connection
    try:
        args._conn.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
