"""Team performance modeling — Bayesian shrinkage, recency weighting, score drawing."""

import numpy as np
import pandas as pd
from multi_league.core.identity import get_manager_col, get_opponent_col
from multi_league.core.logging_config import get_logger

logger = get_logger(__name__)

# Model config constants
HALF_LIFE_WEEKS = 14
SHRINK_K = 5.0  # Pseudocounts for Bayesian shrinkage (unitless)
SIGMA_FLOOR_RATIO = 0.8  # Team variance floor as fraction of league SD
N_SIMS = 1000  # Reduced from 10000 for faster initial import (can be increased via --n-sims)
RNG_SEED = 42

# Fallback coefficient of variation (unitless ratio) - used when SD can't be calculated
# 15% CV is typical for fantasy football week-to-week scoring variability
# This is scoring-format agnostic (works for PPR, standard, etc.)
FALLBACK_CV = 0.15

# Early season compression settings
# Compresses extreme probabilities toward neutral in early weeks when data is limited
EARLY_SEASON_MIN_GAMES = 5  # Weeks until full confidence in predictions
EARLY_SEASON_BASE_RATE = 50  # Neutral probability to compress toward


# -------------------------
# Safe numeric utilities
# -------------------------
def safe_mean(values, default=100.0):
    """
    Calculate mean safely, returning default if empty or all NaN.

    Args:
        values: Iterable of numeric values (list, dict.values(), etc.)
        default: Value to return if mean cannot be calculated

    Returns:
        Mean value or default
    """
    arr = np.array(list(values))
    if len(arr) == 0:
        return default
    valid = arr[~np.isnan(arr)]
    if len(valid) == 0:
        return default
    return float(np.mean(valid))


def safe_get(d: dict, key, fallback_dict: dict = None, default=100.0):
    """
    Safely get value from dict with fallback to mean of another dict.

    Args:
        d: Primary dictionary to get from
        key: Key to look up
        fallback_dict: Dict to calculate mean from if key not found
        default: Final fallback if fallback_dict is also empty

    Returns:
        Value for key, or mean of fallback_dict, or default
    """
    val = d.get(key)
    if val is not None and not (isinstance(val, float) and np.isnan(val)):
        return val
    if fallback_dict and len(fallback_dict) > 0:
        return safe_mean(fallback_dict.values(), default)
    return default


def safe_col_get(df: pd.DataFrame, idx, col: str, default=None):
    """
    Safely get a value from a DataFrame at a specific index and column.

    Args:
        df: DataFrame to access
        idx: Index to access
        col: Column name
        default: Value to return if column doesn't exist or value is NaN

    Returns:
        Value at df.at[idx, col] or default
    """
    if col not in df.columns:
        return default
    try:
        val = df.at[idx, col]
        if pd.isna(val):
            return default
        return val
    except (KeyError, IndexError):
        return default


# -------------------------
# Early season compression
# -------------------------
def compress_early_season(p_raw, week, base_rate=EARLY_SEASON_BASE_RATE, min_games=EARLY_SEASON_MIN_GAMES):
    """
    Compress extreme probabilities toward base_rate in early weeks.

    Uses sqrt for gentler transition - predictions become fully trusted by week 5.
    This improves calibration in weeks 1-4 without affecting later weeks.

    Scales to any league - based on games played, not league-specific values.

    Args:
        p_raw: Raw probability (0-100 scale)
        week: Current week number (proxy for games played)
        base_rate: Neutral probability to compress toward (default 50)
        min_games: Games needed before trusting raw probabilities (default 5)

    Returns:
        Compressed probability (0-100 scale)
    """
    confidence = min(1.0, np.sqrt(week / min_games))
    return confidence * p_raw + (1 - confidence) * base_rate


# -------------------------
# Recency weights
# -------------------------
def recency_weights(df, season, week, half_life, boundary_penalty=0.05, boundary_weeks=4):
    """
    Calculate recency weights for historical data.

    Args:
        df: DataFrame with year and week columns
        season: Current season
        week: Current week
        half_life: Half-life for exponential decay (in weeks)
        boundary_penalty: Penalty factor for prior season data at season start
        boundary_weeks: Number of weeks over which to fade the boundary penalty
                       (proportional to season length, ~25% of regular season)
    """
    hl = max(1, int(half_life))
    lam = np.log(2.0) / hl

    timeline = (df["year"] - season) * 100 + (df["week"] - week)
    weeks_ago = np.maximum(0, (-timeline).astype(float))
    base = np.exp(-lam * weeks_ago)

    prior = (df["year"] < season).astype(float)
    # Proportional fade over boundary_weeks (not hardcoded)
    fade = min(max((week - 1) / float(boundary_weeks), 0.0), 1.0)

    penalty = (1 - fade) * boundary_penalty + fade * 1.0
    return base * np.where(prior == 1.0, penalty, 1.0)


# -------------------------
# Team models
# -------------------------
def build_team_models(
    hist,
    season,
    week,
    half_life,
    shrink_k,
    boundary_penalty=0.05,
    prior_w_cap=2.0,
    season_stats=None,
    regular_season_weeks=None,
):
    """
    Build team performance models with all parameters derived from league data.
    No hardcoded point values - works for any scoring format.

    Args:
        hist: Historical matchup data
        season: Current season year
        week: Current week number
        half_life: Recency weight half-life in weeks
        shrink_k: Bayesian shrinkage pseudocounts
        boundary_penalty: Weight penalty for prior season data
        prior_w_cap: Maximum weight cap for prior season data
        season_stats: Optional dict with keys 'mean', 'sd', 'n_games' for the current season.
                     If provided, uses full-season variance estimates (improves early-week accuracy).
        regular_season_weeks: Number of regular season weeks (used for proportional thresholds).
                             If None, inferred from data or uses reasonable defaults.
    """
    h = hist.copy()

    # Determine season length for proportional calculations
    if regular_season_weeks is None:
        # Fallback: estimate from max week in current season data
        current_season_weeks = h[h["year"] == season]["week"].max()
        if pd.notna(current_season_weeks):
            regular_season_weeks = max(int(current_season_weeks), 14)  # At least 14 if we have data
        else:
            regular_season_weeks = 14  # Universal fallback for first-year edge case

    # Calculate proportional boundary weeks (~25% of season, minimum 3)
    boundary_weeks = max(3, regular_season_weeks // 4)

    h["w"] = recency_weights(h, season, week, half_life, boundary_penalty, boundary_weeks=boundary_weeks)

    # Use franchise_id when available for stable identity tracking
    _id_col = get_manager_col(h)

    prior_mask = h["year"] < season
    if prior_mask.any():
        w_prior = h.loc[prior_mask].groupby(_id_col)["w"].transform(lambda s: s / max(1e-12, s.sum()))
        h.loc[prior_mask, "w"] = w_prior * prior_w_cap

    # Derive league parameters from data (works for any scoring format)
    if season_stats and "mean" in season_stats and "sd" in season_stats:
        league_mu = season_stats["mean"]
        league_sd = season_stats["sd"]
    else:
        league_mu = h["team_points"].mean()
        league_sd = h["team_points"].std(ddof=1)

    # Handle edge case: if SD is 0 or NaN (e.g., all identical scores or first-year league)
    # Use coefficient of variation based on typical fantasy scoring variability
    if pd.isna(league_sd) or league_sd == 0:
        # FALLBACK_CV is typical for fantasy football; if mean is also NaN, use safe defaults
        if pd.isna(league_mu) or league_mu == 0:
            league_mu = 100.0  # Reasonable default for fantasy scoring
            league_sd = league_mu * FALLBACK_CV  # Derived from CV, not hardcoded
        else:
            league_sd = league_mu * FALLBACK_CV

    # Sigma floor as ratio of league SD (not hardcoded points)
    sigma_floor = league_sd * SIGMA_FLOOR_RATIO

    # Optimized: Use direct aggregation instead of apply with lambda
    # This is significantly faster for large datasets
    h["weighted_pts"] = h["team_points"] * h["w"]
    by_mgr = h.groupby(_id_col).agg(
        n=("team_points", "count"),
        w_sum=("w", "sum"),
        weighted_pts_sum=("weighted_pts", "sum"),
        sd_raw=("team_points", lambda x: x.std(ddof=1) if len(x) >= 2 else np.nan),
    )
    by_mgr["mu_raw"] = by_mgr["weighted_pts_sum"] / (by_mgr["w_sum"] + 1e-12)
    # Clean up temporary column
    h.drop("weighted_pts", axis=1, inplace=True)

    k = float(shrink_k)
    weeks_played = max(0, int(week))

    # Gradual transition from league average to observed data
    # Proportional to season length: shrinkage decreases over first ~25% of season
    shrinkage_weeks = boundary_weeks
    k_eff = k * max(1.0, (shrinkage_weeks - min(weeks_played, shrinkage_weeks)) * (1.0 / shrinkage_weeks))

    w_eb = by_mgr["w_sum"] / (by_mgr["w_sum"] + k_eff)
    mu_hat = (w_eb * by_mgr["mu_raw"] + (1 - w_eb) * league_mu).to_dict()

    # Team-specific variance with shrinkage
    # Teams with < 4 games: use league SD (insufficient data for team-specific estimate)
    # Teams with 4+ games: blend toward league SD based on sample size
    sd_raw = by_mgr["sd_raw"]
    n_games = by_mgr["n"]

    k_sigma = k_eff / 2.0
    w_sigma = by_mgr["w_sum"] / (by_mgr["w_sum"] + k_sigma)

    # Fill NaN (teams with < 2 games) with league SD
    sd_fill = sd_raw.fillna(league_sd)

    # Blend team SD toward league SD
    sigma_raw = w_sigma * sd_fill + (1 - w_sigma) * league_sd

    # Apply floor as ratio of league SD
    sigma_hat = sigma_raw.clip(lower=sigma_floor).to_dict()

    # Build samples for bootstrap
    samples_by_team = {}
    for m, g in h.groupby(_id_col):
        g_cur = g[g["year"] == season]
        if week <= 3 and len(g_cur) >= 2:
            samples_by_team[m] = g_cur[["team_points", "w"]].copy()
        else:
            samples_by_team[m] = g[["team_points", "w"]].copy()

    for m in h[_id_col].unique():
        mu_hat.setdefault(m, league_mu)
        sigma_hat.setdefault(m, sigma_floor)

    return mu_hat, sigma_hat, samples_by_team, league_mu, sigma_floor


def ensure_params_for_future(mu_hat, sigma_hat, samples_by_team, df_future, league_mu, sigma_floor):
    if df_future is None or df_future.empty:
        return
    _id_col = get_manager_col(df_future)
    _opp_col = get_opponent_col(df_future)
    future_mgrs = set(df_future[_id_col]).union(set(df_future[_opp_col]))
    for m in future_mgrs:
        mu_hat.setdefault(m, league_mu)
        sigma_hat.setdefault(m, sigma_floor)
        samples_by_team.setdefault(m, pd.DataFrame({"team_points": [], "w": []}))


# -------------------------
# Power rating calculator
# -------------------------
def compute_power_ratings(mu_hat, samples_by_team, bootstrap_min=3):
    """Return raw expected scoring strength before display normalization."""
    out = {}
    for m, samp in samples_by_team.items():
        use_boot = (
            isinstance(samp, pd.DataFrame)
            and "team_points" in samp.columns
            and "w" in samp.columns
            and len(samp) >= bootstrap_min
            and float(samp["w"].sum()) > 0
        )
        if use_boot:
            out[m] = float(np.average(samp["team_points"].to_numpy(), weights=samp["w"].to_numpy()))
        else:
            out[m] = float(safe_get(mu_hat, m, mu_hat, default=100.0))
    for m in mu_hat.keys():
        out.setdefault(m, float(mu_hat[m]))
    return pd.Series(out, name="Power_Rating")


# -------------------------
# Score draw
# -------------------------
def draw_score(manager, rng, mu_hat, sigma_hat, samples_by_team, use_bootstrap=True, bootstrap_min=3):
    samp = samples_by_team.get(manager)
    if use_bootstrap and isinstance(samp, pd.DataFrame) and len(samp) >= bootstrap_min and float(samp["w"].sum()) > 0:
        p = (samp["w"] / samp["w"].sum()).to_numpy()
        return float(rng.choice(samp["team_points"].to_numpy(), p=p))
    mu = safe_get(mu_hat, manager, mu_hat, default=100.0)
    sd = safe_get(sigma_hat, manager, sigma_hat, default=15.0)
    return float(rng.normal(mu, sd))


# -------------------------
# Simulate a single game score
# -------------------------
def _sim_game_score(
    team: str,
    rng: np.random.Generator,
    mu_hat: dict,
    sigma_hat: dict,
) -> float:
    """Simulate a single game score for one team.

    Returns a single float drawn from N(mu, sigma).
    Used by simulate_round() for 2-week rounds where individual
    scores must be summed.
    """
    mu = mu_hat.get(team, 100.0)
    sigma = max(sigma_hat.get(team, 15.0), 0.1)
    return float(rng.normal(mu, sigma))


# -------------------------
# Simulate a single game
# -------------------------
def _sim_game(a, b, rng, mu_hat, sigma_hat, samples_by_team):
    sa = draw_score(a, rng, mu_hat, sigma_hat, samples_by_team)
    sb = draw_score(b, rng, mu_hat, sigma_hat, samples_by_team)
    if sa > sb:
        return a
    if sb > sa:
        return b
    return rng.choice([a, b])


# -------------------------
# Power rating index normalization
# -------------------------
def index_power_rating(power_series):
    """Normalize raw power ratings to a median 100 index.

    ``power_series`` starts as expected fantasy points. The persisted power
    rating should be rebased over the full league-season distribution so the
    season median is 100. This helper performs the index math for whichever
    cohort the caller passes. A 110 team-week is about 10% above that season's
    median scoring strength, regardless of platform, era, or scoring settings.
    """
    adjusted = power_series.copy()
    baseline = adjusted.dropna().median()
    if pd.isna(baseline) or baseline == 0:
        return adjusted
    return (adjusted / float(baseline)) * 100.0


def normalize_power_rating(power_series, inflation_rate=None):
    """Backward-compatible wrapper for the power rating index.

    ``inflation_rate`` is ignored intentionally. Power ratings are now
    self-scaled within the league-season distribution, so external scoring
    inflation adjustment would double-normalize the same concept.
    """
    return index_power_rating(power_series)


# -------------------------
# Model confidence indicator
# -------------------------
def calculate_model_confidence(df_all, season):
    """
    Calculate model confidence based on historical data depth.
    Returns confidence level and number of prior seasons.

    Args:
        df_all: DataFrame with all historical matchup data
        season: Current season year

    Returns:
        Tuple of (confidence_level: str, n_prior_seasons: int)
        confidence_level is one of: "first_season", "limited", "moderate", "high"
    """
    prior_seasons = df_all[df_all["year"] < season]["year"].nunique()

    if prior_seasons == 0:
        return "first_season", 0
    elif prior_seasons < 3:
        return "limited", prior_seasons
    elif prior_seasons < 6:
        return "moderate", prior_seasons
    else:
        return "high", prior_seasons
