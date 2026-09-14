#!/usr/bin/env python3
"""Shared utilities for the RB next-year-PPG predictability workstream.

Target (confirmed 2026-07-08): the super table's `avg_pts_next_year_*` columns
are the *realized* Y+1 season PPG (a shifted lookup, corr=1.0 vs actual Y+1
`ppg_season_*`), NOT a projection. So the label lives directly on the season-Y
row — no self-join needed, and there is no existing-projection baseline to beat.

Data sources (Fly read-only):
  ___ops.nfl_historical.player_nfl_season_all  season-grain features + label
  ___ops.nfl_historical.player_bio             time-invariant bio / draft pedigree

Leakage discipline:
  * Label family `avg_pts_next_year_*` is NEVER a feature.
  * player_bio is CAREER-level; only time-invariant fields are used
    (draft pedigree, physical, combine/RAS, rookie_year). career_games /
    years_active / allpro / probowls / w_av include FUTURE seasons -> excluded.
    Experience is derived as (year - rookie_year), not years_active.
  * Features are season Y and earlier only.

Default scoring variant: 4pt passing / 0.5 PPR  -> the `4pt_half` family.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

# --- make `multi_league` importable and load Fly creds from repo-root .env ---
_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "fantasy_football_data_scripts"))
_ENV = _REPO / ".env"
if _ENV.exists():
    for _line in _ENV.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k, _v.strip())

SCRATCH = Path(
    r"C:/Users/joeye/AppData/Local/Temp/claude/d--yahoo-oauth/"
    r"03fb2042-2335-4f17-87ec-4d7c4002d157/scratchpad"
)
DATASET = SCRATCH / "rb_seasons.parquet"

VARIANT = "4pt_half"                              # default scoring
LABEL = f"avg_pts_next_year_{VARIANT}"           # realized Y+1 PPG
PERSIST = f"ppg_season_{VARIANT}"                # Y PPG = persistence baseline

# ---- season-Y RB feature families (all pre-Y+1, non-leaky) ------------------
# Rank/finish + weighted/consistency + lamar are current-year *persistence
# proxies*; we keep them so the sweep can show them topping the board, but the
# real prize is combos that add lift OVER persistence.
SEASON_FEATURES = [
    # volume / usage
    "carries", "targets", "receptions", "total_touches", "scrimmage_yards",
    "rushing_yards", "receiving_yards", "target_share", "air_yards_share",
    "wopr", "receiving_air_yards",
    "rz_carries", "rz_targets", "rz_rush_td", "rz_rec_td", "rz_pass_att",
    "rushing_first_downs", "receiving_first_downs",
    "rushing_40plus", "receptions_40plus", "rushing_tds_40plus",
    "receptions_0_4", "receptions_5_9", "receptions_10_19",
    "receptions_20_29", "receptions_30_39", "rushing_long", "receiving_long",
    # efficiency
    "yards_per_carry", "yards_per_touch", "yards_per_reception",
    "yards_per_target", "adot", "catch_rate", "rush_td_pct", "rec_td_pct",
    "racr",
    # advanced value
    "rushing_epa", "receiving_epa", "total_epa", "rushing_wpa",
    "receiving_wpa", "total_wpa", "rush_success", "rush_success_plays",
    "rec_success", "rec_success_plays", "rush_explosive_10", "rec_explosive_20",
    # production / TDs
    "rushing_tds", "receiving_tds", "total_tds", "receiving_2pt_conversions",
    "rushing_2pt_conversions",
    # NGS (2016+)
    "ngs_rush_yards_over_expected", "ngs_rush_pct_over_expected",
    "ngs_rush_efficiency", "ngs_expected_rush_yards", "ngs_avg_time_to_los",
    "ngs_pct_att_gte_8_defenders", "ngs_avg_yac_above_expectation",
    "ngs_avg_separation", "ngs_avg_cushion", "ngs_pct_share_intended_air_yards",
    # durability
    "rushing_fumbles", "rushing_fumbles_lost", "receiving_fumbles",
    "games_played",
    # finish / persistence proxies
    "rank_season_rb_half", "rank_season_rb_half_ppg",
    "rank_season_overall_4pt_half", "rank_season_overall_4pt_half_ppg",
    "rank_season_flex_half_ppg", "rank_season_rtflex_half_ppg",
    "consistency_4pt_half", "weighted_ppg_4pt_half",
    "lamar_ppg_12t_flx_half_4pt", "lamar_12t_flx_half_4pt",
    # current-year accolades (known, non-leaky)
    "all_pro_first_team", "all_pro_second_team", "pro_bowl", "mvp", "opoy",
    "oroy",
    # age (already on season row)
    "age",
]

# time-invariant bio fields from player_bio (+ rookie_year for derived exp)
BIO_FIELDS = [
    "draft_round", "draft_overall", "is_undrafted", "age_at_draft",
    "height", "weight", "ras_score", "forty", "vertical", "broad_jump",
    "cone", "shuttle", "bench", "rookie_year",
]
# derived bio features added in build: experience, bmi
DERIVED_BIO = ["experience", "bmi"]

# features that are essentially current-year finish (persistence in disguise);
# flagged so leaderboards can mark "adds nothing beyond persistence"
PERSISTENCE_PROXIES = {
    "rank_season_rb_half", "rank_season_rb_half_ppg",
    "rank_season_overall_4pt_half", "rank_season_overall_4pt_half_ppg",
    "rank_season_flex_half_ppg", "rank_season_rtflex_half_ppg",
    "weighted_ppg_4pt_half", "lamar_ppg_12t_flx_half_4pt",
    "lamar_12t_flx_half_4pt", PERSIST,
}


def all_features() -> list[str]:
    return SEASON_FEATURES + BIO_FIELDS[:-1] + DERIVED_BIO  # drop rookie_year raw


def load_dataset() -> pd.DataFrame:
    if not DATASET.exists():
        raise SystemExit(f"dataset missing: {DATASET}\n  run: python scripts/rb_ppg_dataset.py")
    return pd.read_parquet(DATASET)


# ---- OOS + normalization helpers (shared by sweep/model/strata) -------------
def pctnorm_within_year(df: pd.DataFrame, feats: list[str]) -> pd.DataFrame:
    """Percentile-normalize each feature within season so a stat means the same
    across eras. Returns a copy with feats replaced by their within-year pct."""
    out = df.copy()
    for f in feats:
        if f in out.columns:
            out[f] = out.groupby("year")[f].rank(pct=True)
    return out


def spearman(a: np.ndarray, b: np.ndarray) -> float:
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() < 20:
        return np.nan
    ar = pd.Series(a[m]).rank().to_numpy()
    br = pd.Series(b[m]).rank().to_numpy()
    if ar.std() < 1e-9 or br.std() < 1e-9:
        return np.nan
    return float(np.corrcoef(ar, br)[0, 1])


def pearson(a: np.ndarray, b: np.ndarray) -> float:
    m = np.isfinite(a) & np.isfinite(b)
    if m.sum() < 20:
        return np.nan
    if a[m].std() < 1e-9 or b[m].std() < 1e-9:
        return np.nan
    return float(np.corrcoef(a[m], b[m])[0, 1])
