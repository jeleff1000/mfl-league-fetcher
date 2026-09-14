#!/usr/bin/env python3
"""Targeted draft profile and position residual miner.

This is the product-facing discovery pass. It translates point-in-time draft
features into human-usable profiles, then scores those profiles and position
markets as residual value after draft-capital controls.
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

MODEL_VERSION = "draft-profile-residual-v0.2"
RECENCY_HALF_LIFE_YEARS = 3.0


PROFILE_METADATA: dict[str, dict[str, str]] = {
    "role_proof_missing": {
        "label": "Role proof missing",
        "blurb": "Little prior starting or scoring history. Treat as a price-sensitive bet, not a proven role.",
        "family": "usage_risk",
    },
    "recent_usable_starter": {
        "label": "Recent usable starter",
        "blurb": "Recent production and starts show a real role. Safer than pure name value at similar capital.",
        "family": "usage_proof",
    },
    "proven_high_end_producer": {
        "label": "Proven high-end producer",
        "blurb": "Strong prior value profile. Usually a premium-capital justification if price has not inflated.",
        "family": "production_proof",
    },
    "name_value_rebound": {
        "label": "Name-value rebound",
        "blurb": "Past peak value but weak recent production. Needs a discount before it becomes interesting.",
        "family": "rebound_risk",
    },
    "volume_without_value": {
        "label": "Volume without value",
        "blurb": "Played enough to matter but did not create much value. Watch for empty-role inflation.",
        "family": "role_quality",
    },
    "young_breakout_profile": {
        "label": "Young breakout profile",
        "blurb": "Young player with enough recent role evidence to justify upside pricing.",
        "family": "upside",
    },
    "aging_name_value": {
        "label": "Aging name value",
        "blurb": "Older player with weak recent proof. More dangerous when drafted on reputation.",
        "family": "age_risk",
    },
    "aging_producer": {
        "label": "Aging producer",
        "blurb": "Older player who still has real recent production. Risk is age, not role.",
        "family": "age_production",
    },
    "rookie_pedigree_bet": {
        "label": "Rookie pedigree bet",
        "blurb": "Rookie or near-rookie with NFL draft capital. Upside is pedigree more than league proof.",
        "family": "rookie_upside",
    },
    "athletic_upside_dart": {
        "label": "Athletic upside dart",
        "blurb": "High athletic profile without much proven fantasy role. Better late than expensive.",
        "family": "athletic_upside",
    },
    "undrafted_dart": {
        "label": "Undrafted dart",
        "blurb": "Low pedigree profile. Needs clear usage or a deep discount.",
        "family": "pedigree_risk",
    },
    "boom_week_history": {
        "label": "Boom-week history",
        "blurb": "Prior high-leverage or spike-week profile. Useful as upside context, not a floor signal.",
        "family": "volatility",
    },
    "negative_clutch_profile": {
        "label": "Low-leverage history",
        "blurb": "Prior clutch-equity profile was weak. Treat as a watch flag unless other usage is strong.",
        "family": "volatility",
    },
    "premium_draft_pedigree": {
        "label": "Premium draft pedigree",
        "blurb": "High NFL draft capital. Useful tiebreaker when role and price are similar.",
        "family": "pedigree",
    },
}


def _num(df: pd.DataFrame, column: str, default: float = 0.0) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype="float64")
    return pd.to_numeric(df[column], errors="coerce").fillna(default)


def _text(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype="object")
    return df[column].fillna(default).astype(str)


def _safe_weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    total_weight = float(weights.sum())
    if total_weight <= 0:
        return 0.0
    return float((values * weights).sum() / total_weight)


def _recency_weight(years: pd.Series, weights: pd.Series, reference_year: int) -> float:
    """Capital-weighted exponential recency weight for a candidate's evidence."""
    total_weight = float(weights.sum())
    if total_weight <= 0:
        return 1.0
    ages = (float(reference_year) - years.astype(float)).clip(lower=0)
    decay = 0.5 ** (ages / RECENCY_HALF_LIFE_YEARS)
    return float((decay * weights).sum() / total_weight)


def _add_profile(
    rows: list[dict[str, Any]],
    df: pd.DataFrame,
    mask: pd.Series,
    profile_key: str,
) -> None:
    meta = PROFILE_METADATA[profile_key]
    for idx in df.index[mask.fillna(False)]:
        rows.append(
            {
                "row_index": idx,
                "feature_type": f"profile.{profile_key}",
                "feature_value": meta["label"],
                "profile_key": profile_key,
                "profile_label": meta["label"],
                "profile_family": meta["family"],
                "profile_blurb": meta["blurb"],
                "baseline_mode": "within_position",
            }
        )


def build_profile_events(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Build actionable profile events from draft-time columns."""
    rows: list[dict[str, Any]] = []
    if df.empty:
        return rows

    position = _text(df, "position_group").str.upper()
    age = _num(df, "draft__draft_age")
    prior_fp = _num(df, "player_prior__avg_fantasy_points")
    prev_fp = _num(df, "player_prev__fantasy_points")
    prior_lamar = _num(df, "player_prior__avg_player_lamar")
    max_lamar = _num(df, "player_prior__max_player_lamar")
    prior_starts = _num(df, "player_prior__avg_games_started")
    prev_starts = _num(df, "player_prev__games_started")
    prior_optimal = _num(df, "player_prior__optimal_player_count")
    prev_optimal = _num(df, "player_prev__optimal_player_count")
    prior_clutch = _num(df, "player_prior__avg_clutch_equity")
    prev_clutch = _num(df, "player_prev__clutch_equity")
    draft_round = _num(df, "bio__draft_round", 99)
    draft_overall = _num(df, "bio__draft_overall", 999)
    is_undrafted = _num(df, "bio__is_undrafted")
    rookie_year = _num(df, "bio__rookie_year")
    year = _num(df, "year")
    ras = _num(df, "bio__ras_score")

    recent_points = pd.concat([prior_fp, prev_fp], axis=1).max(axis=1)
    recent_starts = pd.concat([prior_starts, prev_starts], axis=1).max(axis=1)
    recent_optimal = pd.concat([prior_optimal, prev_optimal], axis=1).max(axis=1)
    recent_clutch = pd.concat([prior_clutch, prev_clutch], axis=1).max(axis=1)
    experience = year - rookie_year
    skill_position = position.isin(["QB", "RB", "WR", "TE"])

    _add_profile(
        rows,
        df,
        skill_position & (recent_points <= 95) & (recent_starts <= 4) & (recent_optimal <= 4),
        "role_proof_missing",
    )
    _add_profile(
        rows,
        df,
        skill_position & ((prev_fp >= 130) | (prev_starts >= 7) | (prev_optimal >= 8)),
        "recent_usable_starter",
    )
    _add_profile(
        rows,
        df,
        skill_position & ((prior_lamar >= 70) | (max_lamar >= 120) | (prior_fp >= 180)),
        "proven_high_end_producer",
    )
    _add_profile(
        rows,
        df,
        skill_position & (max_lamar >= 70) & (prev_fp <= 95) & (prev_starts <= 5),
        "name_value_rebound",
    )
    _add_profile(
        rows,
        df,
        skill_position & (recent_starts >= 7) & (prior_lamar <= 20) & (recent_points <= 130),
        "volume_without_value",
    )
    _add_profile(
        rows,
        df,
        skill_position & (age > 0) & (age <= 24) & ((prev_fp >= 95) | (prev_starts >= 5) | (prev_optimal >= 4)),
        "young_breakout_profile",
    )
    _add_profile(
        rows,
        df,
        skill_position & (age >= 29) & (recent_points <= 110) & (prior_lamar <= 30),
        "aging_name_value",
    )
    _add_profile(
        rows,
        df,
        skill_position & (age >= 29) & ((prev_fp >= 150) | (prior_lamar >= 50) | (prev_starts >= 8)),
        "aging_producer",
    )
    _add_profile(
        rows,
        df,
        skill_position & (experience <= 1) & ((draft_round <= 3) | (draft_overall <= 100)),
        "rookie_pedigree_bet",
    )
    _add_profile(
        rows,
        df,
        skill_position & (ras >= 8) & (recent_starts <= 4) & (recent_points <= 120),
        "athletic_upside_dart",
    )
    _add_profile(
        rows,
        df,
        skill_position & (is_undrafted >= 1) & (recent_starts <= 5),
        "undrafted_dart",
    )
    _add_profile(
        rows,
        df,
        skill_position & (recent_clutch >= 0.5),
        "boom_week_history",
    )
    _add_profile(
        rows,
        df,
        skill_position & (recent_clutch <= -0.35),
        "negative_clutch_profile",
    )
    _add_profile(
        rows,
        df,
        skill_position & ((draft_round == 1) | (draft_overall <= 32)),
        "premium_draft_pedigree",
    )

    return rows


def build_position_market_events(df: pd.DataFrame) -> list[dict[str, Any]]:
    """Build position x capital tier market events."""
    rows: list[dict[str, Any]] = []
    if df.empty:
        return rows
    position = _text(df, "position_group").str.upper()
    bucket = _text(df, "capital_bucket")
    for idx in df.index[position.isin(["QB", "RB", "WR", "TE"])]:
        pos = position.loc[idx]
        tier = bucket.loc[idx]
        rows.append(
            {
                "row_index": idx,
                "feature_type": "position_market",
                "feature_value": f"{pos}|{tier}",
                "profile_key": f"{pos.lower()}_{tier}",
                "profile_label": f"{pos} market in {tier.replace('_', ' ')}",
                "profile_family": "position_market",
                "profile_blurb": f"{pos} results versus similarly priced picks.",
                "baseline_mode": "capital_only",
            }
        )
    return rows


def _baseline_frame(base: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    rows = []
    for group_key, group in base.groupby(keys, dropna=False):
        weights = group["capital_weight"].astype(float)
        residual = group["value_residual"].astype(float)
        mean = _safe_weighted_mean(residual, weights)
        variance = max(_safe_weighted_mean((residual - mean) ** 2, weights), 0.01)
        if not isinstance(group_key, tuple):
            group_key = (group_key,)
        rows.append((*group_key, mean, variance))
    return pd.DataFrame(rows, columns=[*keys, "expected_residual", "residual_variance"])


def score_residual_events(
    df: pd.DataFrame,
    events: list[dict[str, Any]],
    *,
    baseline_keys: list[str],
    scope_label: str,
    min_picks: int = 100,
    min_years: int = 12,
    min_abs_z: float = 2.5,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Score event residuals against a specified baseline definition."""
    if df.empty or not events:
        return []

    base = df.reset_index(names="row_index").copy()
    base["manager_lamar"] = _num(base, "manager_lamar")
    base["expected_lamar"] = _num(base, "expected_lamar")
    base["capital_weight"] = _num(base, "capital_weight", 1).clip(lower=1)
    base["value_residual"] = base["manager_lamar"] - base["expected_lamar"]
    base = base.dropna(
        subset=["db_name", "year", "capital_bucket", "position_group", "value_residual", "capital_weight"]
    )
    if base.empty:
        return []

    baseline = _baseline_frame(base, baseline_keys)
    scored_base = base.merge(baseline, on=baseline_keys, how="inner")
    events_df = pd.DataFrame(events)
    scored = events_df.merge(
        scored_base[
            [
                "row_index",
                "db_name",
                "year",
                "position_group",
                "capital_bucket",
                "capital_weight",
                "value_residual",
                "expected_residual",
                "residual_variance",
            ]
        ],
        on="row_index",
        how="inner",
    )
    if scored.empty:
        return []

    year_rows = []
    for keys, group in scored.groupby(
        ["feature_type", "feature_value", "profile_label", "profile_family", "profile_blurb", "db_name", "year"],
        dropna=False,
    ):
        weights = group["capital_weight"].astype(float)
        observed = _safe_weighted_mean(group["value_residual"].astype(float), weights)
        expected = _safe_weighted_mean(group["expected_residual"].astype(float), weights)
        variance = float(
            (weights * weights * group["residual_variance"].astype(float)).sum() / max(float(weights.sum()) ** 2, 1e-9)
        )
        year_rows.append(
            (*keys, int(len(group)), float(weights.sum()), observed, expected, observed - expected, variance)
        )

    candidate_year = pd.DataFrame(
        year_rows,
        columns=[
            "feature_type",
            "feature_value",
            "profile_label",
            "profile_family",
            "profile_blurb",
            "db_name",
            "year",
            "picks",
            "observed_capital",
            "observed_residual",
            "expected_residual",
            "excess_residual",
            "variance",
        ],
    )
    if candidate_year.empty:
        return []

    scope_db = scope_label
    reference_year = int(candidate_year["year"].max())
    rows = []
    for keys, group in candidate_year.groupby(
        ["feature_type", "feature_value", "profile_label", "profile_family", "profile_blurb"],
        dropna=False,
    ):
        picks = int(group["picks"].sum())
        years_seen = int(group[["db_name", "year"]].drop_duplicates().shape[0])
        if picks < min_picks or years_seen < min_years:
            continue
        weights = group["observed_capital"].astype(float)
        observed = _safe_weighted_mean(group["observed_residual"].astype(float), weights)
        expected = _safe_weighted_mean(group["expected_residual"].astype(float), weights)
        excess = observed - expected
        variance = float(
            (group["variance"].astype(float) * weights * weights).sum() / max(float(weights.sum()) ** 2, 1e-9)
        )
        z_score = excess / math.sqrt(max(variance, 0.01))
        if abs(z_score) < min_abs_z:
            continue
        positive_years = int((group["excess_residual"] > 0).sum())
        negative_years = int((group["excess_residual"] < 0).sum())
        repeatability = (positive_years if excess >= 0 else negative_years) / max(years_seen, 1)
        recency_weight = _recency_weight(group["year"], weights, reference_year)
        rows.append(
            {
                "db_name": scope_db,
                "scope_type": "league_inefficiency",
                "scope_key": scope_db,
                "scope_label": scope_db,
                "feature_type": str(keys[0]),
                "feature_value": str(keys[1]),
                "profile_label": str(keys[2]),
                "profile_family": str(keys[3]),
                "profile_blurb": str(keys[4]),
                "picks": picks,
                "years_seen": years_seen,
                "earliest_year": int(group["year"].min()),
                "latest_year": int(group["year"].max()),
                "recency_weight": round(float(recency_weight), 4),
                "positive_years": positive_years,
                "negative_years": negative_years,
                "repeatability": round(float(repeatability), 4),
                "observed_capital": round(float(weights.sum()), 3),
                "observed_residual": round(float(observed), 4),
                "expected_residual": round(float(expected), 4),
                "excess_residual": round(float(excess), 4),
                "observed_pick_score": 0.0,
                "expected_pick_score": 0.0,
                "pick_score_delta": 0.0,
                "value_z_score": round(float(z_score), 3),
                "confidence": "targeted",
                "evidence_level": "profile_candidate",
                "model_version": MODEL_VERSION,
            }
        )
    return sorted(rows, key=lambda row: (abs(row["value_z_score"]), abs(row["excess_residual"])), reverse=True)[:limit]


def run_profile_residual_miner_df(
    df: pd.DataFrame,
    *,
    scope_label: str = "fleet",
    min_picks: int = 100,
    min_years: int = 12,
    min_abs_z: float = 2.5,
    limit: int = 100,
) -> dict[str, list[dict[str, Any]]]:
    """Run targeted residual discovery on an already-built cross-table matrix."""
    position_events = build_position_market_events(df)
    profile_events = build_profile_events(df)
    return {
        "position_market": score_residual_events(
            df,
            position_events,
            baseline_keys=["db_name", "year", "capital_bucket"],
            scope_label=scope_label,
            min_picks=min_picks,
            min_years=min_years,
            min_abs_z=min_abs_z,
            limit=limit,
        ),
        "draft_profiles": score_residual_events(
            df,
            profile_events,
            baseline_keys=["db_name", "year", "position_group", "capital_bucket"],
            scope_label=scope_label,
            min_picks=min_picks,
            min_years=min_years,
            min_abs_z=min_abs_z,
            limit=limit,
        ),
    }
