#!/usr/bin/env python3
"""Stratified draft population ML census.

Reads centralized Fly league tables, labels league-years by format, trains a
small holdout model, and writes a compact JSON report of population-level draft
signals. The report is intended for optimizer research, not direct data writes.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
import sys
from time import sleep
from typing import Any

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import GroupShuffleSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"

for module_path in (REPO_ROOT, SCRIPTS_ROOT):
    path_str = str(module_path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.core.readers.fly_reader import FlyReader


IDP_POSITIONS = {"LB", "DL", "DB", "IDP", "DE", "DT", "CB", "S", "SS", "FS", "ILB", "OLB"}
STREAMING_POSITIONS = {"K", "DEF"}
QUERY_RETRY_SECONDS = (8, 20, 45)


def load_env() -> None:
    for name in [".env", ".env.local", "frontend/.env", "frontend/.env.local"]:
        path = REPO_ROOT / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def query_df_with_retries(reader: FlyReader, sql: str, *, label: str) -> pd.DataFrame:
    for attempt, wait_seconds in enumerate((*QUERY_RETRY_SECONDS, 0), start=1):
        try:
            return reader.query_df(sql, database="___leagues")
        except Exception as exc:
            if wait_seconds == 0:
                raise
            print(
                f"[draft-ml] {label} failed on attempt {attempt}: {exc}; retrying in {wait_seconds}s",
                file=sys.stderr,
                flush=True,
            )
            sleep(wait_seconds)
    raise RuntimeError(f"unreachable retry state for {label}")


def fetch_available_years(reader: FlyReader, *, min_year: int | None) -> list[int]:
    year_filter = f"AND year >= {int(min_year)}" if min_year else ""
    sql = f"""
SELECT DISTINCT CAST(year AS INTEGER) AS year
FROM public.draft
WHERE db_name IS NOT NULL AND year IS NOT NULL {year_filter}
ORDER BY year
"""
    years = query_df_with_retries(reader, sql, label="fetch years")
    return [int(year) for year in years["year"].dropna().tolist()]


def build_pick_dataset_sql(
    *,
    min_year: int | None,
    exact_year: int | None,
    max_rows: int | None,
) -> str:
    draft_year_filter = ""
    settings_year_filter = ""
    main_year_filter = ""
    if exact_year is not None:
        draft_year_filter = f"AND year = {int(exact_year)}"
        settings_year_filter = f"WHERE year = {int(exact_year)}"
        main_year_filter = f"AND d.year = {int(exact_year)}"
    elif min_year is not None:
        draft_year_filter = f"AND year >= {int(min_year)}"
        settings_year_filter = f"WHERE year >= {int(min_year)}"
        main_year_filter = f"AND d.year >= {int(min_year)}"
    limit = f"\nLIMIT {int(max_rows)}" if max_rows else ""
    return f"""
WITH year_format AS (
    SELECT
        db_name,
        year,
        COUNT(*) AS picks_in_year,
        COUNT(DISTINCT manager) AS managers_in_year,
        MAX(COALESCE(round, 0)) AS max_round,
        MAX(COALESCE(pick, 0)) AS max_pick,
        SUM(COALESCE(cost, 0)) AS total_cost,
        SUM(CASE WHEN COALESCE(cost, 0) > 0 THEN 1 ELSE 0 END) AS cost_picks,
        SUM(CASE
            WHEN COALESCE(is_keeper, 0) = 1 OR LOWER(COALESCE(draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END) AS keeper_picks,
        STRING_AGG(DISTINCT LOWER(COALESCE(draft_type, '')), ',') AS draft_type_tokens,
        CASE
            WHEN STRING_AGG(DISTINCT LOWER(COALESCE(draft_type, '')), ',') SIMILAR TO '.*(auction|live|offline).*'
              OR SUM(CASE WHEN COALESCE(cost, 0) > 0 THEN 1 ELSE 0 END) >= COUNT(*) * 0.25
            THEN 'auction'
            ELSE 'snake'
        END AS draft_kind
    FROM public.draft
    WHERE db_name IS NOT NULL AND year IS NOT NULL {draft_year_filter}
    GROUP BY db_name, year
),
keeper_cfg AS (
    SELECT
        db_name,
        MAX(CASE WHEN COALESCE(enabled, 0) = 1 THEN 1 ELSE 0 END) AS keeper_config_enabled,
        MAX(COALESCE(max_keepers, 0)) AS keeper_config_max_keepers,
        MAX(COALESCE(keeper_payroll_cap, 0)) AS keeper_payroll_cap
    FROM public.keeper_config
    GROUP BY db_name
),
settings AS (
    SELECT
        db_name,
        year,
        ANY_VALUE(platform) AS settings_platform,
        MAX(COALESCE(num_teams, 0)) AS num_teams,
        MAX(COALESCE(uses_median, 0)) AS uses_median,
        MAX(COALESCE(is_dynasty, 0)) AS is_dynasty,
        ANY_VALUE(LOWER(COALESCE(league_type, ''))) AS league_type,
        MAX(COALESCE(max_keepers, 0)) AS max_keepers,
        MAX(COALESCE(draft_rounds, 0)) AS draft_rounds,
        MAX(COALESCE(scoring_rec, 0)) AS scoring_rec,
        MAX(COALESCE(scoring_pass_td, 4)) AS scoring_pass_td,
        MAX(COALESCE(roster_QB, 0)) AS roster_qb,
        MAX(COALESCE(roster_RB, 0)) AS roster_rb,
        MAX(COALESCE(roster_WR, 0)) AS roster_wr,
        MAX(COALESCE(roster_TE, 0)) AS roster_te,
        MAX(COALESCE(roster_K, 0)) AS roster_k,
        MAX(COALESCE(roster_DEF, 0)) AS roster_def,
        MAX(COALESCE(roster_FLX, 0)) AS roster_flex,
        MAX(COALESCE(roster_SUPER_FLEX, 0)) AS roster_super_flex,
        MAX(COALESCE(roster_REC_FLEX, 0)) AS roster_rec_flex,
        MAX(COALESCE(roster_LB, 0)) AS roster_lb,
        MAX(COALESCE(roster_DL, 0)) AS roster_dl,
        MAX(COALESCE(roster_DB, 0)) AS roster_db,
        MAX(COALESCE(roster_IDP, 0)) AS roster_idp,
        MAX(COALESCE(roster_DB_LB, 0)) AS roster_db_lb,
        MAX(COALESCE(roster_DL_LB, 0)) AS roster_dl_lb,
        MAX(COALESCE(roster_BN, 0)) AS roster_bn,
        MAX(COALESCE(sleeper_taxi_slots, 0)) AS taxi_slots,
        MAX(COALESCE(sleeper_pick_trading, 0)) AS pick_trading
    FROM public.league_settings
    {settings_year_filter}
    GROUP BY db_name, year
)
SELECT
    d.db_name,
    d.year,
    COALESCE(d.platform, s.settings_platform) AS platform,
    yf.draft_kind,
    yf.picks_in_year,
    yf.managers_in_year,
    COALESCE(NULLIF(s.num_teams, 0), yf.managers_in_year) AS num_teams,
    COALESCE(NULLIF(s.draft_rounds, 0), NULLIF(yf.max_round, 0), 0) AS draft_rounds,
    yf.cost_picks,
    yf.keeper_picks,
    yf.total_cost,
    yf.cost_picks * 1.0 / NULLIF(yf.picks_in_year, 0) AS cost_pick_share,
    d.manager,
    d.franchise_id,
    UPPER(COALESCE(NULLIF(d.position, ''), 'UNK')) AS position,
    COALESCE(d.cost, 0) AS cost,
    d.pick,
    d.round,
    d.draft_slot,
    COALESCE(d.is_keeper, 0) AS is_keeper_pick,
    CASE WHEN LOWER(COALESCE(d.draft_category, '')) = 'keeper' THEN 1 ELSE 0 END AS draft_category_keeper,
    d.draft_type,
    d.NFL_player_id,
    COALESCE(d.manager_lamar, d.player_lamar) AS manager_lamar,
    d.player_lamar,
    d.pick_score,
    d.draft_grade,
    d.total_fantasy_points,
    d.drafted_as_starter,
    d.position_draft_rank,
    d.starter_slots_available,
    d.draft_age_zscore,
    d.position_activation_rate,
    d.position_failure_rate,
    d.bench_insurance_discount,
    COALESCE(kc.keeper_config_enabled, 0) AS keeper_config_enabled,
    COALESCE(kc.keeper_config_max_keepers, 0) AS keeper_config_max_keepers,
    COALESCE(kc.keeper_payroll_cap, 0) AS keeper_payroll_cap,
    COALESCE(s.uses_median, 0) AS uses_median,
    COALESCE(s.is_dynasty, 0) AS is_dynasty_setting,
    COALESCE(s.league_type, '') AS league_type,
    COALESCE(s.max_keepers, 0) AS max_keepers,
    COALESCE(s.scoring_rec, 0) AS scoring_rec,
    COALESCE(s.scoring_pass_td, 4) AS scoring_pass_td,
    COALESCE(s.roster_qb, 0) AS roster_qb,
    COALESCE(s.roster_rb, 0) AS roster_rb,
    COALESCE(s.roster_wr, 0) AS roster_wr,
    COALESCE(s.roster_te, 0) AS roster_te,
    COALESCE(s.roster_k, 0) AS roster_k,
    COALESCE(s.roster_def, 0) AS roster_def,
    COALESCE(s.roster_flex, 0) AS roster_flex,
    COALESCE(s.roster_super_flex, 0) AS roster_super_flex,
    COALESCE(s.roster_rec_flex, 0) AS roster_rec_flex,
    COALESCE(s.roster_lb, 0) AS roster_lb,
    COALESCE(s.roster_dl, 0) AS roster_dl,
    COALESCE(s.roster_db, 0) AS roster_db,
    COALESCE(s.roster_idp, 0) AS roster_idp,
    COALESCE(s.roster_db_lb, 0) AS roster_db_lb,
    COALESCE(s.roster_dl_lb, 0) AS roster_dl_lb,
    COALESCE(s.roster_bn, 0) AS roster_bn,
    COALESCE(s.taxi_slots, 0) AS taxi_slots,
    COALESCE(s.pick_trading, 0) AS pick_trading
FROM public.draft d
JOIN year_format yf ON d.db_name = yf.db_name AND d.year = yf.year
LEFT JOIN settings s ON d.db_name = s.db_name AND d.year = s.year
LEFT JOIN keeper_cfg kc ON d.db_name = kc.db_name
WHERE d.db_name IS NOT NULL
  AND d.year IS NOT NULL
  AND d.manager IS NOT NULL
  AND d.NFL_player_id IS NOT NULL
  AND COALESCE(d.manager_lamar, d.player_lamar) IS NOT NULL
  {main_year_filter}
ORDER BY d.db_name, d.year, COALESCE(d.pick, d.round, 9999)
{limit}
"""


def fetch_pick_dataset(
    reader: FlyReader,
    *,
    min_year: int | None,
    max_rows: int | None,
    chunk_years: bool,
) -> pd.DataFrame:
    if not chunk_years:
        sql = build_pick_dataset_sql(min_year=min_year, exact_year=None, max_rows=max_rows)
        return query_df_with_retries(reader, sql, label="fetch all picks")

    frames: list[pd.DataFrame] = []
    years = fetch_available_years(reader, min_year=min_year)
    for year in years:
        print(f"[draft-ml] fetching draft picks for {year}", file=sys.stderr, flush=True)
        sql = build_pick_dataset_sql(min_year=None, exact_year=year, max_rows=None)
        frame = query_df_with_retries(reader, sql, label=f"fetch picks {year}")
        if not frame.empty:
            frames.append(frame)
        if max_rows and sum(len(item) for item in frames) >= max_rows:
            break
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.sort_values(["db_name", "year", "pick", "round"], kind="stable")
    if max_rows:
        out = out.head(max_rows)
    return out.reset_index(drop=True)


def clean_dataset(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    numeric_cols = [
        "year",
        "picks_in_year",
        "managers_in_year",
        "num_teams",
        "draft_rounds",
        "cost_picks",
        "keeper_picks",
        "total_cost",
        "cost_pick_share",
        "cost",
        "pick",
        "round",
        "draft_slot",
        "is_keeper_pick",
        "draft_category_keeper",
        "manager_lamar",
        "player_lamar",
        "pick_score",
        "total_fantasy_points",
        "drafted_as_starter",
        "position_draft_rank",
        "starter_slots_available",
        "draft_age_zscore",
        "position_activation_rate",
        "position_failure_rate",
        "bench_insurance_discount",
        "keeper_config_enabled",
        "keeper_config_max_keepers",
        "keeper_payroll_cap",
        "uses_median",
        "is_dynasty_setting",
        "max_keepers",
        "scoring_rec",
        "scoring_pass_td",
        "roster_qb",
        "roster_rb",
        "roster_wr",
        "roster_te",
        "roster_k",
        "roster_def",
        "roster_flex",
        "roster_super_flex",
        "roster_rec_flex",
        "roster_lb",
        "roster_dl",
        "roster_db",
        "roster_idp",
        "roster_db_lb",
        "roster_dl_lb",
        "roster_bn",
        "taxi_slots",
        "pick_trading",
    ]
    for col in numeric_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    out["position"] = out["position"].fillna("UNK").astype(str).str.upper().replace({"D/ST": "DEF", "DST": "DEF"})
    out["platform"] = out["platform"].fillna("unknown").astype(str).str.lower()
    out["draft_kind"] = out["draft_kind"].fillna("snake").astype(str)
    out["league_type"] = out["league_type"].fillna("").astype(str).str.lower()
    out["is_keeper_pick_any"] = (
        (out["is_keeper_pick"].fillna(0) > 0) | (out["draft_category_keeper"].fillna(0) > 0)
    ).astype(int)
    out["league_has_keepers"] = (
        (out["keeper_picks"].fillna(0) > 0)
        | (out["keeper_config_enabled"].fillna(0) > 0)
        | (out["keeper_config_max_keepers"].fillna(0) > 0)
        | (out["max_keepers"].fillna(0) > 0)
    ).astype(int)
    out["is_dynasty"] = (
        (out["is_dynasty_setting"].fillna(0) > 0)
        | out["league_type"].str.contains("dynasty", na=False)
        | (out["taxi_slots"].fillna(0) > 0)
        | (out["pick_trading"].fillna(0) > 0)
    ).astype(int)
    out["is_keeper_or_dynasty"] = ((out["league_has_keepers"] > 0) | (out["is_dynasty"] > 0)).astype(int)
    out["is_superflex"] = ((out["roster_super_flex"].fillna(0) > 0) | (out["roster_qb"].fillna(0) >= 2)).astype(int)
    out["is_idp"] = (
        out[["roster_lb", "roster_dl", "roster_db", "roster_idp", "roster_db_lb", "roster_dl_lb"]].fillna(0).sum(axis=1)
        > 0
    ).astype(int)
    out["uses_median"] = (out["uses_median"].fillna(0) > 0).astype(int)
    out["ppr_bucket"] = pd.cut(
        out["scoring_rec"].fillna(0),
        bins=[-0.1, 0.01, 0.75, 1.01, 99],
        labels=["standard", "half_ppr", "full_ppr", "premium_ppr"],
    ).astype(str)
    out["pass_td_bucket"] = np.where(out["scoring_pass_td"].fillna(4) >= 6, "pass_td_6", "pass_td_4_5")
    out["position_class"] = np.select(
        [
            out["position"].isin(["QB"]),
            out["position"].isin(["RB", "WR", "TE"]),
            out["position"].isin(STREAMING_POSITIONS),
            out["position"].isin(IDP_POSITIONS),
        ],
        ["qb", "skill", "streaming", "idp"],
        default="other",
    )
    out["capital_bucket"] = out.apply(capital_bucket, axis=1)
    out["capital_norm"] = out.apply(capital_norm, axis=1)
    out["group_key"] = out["db_name"].astype(str) + ":" + out["year"].astype("Int64").astype(str)
    out["good_pick"] = (out["manager_lamar"] > 0).astype(int)
    return out


def capital_bucket(row: pd.Series) -> str:
    if row.get("draft_kind") == "auction":
        cost = float(row.get("cost") or 0)
        if cost >= 50:
            return "auction_50_plus"
        if cost >= 30:
            return "auction_30_49"
        if cost >= 16:
            return "auction_16_29"
        if cost >= 6:
            return "auction_6_15"
        return "auction_1_5"
    round_no = float(row.get("round") or 99)
    if round_no <= 2:
        return "snake_r1_2"
    if round_no <= 5:
        return "snake_r3_5"
    if round_no <= 9:
        return "snake_r6_9"
    return "snake_r10_plus"


def capital_norm(row: pd.Series) -> float:
    if row.get("draft_kind") == "auction":
        total_cost = float(row.get("total_cost") or 0)
        managers = max(float(row.get("managers_in_year") or row.get("num_teams") or 1), 1.0)
        denom = total_cost / managers if total_cost > 0 else 200.0
        return float(row.get("cost") or 0) / denom
    max_pick = max(float(row.get("picks_in_year") or 1), 1.0)
    pick = float(row.get("pick") or row.get("round") or max_pick)
    return 1.0 - ((pick - 1.0) / max_pick)


def model_report(
    df: pd.DataFrame,
    *,
    random_state: int,
    max_importance_rows: int,
    max_model_rows: int,
) -> dict[str, Any]:
    model_df = df[(df["manager_lamar"].notna()) & (df["position"] != "UNK") & (df["is_keeper_pick_any"] == 0)].copy()
    source_rows = len(model_df)
    if source_rows < 1000 or model_df["group_key"].nunique() < 5:
        return {"skipped": True, "reason": "not enough rows/groups", "rows": int(len(model_df))}
    if max_model_rows and source_rows > max_model_rows:
        model_df = model_df.sample(n=max_model_rows, random_state=random_state)

    categorical = [
        "draft_kind",
        "platform",
        "position",
        "position_class",
        "capital_bucket",
        "ppr_bucket",
        "pass_td_bucket",
    ]
    numeric = [
        "num_teams",
        "draft_rounds",
        "capital_norm",
        "cost",
        "pick",
        "round",
        "position_draft_rank",
        "starter_slots_available",
        "draft_age_zscore",
        "league_has_keepers",
        "is_dynasty",
        "is_keeper_or_dynasty",
        "is_superflex",
        "is_idp",
        "uses_median",
        "roster_qb",
        "roster_super_flex",
        "roster_lb",
        "roster_dl",
        "roster_db",
        "roster_bn",
    ]
    features = categorical + numeric
    usable = model_df[features + ["manager_lamar", "group_key"]].copy()
    groups = usable["group_key"].astype(str)
    splitter = GroupShuffleSplit(n_splits=1, test_size=0.22, random_state=random_state)
    train_idx, test_idx = next(splitter.split(usable, usable["manager_lamar"], groups))
    train = usable.iloc[train_idx]
    test = usable.iloc[test_idx]

    worker_count = max(1, min(os.cpu_count() or 1, 4))
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore", min_frequency=25), categorical),
            ("num", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric),
        ]
    )
    regressor = RandomForestRegressor(
        n_estimators=180,
        max_depth=13,
        min_samples_leaf=25,
        random_state=random_state,
        n_jobs=worker_count,
    )
    pipeline = Pipeline([("pre", preprocessor), ("model", regressor)])
    pipeline.fit(train[features], train["manager_lamar"])
    pred = pipeline.predict(test[features])

    rng = np.random.default_rng(random_state)
    importance_test = test
    if len(importance_test) > max_importance_rows:
        importance_test = importance_test.iloc[
            rng.choice(len(importance_test), size=max_importance_rows, replace=False)
        ]
    perm = permutation_importance(
        pipeline,
        importance_test[features],
        importance_test["manager_lamar"],
        n_repeats=3,
        random_state=random_state,
        n_jobs=worker_count,
        scoring="neg_mean_absolute_error",
    )

    importances = sorted(
        [
            {
                "feature": feature,
                "mae_increase": round(float(mean), 4),
                "std": round(float(std), 4),
            }
            for feature, mean, std in zip(features, perm.importances_mean, perm.importances_std, strict=True)
        ],
        key=lambda row: row["mae_increase"],
        reverse=True,
    )

    return {
        "skipped": False,
        "source_rows": int(source_rows),
        "rows": int(len(model_df)),
        "train_rows": int(len(train)),
        "test_rows": int(len(test)),
        "train_groups": int(train["group_key"].nunique()),
        "test_groups": int(test["group_key"].nunique()),
        "target_mean": round(float(model_df["manager_lamar"].mean()), 3),
        "target_median": round(float(model_df["manager_lamar"].median()), 3),
        "holdout_mae": round(float(mean_absolute_error(test["manager_lamar"], pred)), 3),
        "holdout_r2": round(float(r2_score(test["manager_lamar"], pred)), 3),
        "top_permutation_importance": importances[:20],
    }


def aggregate_report(df: pd.DataFrame, *, min_group_rows: int) -> dict[str, Any]:
    draft_rows = df[df["is_keeper_pick_any"] == 0].copy()
    draft_rows["baseline_key"] = (
        draft_rows["draft_kind"].astype(str)
        + "|"
        + draft_rows["capital_bucket"].astype(str)
        + "|"
        + draft_rows["position"].astype(str)
    )
    baseline = draft_rows.groupby("baseline_key", observed=True)["manager_lamar"].mean()
    draft_rows["capital_position_baseline"] = draft_rows["baseline_key"].map(baseline)
    draft_rows["lamar_residual"] = draft_rows["manager_lamar"] - draft_rows["capital_position_baseline"]

    return {
        "population": population_summary(df),
        "format_summary": grouped_summary(
            draft_rows,
            ["draft_kind", "is_keeper_or_dynasty", "is_superflex", "is_idp"],
            min_rows=min_group_rows,
        ),
        "position_by_format": grouped_summary(
            draft_rows,
            ["draft_kind", "position", "is_keeper_or_dynasty", "is_superflex", "is_idp"],
            min_rows=max(min_group_rows, 40),
            sort_by="residual_mean",
            limit=80,
        ),
        "capital_by_position": grouped_summary(
            draft_rows,
            ["draft_kind", "capital_bucket", "position"],
            min_rows=max(min_group_rows, 50),
            sort_by="manager_lamar_mean",
            limit=80,
        ),
        "format_residual_edges": residual_edges(draft_rows, min_rows=max(min_group_rows, 40)),
        "keeper_pick_summary": grouped_summary(
            df[df["is_keeper_pick_any"] == 1],
            ["draft_kind", "position"],
            min_rows=15,
            sort_by="manager_lamar_mean",
            limit=40,
        ),
    }


def population_summary(df: pd.DataFrame) -> dict[str, Any]:
    league_years = df.drop_duplicates(["db_name", "year"])
    return {
        "rows": int(len(df)),
        "draft_rows": int((df["is_keeper_pick_any"] == 0).sum()),
        "keeper_rows": int((df["is_keeper_pick_any"] == 1).sum()),
        "leagues": int(df["db_name"].nunique()),
        "league_years": int(league_years[["db_name", "year"]].drop_duplicates().shape[0]),
        "years": [int(df["year"].min()), int(df["year"].max())],
        "draft_kind_league_years": counts_dict(league_years["draft_kind"]),
        "keeper_or_dynasty_league_years": int(league_years["is_keeper_or_dynasty"].sum()),
        "superflex_league_years": int(league_years["is_superflex"].sum()),
        "idp_league_years": int(league_years["is_idp"].sum()),
        "median_league_years": int(league_years["uses_median"].sum()),
    }


def grouped_summary(
    df: pd.DataFrame,
    keys: list[str],
    *,
    min_rows: int,
    sort_by: str = "n",
    limit: int | None = None,
) -> list[dict[str, Any]]:
    if df.empty:
        return []
    grouped = (
        df.groupby(keys, dropna=False, observed=True)
        .agg(
            n=("manager_lamar", "size"),
            leagues=("db_name", "nunique"),
            league_years=("group_key", "nunique"),
            manager_lamar_mean=("manager_lamar", "mean"),
            manager_lamar_median=("manager_lamar", "median"),
            residual_mean=("lamar_residual", "mean") if "lamar_residual" in df.columns else ("manager_lamar", "mean"),
            hit_rate=("good_pick", "mean"),
            pick_score_mean=("pick_score", "mean"),
            downside_p10=("manager_lamar", lambda s: s.quantile(0.10)),
            upside_p90=("manager_lamar", lambda s: s.quantile(0.90)),
        )
        .reset_index()
    )
    grouped = grouped[grouped["n"] >= min_rows]
    if grouped.empty:
        return []
    sort_col = sort_by if sort_by in grouped.columns else "n"
    grouped = grouped.sort_values(sort_col, ascending=sort_col == "residual_mean")
    if sort_by == "residual_mean":
        grouped = grouped.reindex(grouped["residual_mean"].abs().sort_values(ascending=False).index)
    else:
        grouped = grouped.sort_values(sort_col, ascending=False)
    if limit:
        grouped = grouped.head(limit)
    return records(grouped)


def residual_edges(df: pd.DataFrame, *, min_rows: int) -> list[dict[str, Any]]:
    edge_frames: list[pd.DataFrame] = []
    specs = [
        ("keeper_or_dynasty", "is_keeper_or_dynasty"),
        ("superflex", "is_superflex"),
        ("idp", "is_idp"),
        ("median", "uses_median"),
    ]
    for label, col in specs:
        work = grouped_summary(df[df[col] == 1], ["draft_kind", "position"], min_rows=min_rows, sort_by="residual_mean")
        for row in work:
            row["edge"] = label
        edge_frames.extend(pd.DataFrame([row]) for row in work)
    if not edge_frames:
        return []
    out = pd.concat(edge_frames, ignore_index=True)
    out = out.reindex(out["residual_mean"].abs().sort_values(ascending=False).index).head(60)
    return records(out)


def counts_dict(series: pd.Series) -> dict[str, int]:
    return {str(k): int(v) for k, v in series.value_counts(dropna=False).sort_index().items()}


def records(df: pd.DataFrame) -> list[dict[str, Any]]:
    cleaned = df.replace({np.nan: None})
    out: list[dict[str, Any]] = []
    for row in cleaned.to_dict("records"):
        item: dict[str, Any] = {}
        for key, value in row.items():
            if isinstance(value, (np.integer,)):
                item[key] = int(value)
            elif isinstance(value, (float, np.floating)):
                item[key] = round(float(value), 4)
            else:
                item[key] = value
        out.append(item)
    return out


def write_report(report: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "draft_population_ml_summary.json"
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Run stratified draft population ML census from Fly.")
    parser.add_argument("--min-year", type=int, default=2017)
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--min-group-rows", type=int, default=75)
    parser.add_argument("--max-importance-rows", type=int, default=7000)
    parser.add_argument("--max-model-rows", type=int, default=180000)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--output-dir", default="artifacts/draft_population_ml")
    parser.add_argument(
        "--no-chunk-years",
        action="store_true",
        help="Use one aggregate query instead of smaller per-year queries.",
    )
    parser.add_argument("--quiet", action="store_true", help="Print only a compact run summary.")
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args()

    load_env()
    reader = FlyReader()
    raw = fetch_pick_dataset(
        reader,
        min_year=args.min_year,
        max_rows=args.max_rows,
        chunk_years=not args.no_chunk_years,
    )
    df = clean_dataset(raw)
    if df.empty:
        raise RuntimeError("No draft rows returned from Fly.")

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "min_year": args.min_year,
        "max_rows": args.max_rows,
        "aggregates": aggregate_report(df, min_group_rows=args.min_group_rows),
        "model": model_report(
            df,
            random_state=args.random_state,
            max_importance_rows=args.max_importance_rows,
            max_model_rows=args.max_model_rows,
        ),
    }

    if not args.no_write:
        path = write_report(report, REPO_ROOT / args.output_dir)
        report["output_path"] = str(path)

    if args.quiet:
        print(
            json.dumps(
                {
                    "output_path": report.get("output_path"),
                    "population": report["aggregates"]["population"],
                    "model": {
                        key: value for key, value in report["model"].items() if key != "top_permutation_importance"
                    },
                    "top_features": report["model"].get("top_permutation_importance", [])[:8],
                },
                indent=2,
            )
        )
    else:
        print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
