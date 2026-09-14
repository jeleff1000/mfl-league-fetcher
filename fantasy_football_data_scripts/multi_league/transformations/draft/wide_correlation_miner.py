#!/usr/bin/env python3
"""Wide-column draft correlation miner.

This is the unknown-unknowns pass. Unlike the curated tendency miner, this
module generates candidate features from many available draft/player/bio
columns and then scores both manager affinities and league-wide value
inefficiencies. The output is discovery evidence only; promotion still belongs
to the validation gate.
"""

from __future__ import annotations

import argparse
from collections import Counter
import json
import math
from pathlib import Path
import sys
from typing import Any

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

from multi_league.core.db_utils import get_pipeline_connection
from multi_league.transformations.aggregation.aggregation_utils import (
    central_table,
    configure_table_catalog,
    make_logger,
    resolve_db_name,
)
from multi_league.transformations.draft.draft_feature_mining import quote_sql
from multi_league.transformations.draft.mining_feature_registry import (
    league_scoring_config,
    load_registry,
    select_miner_features,
)

log = make_logger("DRAFT-WIDE")

MODEL_VERSION = "draft-wide-correlation-v0.2-registry"

FEATURE_PREFIXES = {
    "draft",
    "bio",
    "prev",
}

ID_COLUMNS = {
    "db_name",
    "year",
    "manager",
    "manager_guid",
    "franchise_id",
    "team_key",
    "team_name",
    "league_id",
    "player",
    "NFL_player_id",
    "draft_id",
    "yahoo_player_id",
    "sleeper_player_id",
    "espn_player_id",
}

TARGET_OR_LEAK_COLUMNS = {
    "manager_lamar",
    "player_lamar",
    "expected_lamar",
    "pick_quality_zscore",
    "draft_value_zscore",
    "draft_grade",
    "manager_draft_score",
    "manager_draft_grade",
    "keeper_draft_score",
    "keeper_draft_grade",
    "pick_score",
    "total_fantasy_points",
    "season_ppg",
    "games_played",
    "games_started",
    "games_eligible",
    "weeks_rostered",
    "weeks_started",
    "season_position_rank",
    "manager_total_lamar",
    "manager_avg_lamar",
    "manager_picks_count",
    "manager_hit_rate",
    "manager_draft_percentile_alltime",
    "manager_weighted_age",
    "bench_lamar",
    "bench_value_by_rank",
    "latest_team",
    "status",
}

# Feature columns are no longer hand-listed: the semantic-contracted mining
# registry (mining_feature_registry.py -> docs/mining-feature-registry.json)
# is the single source of the candidate universe. ID_COLUMNS and
# TARGET_OR_LEAK_COLUMNS remain as defense-in-depth guards; the registry's
# contracts are the primary firewall (proven by
# tests/unit/transformations/test_mining_feature_registry.py).

_REGISTRY_TABLE_PREFIXES = {
    "___leagues.public.draft": "draft",
    "___ops.nfl_historical.player_bio": "bio",
    "___ops.nfl_historical.player_nfl_season": "prev",
}

#: Derived draft features — computed in SQL, not registry columns. Raw
#: market numbers would only ever surface as bin edges; these are the
#: LEGIBLE behavioral reads (reach vs value vs market, stacking).
#: The market baseline is FLEET ADP — how ~1,300 real leagues drafted the
#: same player that same year (the centralized table has no platform ADP,
#: and our own market is the better baseline anyway: real drafts, every
#: platform). Stage units: 0.07 of a board ~ a full round in a 12-teamer.
DERIVED_DRAFT_FEATURES: dict[str, dict[str, Any]] = {
    "market_reach": {
        "label": "Fleet ADP Reach",
        "legibility": "A",
        "importance": 3,
        "sql": (
            "CASE"
            " WHEN fa.adp_stage IS NULL OR COALESCE(b.pick, 0) <= 0"
            "  OR b.cost > 0 OR b.max_pick <= 1 THEN NULL"
            " WHEN fa.adp_stage - (b.pick * 1.0 / b.max_pick) >= 0.07 THEN 'big reach'"
            " WHEN fa.adp_stage - (b.pick * 1.0 / b.max_pick) >= 0.025 THEN 'reach'"
            " WHEN (b.pick * 1.0 / b.max_pick) - fa.adp_stage >= 0.07 THEN 'big value'"
            " WHEN (b.pick * 1.0 / b.max_pick) - fa.adp_stage >= 0.025 THEN 'value'"
            " ELSE 'market price'"
            " END"
        ),
    },
    "market_price": {
        "label": "Fleet Market Price Paid",
        "legibility": "A",
        "importance": 3,
        "sql": (
            "CASE"
            " WHEN fa.adp_cost_share IS NULL OR COALESCE(b.cost, 0) <= 0"
            "  OR COALESCE(tl.total_cost, 0) <= 0 OR COALESCE(tl.n_mgrs, 0) <= 0 THEN NULL"
            " WHEN (b.cost * tl.n_mgrs * 1.0 / tl.total_cost) - fa.adp_cost_share >= 0.04 THEN 'way over market'"
            " WHEN (b.cost * tl.n_mgrs * 1.0 / tl.total_cost) - fa.adp_cost_share >= 0.015 THEN 'over market'"
            " WHEN fa.adp_cost_share - (b.cost * tl.n_mgrs * 1.0 / tl.total_cost) >= 0.04 THEN 'big bargain'"
            " WHEN fa.adp_cost_share - (b.cost * tl.n_mgrs * 1.0 / tl.total_cost) >= 0.015 THEN 'bargain'"
            " ELSE 'market price'"
            " END"
        ),
    },
    "team_stack": {
        "label": "QB Stack",
        "legibility": "A",
        "importance": 3,
        "sql": (
            "CASE"
            " WHEN b.position_group IN ('WR', 'TE')"
            "  AND SUM(CASE WHEN b.position_group = 'QB' THEN 1 ELSE 0 END)"
            "      OVER (PARTITION BY b.manager_key, b.year, b.stack_team) >= 1"
            "  AND b.stack_team IS NOT NULL"
            " THEN 'QB stack'"
            " WHEN b.position_group = 'QB'"
            "  AND SUM(CASE WHEN b.position_group IN ('WR', 'TE') THEN 1 ELSE 0 END)"
            "      OVER (PARTITION BY b.manager_key, b.year, b.stack_team) >= 1"
            "  AND b.stack_team IS NOT NULL"
            " THEN 'QB stack'"
            " ELSE NULL"
            " END"
        ),
    },
}

MANAGER_AFFINITY_EXCLUDED_PREFIXES = {
    "draft.round",
    "draft.pick",
    "draft.pick_in_round",
    "draft.draft_slot",
    "draft.cost",
    "draft.cost_bucket",
    "draft.draft_slot_roster_id",
    "manager_draft_prior",
    "manager_matchup_prior",
    "league",
}

LEAGUE_INEFFICIENCY_EXCLUDED_PREFIXES = {
    "draft.round",
    "draft.pick",
    "draft.pick_in_round",
    "draft.draft_slot",
    "draft.cost",
    "draft.cost_bucket",
    "draft.draft_slot_roster_id",
    # Reach/price vs the fleet market are MANAGER tendencies; as league
    # value-residual features they are circular ("overpays for overpays").
    "draft.market_reach",
    "draft.market_price",
}

#: A manager tendency only counts as a "habit" if it spans at least this many
#: distinct players AND no single player accounts for more than
#: MAX_SINGLE_PLAYER_SHARE of the picks. One player drafted N years running
#: (a keeper) is a loyalty to that player (surfaced separately by the
#: construction miner), not a habit of drafting their school/team/type — and
#: two players where one is a repeat-kept keeper is still really about that
#: one player.
MIN_DISTINCT_PLAYERS = 2
MAX_SINGLE_PLAYER_SHARE = 0.6

def _qident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _draft_table() -> str:
    return central_table("draft")


def _settings_table() -> str:
    return central_table("league_settings")


#: Stats where a LOWER number is better (ranks, timed combine drills) —
#: the named-tier featurization inverts its percentile for these.
LOWER_IS_BETTER_COLUMNS = {"forty", "cone", "shuttle"}


class RegistrySelection:
    """The sanctioned feature universe for one league's mining run."""

    def __init__(self, entries: list[dict[str, Any]]):
        self.by_prefix: dict[str, list[str]] = {prefix: [] for prefix in _REGISTRY_TABLE_PREFIXES.values()}
        self.metadata: dict[str, dict[str, Any]] = {}
        for entry in entries:
            prefix = _REGISTRY_TABLE_PREFIXES.get(entry["table"])
            if prefix is None:
                continue
            column = entry["column"]
            if column in ID_COLUMNS or column in TARGET_OR_LEAK_COLUMNS:
                continue  # defense-in-depth; the registry should already bar these
            self.by_prefix[prefix].append(column)
            self.metadata[f"{prefix}.{column}"] = {
                "feature_label": entry["semantic_name"],
                "legibility": entry["legibility"],
                "importance": entry.get("importance", 2),
                "family": entry["family"],
            }

    def register_derived(self) -> None:
        for name, spec in DERIVED_DRAFT_FEATURES.items():
            self.metadata[f"draft.{name}"] = {
                "feature_label": spec["label"],
                "legibility": spec["legibility"],
                "importance": spec["importance"],
                "family": "derived",
            }

    def lookup(self, feature_type: str) -> dict[str, Any] | None:
        key = feature_type
        for suffix in ("_bin", "_tier"):
            if key.endswith(suffix):
                key = key[: -len(suffix)]
                break
        return self.metadata.get(key)

    def lower_is_better(self, prefix: str, column: str) -> bool:
        if column in LOWER_IS_BETTER_COLUMNS:
            return True
        meta = self.metadata.get(f"{prefix}.{column}")
        return bool(meta and meta.get("family") == "rank")


def load_registry_selection(
    league_settings_row: dict[str, Any] | None,
    *,
    max_features: int = 300,
) -> RegistrySelection:
    registry = load_registry()
    config = league_scoring_config(league_settings_row)
    entries = select_miner_features(
        registry,
        tables=set(_REGISTRY_TABLE_PREFIXES),
        league_config=config,
        max_features=max_features,
    )
    selection = RegistrySelection(entries)
    selection.register_derived()
    return selection


def fetch_league_settings_row(fetch_df, db_name: str) -> dict[str, Any] | None:
    """Latest league_settings row (scoring config) via any dataframe fetcher."""
    sql = (
        f"SELECT * FROM {_settings_table()} "
        f"WHERE db_name = {quote_sql(db_name)} ORDER BY year DESC LIMIT 1"
    )
    try:
        df = fetch_df(sql)
        if df is None or len(df) == 0:
            return None
        return {str(k): v for k, v in df.iloc[0].to_dict().items()}
    except Exception as exc:  # pragma: no cover - schema drift tolerance
        log(f"[wide-miner] league_settings unavailable for {db_name}: {exc}")
        return None


def _live_columns(fetch_df, fq_table: str) -> set[str]:
    """Column names that actually exist in a live table (empty set if the
    schema can't be introspected — callers treat that as 'don't prune')."""
    try:
        df = fetch_df(f"DESCRIBE {fq_table}")
    except Exception:  # pragma: no cover - introspection is best-effort
        return set()
    if df is None or len(df) == 0:
        return set()
    col = "column_name" if "column_name" in df.columns else df.columns[0]
    return {str(v) for v in df[col]}


def prune_selection_to_live_schema(fetch_df, selection: RegistrySelection) -> None:
    """Drop selected columns the live schema no longer has, so registry/table
    drift skips a feature instead of failing the whole run with a binder error
    (e.g. a stale `player_nfl_season.pts_k_std` entry). Mutates the selection.
    """
    for table, prefix in _REGISTRY_TABLE_PREFIXES.items():
        wanted = selection.by_prefix.get(prefix)
        if not wanted:
            continue
        live = _live_columns(fetch_df, table)
        if not live:
            continue  # couldn't introspect — leave as-is
        kept = [c for c in wanted if c in live]
        dropped = [c for c in wanted if c not in live]
        if dropped:
            log(f"[wide-miner] {prefix}: dropping {len(dropped)} column(s) absent "
                f"from live schema: {sorted(dropped)[:6]}")
            selection.by_prefix[prefix] = kept


def build_wide_base_sql(db_name: str, selection: RegistrySelection) -> str:
    """Build the row-level dataset used by the wide miner."""
    draft_select = ",\n        ".join(
        f"d.{_qident(col)} AS draft__{col}" for col in selection.by_prefix["draft"]
    ) or "NULL AS draft__none"
    bio_select = ",\n        ".join(
        f"pb.{_qident(col)} AS bio__{col}" for col in selection.by_prefix["bio"]
    ) or "NULL AS bio__none"
    prev_select = ",\n    ".join(
        f"ps.{_qident(col)} AS prev__{col}" for col in selection.by_prefix["prev"]
    ) or "NULL AS prev__none"
    return f"""
WITH draft_base AS (
    SELECT
        d.db_name,
        d.year,
        COALESCE(d.franchise_id, d.manager) AS manager_key,
        d.manager,
        d.NFL_player_id,
        d.position,
        COALESCE(d.cost, 0) AS cost,
        d.pick,
        d.round,
        d.manager_lamar,
        d.expected_lamar,
        d.pick_score,
        d.draft_grade,
        UPPER(NULLIF(d.nfl_team, '')) AS stack_team,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper,
        {draft_select}
    FROM {_draft_table()} d
    WHERE d.db_name = {quote_sql(db_name)}
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
),
with_capital AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.year) AS max_pick,
        CASE
            WHEN COALESCE(b.cost, 0) > 0 THEN 'auction'
            ELSE 'snake'
        END AS pick_mode,
        UPPER(COALESCE(NULLIF(b.position, ''), 'UNK')) AS position_group
    FROM draft_base b
    WHERE b.is_keeper = 0
),
target_ly AS (
    SELECT
        year,
        COUNT(DISTINCT manager_key) AS n_mgrs,
        SUM(CASE WHEN cost > 0 THEN cost END) AS total_cost
    FROM draft_base
    WHERE is_keeper = 0
    GROUP BY year
),
fleet_base AS (
    SELECT db_name, year, NFL_player_id, manager, cost, pick
    FROM {_draft_table()}
    WHERE year IS NOT NULL
      AND NFL_player_id IS NOT NULL
      AND COALESCE(is_keeper, 0) = 0
),
fleet_league_year AS (
    SELECT
        db_name,
        year,
        MAX(pick) AS mx,
        SUM(CASE WHEN cost > 0 THEN cost END) AS total_cost,
        COUNT(DISTINCT manager) AS n_mgrs
    FROM fleet_base
    GROUP BY db_name, year
),
fleet_adp AS (
    -- Fleet ADP: how ~1,300 real leagues priced this player this year.
    SELECT
        fb.NFL_player_id,
        fb.year,
        AVG(CASE WHEN COALESCE(fb.pick, 0) > 0 AND fly.mx > 1
                 THEN fb.pick * 1.0 / fly.mx END) AS adp_stage,
        AVG(CASE WHEN COALESCE(fb.cost, 0) > 0 AND COALESCE(fly.total_cost, 0) > 0 AND fly.n_mgrs > 0
                 THEN fb.cost * fly.n_mgrs * 1.0 / fly.total_cost END) AS adp_cost_share,
        COUNT(*) AS fleet_picks
    FROM fleet_base fb
    JOIN fleet_league_year fly
      ON fb.db_name = fly.db_name AND fb.year = fly.year
    GROUP BY fb.NFL_player_id, fb.year
    HAVING COUNT(*) >= 25
)
SELECT
    b.db_name,
    b.year,
    b.manager_key,
    b.manager,
    b.NFL_player_id,
    b.position_group,
    CASE
        WHEN b.cost > 0 THEN GREATEST(b.cost, 1)
        ELSE GREATEST(b.max_pick + 1 - COALESCE(b.pick, b.round, b.max_pick), 1)
    END AS capital_weight,
    CASE
        WHEN b.cost > 0 THEN
            CASE
                WHEN b.cost >= 50 THEN 'auction_50_plus'
                WHEN b.cost >= 30 THEN 'auction_30_49'
                WHEN b.cost >= 16 THEN 'auction_16_29'
                WHEN b.cost >= 6 THEN 'auction_6_15'
                ELSE 'auction_1_5'
            END
        ELSE
            CASE
                WHEN COALESCE(b.round, 99) <= 2 THEN 'snake_r1_2'
                WHEN COALESCE(b.round, 99) <= 5 THEN 'snake_r3_5'
                WHEN COALESCE(b.round, 99) <= 9 THEN 'snake_r6_9'
                ELSE 'snake_r10_plus'
            END
    END AS capital_bucket,
    b.manager_lamar,
    b.expected_lamar,
    b.pick_score,
    b.draft_grade,
    {", ".join(f"b.draft__{col}" for col in selection.by_prefix["draft"]) or "NULL AS draft__none"},
    {", ".join(f"{spec['sql']} AS draft__{name}" for name, spec in DERIVED_DRAFT_FEATURES.items())},
    {bio_select},
    {prev_select}
FROM with_capital b
LEFT JOIN ___ops.nfl_historical.player_bio pb ON b.NFL_player_id = pb.NFL_player_id
LEFT JOIN ___ops.nfl_historical.player_nfl_season ps
    ON b.NFL_player_id = ps.NFL_player_id AND ps.year = b.year - 1
LEFT JOIN fleet_adp fa ON b.NFL_player_id = fa.NFL_player_id AND b.year = fa.year
LEFT JOIN target_ly tl ON b.year = tl.year
"""


def _is_numeric_series(series) -> bool:
    import pandas as pd

    return pd.api.types.is_numeric_dtype(series)


def _clean_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null", "unknown"}:
        return None
    if len(text) > 80:
        return None
    return text


def _normalize_feature_value(raw_name: str, value: str) -> str:
    if raw_name in {"nfl_team", "nfl_team_api", "latest_team", "nfl_draft_team"}:
        return value.upper()
    return value


def _numeric_bins(series, *, max_bins: int = 4) -> dict[int, str]:
    import pandas as pd

    numeric = pd.to_numeric(series, errors="coerce")
    valid = numeric.dropna()
    if valid.nunique() < 2:
        return {}
    try:
        bucketed = pd.qcut(valid, q=min(max_bins, valid.nunique()), duplicates="drop")
    except ValueError:
        return {}
    labels = {}
    for index, interval in bucketed.items():
        labels[index] = f"{float(interval.left):.3g}_to_{float(interval.right):.3g}"
    return labels


#: Named tiers for position-relative percentiles. Plain words a league-mate
#: reads without explanation — never numeric bin edges (nugget contract).
def _tier_label(pct: float) -> str | None:
    if pct >= 0.90:
        return "elite"
    if pct >= 0.65:
        return "strong"
    if pct >= 0.35:
        return "middling"
    return "weak"


def _position_tier_labels(df, column: str, *, invert: bool, min_group: int = 8) -> dict[int, str]:
    """Label each pick's stat as elite/strong/middling/weak RELATIVE TO THE
    SAME POSITION in the same season — 'coming off an elite season for a TE'
    is readable; '9.99_to_13.6' is not."""
    import pandas as pd

    numeric = pd.to_numeric(df[column], errors="coerce")
    frame = pd.DataFrame({
        "value": numeric,
        "position_group": df["position_group"],
        "year": df["year"],
    }).dropna(subset=["value"])
    if frame.empty:
        return {}

    labels: dict[int, str] = {}
    for _, group in frame.groupby(["position_group", "year"]):
        if len(group) < min_group or group["value"].nunique() < 2:
            continue
        pct = group["value"].rank(pct=True, method="average")
        if invert:
            pct = 1.0 - pct + (1.0 / len(group))
        for idx, value in pct.items():
            label = _tier_label(float(value))
            if label:
                labels[idx] = label
    return labels


def build_wide_features(
    df, *, min_support: int = 4, selection: "RegistrySelection | None" = None
) -> list[dict[str, Any]]:
    """Generate binary feature events from many source columns."""
    import pandas as pd

    feature_cols = [
        col
        for col in df.columns
        if "__" in col
        and col.split("__", 1)[0] in FEATURE_PREFIXES
        and col.split("__", 1)[1] not in ID_COLUMNS
        and col.split("__", 1)[1] not in TARGET_OR_LEAK_COLUMNS
    ]

    rows: list[dict[str, Any]] = []
    for column in feature_cols:
        source, raw_name = column.split("__", 1)
        series = df[column]
        if _is_numeric_series(series):
            numeric = pd.to_numeric(series, errors="coerce")
            if numeric.dropna().nunique() <= 2:
                values = numeric.dropna().astype(int).astype(str)
                counts = Counter(values)
                for value, count in counts.items():
                    if count >= min_support:
                        mask = values == value
                        for idx in values[mask].index:
                            rows.append(
                                {"row_index": idx, "feature_type": f"{source}.{raw_name}", "feature_value": value}
                            )
            elif source in ("prev", "bio"):
                # Stat features: position-relative named tiers, never bin edges.
                invert = selection.lower_is_better(source, raw_name) if selection else False
                labels = _position_tier_labels(df, column, invert=invert)
                counts = Counter(labels.values())
                for idx, value in labels.items():
                    if counts[value] >= min_support:
                        rows.append(
                            {"row_index": idx, "feature_type": f"{source}.{raw_name}_tier", "feature_value": value}
                        )
            else:
                labels = _numeric_bins(numeric)
                counts = Counter(labels.values())
                for idx, value in labels.items():
                    if counts[value] >= min_support:
                        rows.append(
                            {"row_index": idx, "feature_type": f"{source}.{raw_name}_bin", "feature_value": value}
                        )
        else:
            cleaned = series.map(_clean_value)
            feature_name = raw_name
            cleaned = cleaned.map(
                lambda value, name=feature_name: _normalize_feature_value(name, value) if value is not None else None
            )
            counts = Counter(value for value in cleaned if value is not None)
            for idx, value in cleaned.items():
                if value is not None and counts[value] >= min_support:
                    rows.append({"row_index": idx, "feature_type": f"{source}.{raw_name}", "feature_value": value})
    return rows


def _is_manager_affinity_feature(feature_type: str) -> bool:
    return not any(
        feature_type == prefix or feature_type.startswith(f"{prefix}.") or feature_type.startswith(f"{prefix}_bin")
        for prefix in MANAGER_AFFINITY_EXCLUDED_PREFIXES
    )


def _is_league_inefficiency_feature(feature_type: str) -> bool:
    return not any(
        feature_type == prefix or feature_type.startswith(f"{prefix}.") or feature_type.startswith(f"{prefix}_bin")
        for prefix in LEAGUE_INEFFICIENCY_EXCLUDED_PREFIXES
    )


def _weighted_mean(values, weights) -> float:
    total_weight = float(weights.sum())
    if total_weight <= 0:
        return 0.0
    return float((values * weights).sum() / total_weight)


def _score_manager_affinities(
    df, feature_rows, *, min_picks: int, min_abs_z: float, limit: int
) -> list[dict[str, Any]]:
    import pandas as pd

    events = pd.DataFrame(feature_rows)
    if events.empty:
        return []
    events = events[events["feature_type"].map(_is_manager_affinity_feature)].copy()
    if events.empty:
        return []
    base = df.reset_index(names="row_index")
    events = events.merge(
        base[
            [
                "row_index",
                "db_name",
                "year",
                "manager_key",
                "manager",
                "NFL_player_id",
                "position_group",
                "capital_bucket",
                "capital_weight",
            ]
        ],
        on="row_index",
        how="inner",
    )
    # Two measurement axes (Joe, 2026-07-06): CAPITAL (how much money/draft
    # position a manager invests) and COUNT (how many roster spots). They are
    # different tendencies — "pays up for elite RBs" vs "hoards cheap RBs" —
    # and capital alone is structurally blind to cheap-position behaviors:
    # the manager who rosters a K2 every single year spends ~$1 on it, but
    # owns 100% of the league's K2 market.
    strata_keys = ["db_name", "year", "feature_type", "position_group", "capital_bucket"]
    feature_keys = ["db_name", "year", "feature_type", "feature_value", "position_group", "capital_bucket"]

    strata = (
        events.groupby(strata_keys, dropna=False)
        .agg(stratum_capital=("capital_weight", "sum"), stratum_events=("capital_weight", "size"))
        .reset_index()
    )
    feature_strata = (
        events.groupby(feature_keys, dropna=False)
        .agg(feature_capital=("capital_weight", "sum"), feature_events=("capital_weight", "size"))
        .reset_index()
    )
    rates = feature_strata.merge(strata, on=strata_keys, how="inner")
    rates["expected_share"] = rates["feature_capital"] / rates["stratum_capital"]
    rates["expected_count_share"] = rates["feature_events"] / rates["stratum_events"]

    # League-wide event totals per feature value (all years): the market a
    # manager can own outright.
    league_feature_events = (
        events.groupby(["feature_type", "feature_value"], dropna=False)
        .size()
        .rename("league_feature_events")
        .reset_index()
    )

    # Distinct players behind each manager tendency, and how concentrated the
    # tendency is in a single player. A "habit" must cross multiple players and
    # not be carried by one repeat-drafted guy — drafting the SAME Tulane
    # player six years running (a keeper) is a loyalty to that player (surfaced
    # separately), not a habit of drafting Tulane. We gate both out below.
    _player_counts = (
        events.groupby(
            ["db_name", "manager_key", "manager", "feature_type", "feature_value", "NFL_player_id"],
            dropna=False,
        )
        .size()
        .rename("player_picks")
        .reset_index()
    )
    manager_feature_players = (
        _player_counts.groupby(
            ["db_name", "manager_key", "manager", "feature_type", "feature_value"], dropna=False
        )
        .agg(
            n_players=("player_picks", "size"),
            _total_player_picks=("player_picks", "sum"),
            _max_player_picks=("player_picks", "max"),
        )
        .reset_index()
    )
    manager_feature_players["top_player_share"] = (
        manager_feature_players["_max_player_picks"] / manager_feature_players["_total_player_picks"]
    )

    manager_strata = (
        events.groupby(
            ["db_name", "year", "manager_key", "feature_type", "position_group", "capital_bucket"], dropna=False
        )
        .agg(
            manager_capital=("capital_weight", "sum"),
            manager_weight_sq=("capital_weight", lambda s: float((s * s).sum())),
            manager_events=("capital_weight", "size"),
        )
        .reset_index()
    )
    manager_feature = (
        events.groupby(
            [
                "db_name",
                "year",
                "manager_key",
                "manager",
                "feature_type",
                "feature_value",
                "position_group",
                "capital_bucket",
            ],
            dropna=False,
        )
        .agg(picks=("capital_weight", "size"), observed_capital=("capital_weight", "sum"))
        .reset_index()
    )
    scored = manager_feature.merge(
        manager_strata,
        on=["db_name", "year", "manager_key", "feature_type", "position_group", "capital_bucket"],
        how="inner",
    ).merge(
        rates[feature_keys + ["expected_share", "expected_count_share"]],
        on=feature_keys,
        how="inner",
    )
    # Capital axis.
    scored["expected_capital"] = scored["manager_capital"] * scored["expected_share"]
    scored["effective_n"] = (scored["manager_capital"] ** 2 / scored["manager_weight_sq"].clip(lower=1e-9)).clip(
        lower=1.0
    )
    scored["observed_effective"] = (
        scored["observed_capital"] / scored["manager_capital"].replace(0, math.nan) * scored["effective_n"]
    )
    scored["expected_effective"] = scored["expected_share"] * scored["effective_n"]
    scored["variance"] = scored["effective_n"] * scored["expected_share"] * (1 - scored["expected_share"])
    # Count axis (integer slots — plain binomial).
    scored["expected_picks"] = scored["manager_events"] * scored["expected_count_share"]
    scored["count_variance"] = (
        scored["manager_events"] * scored["expected_count_share"] * (1 - scored["expected_count_share"])
    )

    year_rollup = (
        scored.groupby(["db_name", "manager_key", "manager", "feature_type", "feature_value", "year"], dropna=False)
        .agg(
            picks=("picks", "sum"),
            observed_capital=("observed_capital", "sum"),
            expected_capital=("expected_capital", "sum"),
            manager_capital=("manager_capital", "sum"),
            observed_effective=("observed_effective", "sum"),
            expected_effective=("expected_effective", "sum"),
            variance=("variance", "sum"),
            expected_picks=("expected_picks", "sum"),
            count_variance=("count_variance", "sum"),
        )
        .reset_index()
    )
    rollup = (
        year_rollup.groupby(["db_name", "manager_key", "manager", "feature_type", "feature_value"], dropna=False)
        .agg(
            picks=("picks", "sum"),
            years_seen=("year", "nunique"),
            positive_years=(
                "observed_capital",
                lambda s: int((s > year_rollup.loc[s.index, "expected_capital"]).sum()),
            ),
            negative_years=(
                "observed_capital",
                lambda s: int((s < year_rollup.loc[s.index, "expected_capital"]).sum()),
            ),
            count_positive_years=(
                "picks",
                lambda s: int((s > year_rollup.loc[s.index, "expected_picks"]).sum()),
            ),
            count_negative_years=(
                "picks",
                lambda s: int((s < year_rollup.loc[s.index, "expected_picks"]).sum()),
            ),
            observed_capital=("observed_capital", "sum"),
            expected_capital=("expected_capital", "sum"),
            manager_capital=("manager_capital", "sum"),
            observed_effective=("observed_effective", "sum"),
            expected_effective=("expected_effective", "sum"),
            variance=("variance", "sum"),
            expected_picks=("expected_picks", "sum"),
            count_variance=("count_variance", "sum"),
        )
        .reset_index()
    )
    rollup = rollup[rollup["picks"] >= min_picks].copy()
    if rollup.empty:
        return []
    # A habit must span multiple distinct players AND not be dominated by a
    # single repeat-drafted player (see manager_feature_players).
    rollup = rollup.merge(
        manager_feature_players[
            ["db_name", "manager_key", "manager", "feature_type", "feature_value", "n_players", "top_player_share"]
        ],
        on=["db_name", "manager_key", "manager", "feature_type", "feature_value"],
        how="left",
    )
    rollup["n_players"] = rollup["n_players"].fillna(0).astype(int)
    rollup["top_player_share"] = rollup["top_player_share"].fillna(1.0)
    rollup = rollup[
        (rollup["n_players"] >= MIN_DISTINCT_PLAYERS)
        & (rollup["top_player_share"] <= MAX_SINGLE_PLAYER_SHARE)
    ].copy()
    if rollup.empty:
        return []
    rollup = rollup.merge(league_feature_events, on=["feature_type", "feature_value"], how="left")
    rollup["capital_z_score"] = (rollup["observed_effective"] - rollup["expected_effective"]) / rollup["variance"].clip(
        lower=1.0
    ).pow(0.5)
    rollup["count_z_score"] = (rollup["picks"] - rollup["expected_picks"]) / rollup["count_variance"].clip(
        lower=0.5
    ).pow(0.5)
    # A signal qualifies on EITHER axis.
    keep = (rollup["capital_z_score"].abs() >= min_abs_z) | (rollup["count_z_score"].abs() >= min_abs_z)
    rollup = rollup[keep].copy()
    if rollup.empty:
        return []
    rollup["repeatability"] = rollup.apply(
        lambda r: (r["positive_years"] if r["observed_capital"] >= r["expected_capital"] else r["negative_years"])
        / max(r["years_seen"], 1),
        axis=1,
    )
    rollup["count_repeatability"] = rollup.apply(
        lambda r: (r["count_positive_years"] if r["picks"] >= r["expected_picks"] else r["count_negative_years"])
        / max(r["years_seen"], 1),
        axis=1,
    )
    rollup["observed_share"] = rollup["observed_capital"] / rollup["manager_capital"].replace(0, math.nan)
    rollup["expected_share"] = rollup["expected_capital"] / rollup["manager_capital"].replace(0, math.nan)
    rollup["lift"] = rollup["observed_capital"] / rollup["expected_capital"].replace(0, math.nan)
    rollup["count_lift"] = rollup["picks"] / rollup["expected_picks"].replace(0, math.nan)
    rollup["market_share"] = rollup["picks"] / rollup["league_feature_events"].replace(0, math.nan)
    rollup["excess_capital"] = rollup["observed_capital"] - rollup["expected_capital"]
    rollup["dominant_z"] = rollup[["capital_z_score", "count_z_score"]].abs().max(axis=1)
    rollup = rollup.sort_values(["dominant_z", "excess_capital"], key=lambda s: s.abs(), ascending=False).head(limit)
    return [
        {
            "db_name": str(row.db_name),
            "scope_type": "manager",
            "scope_key": str(row.manager_key),
            "scope_label": str(row.manager),
            "feature_type": str(row.feature_type),
            "feature_value": str(row.feature_value),
            "picks": int(row.picks),
            "n_players": int(row.n_players),
            "top_player_share": round(float(row.top_player_share), 3),
            "years_seen": int(row.years_seen),
            "positive_years": int(row.positive_years),
            "negative_years": int(row.negative_years),
            "count_positive_years": int(row.count_positive_years),
            "count_negative_years": int(row.count_negative_years),
            "repeatability": round(float(row.repeatability), 4),
            "count_repeatability": round(float(row.count_repeatability), 4),
            "observed_capital": round(float(row.observed_capital), 3),
            "expected_capital": round(float(row.expected_capital), 3),
            "excess_capital": round(float(row.excess_capital), 3),
            "observed_share": round(float(row.observed_share), 4),
            "expected_share": round(float(row.expected_share), 4),
            "lift": round(float(row.lift), 4),
            "count_lift": round(float(row.count_lift), 4),
            "expected_picks": round(float(row.expected_picks), 2),
            "market_share": round(float(row.market_share), 4) if row.market_share == row.market_share else None,
            "capital_z_score": round(float(row.capital_z_score), 3),
            "count_z_score": round(float(row.count_z_score), 3),
            "confidence": "wide",
            "evidence_level": "explore",
            "model_version": MODEL_VERSION,
        }
        for row in rollup.itertuples(index=False)
    ]


def _score_league_inefficiencies(
    df, feature_rows, *, min_picks: int, min_years: int, min_abs_z: float, limit: int
) -> list[dict[str, Any]]:
    import pandas as pd

    events = pd.DataFrame(feature_rows)
    if events.empty:
        return []
    events = events[events["feature_type"].map(_is_league_inefficiency_feature)].copy()
    if events.empty:
        return []
    base = df.reset_index(names="row_index").copy()
    base["value_residual"] = pd.to_numeric(base["manager_lamar"], errors="coerce") - pd.to_numeric(
        base["expected_lamar"], errors="coerce"
    )
    base["pick_score_value"] = pd.to_numeric(base["pick_score"], errors="coerce")
    base = base.dropna(subset=["value_residual", "capital_weight"])
    events = events.merge(
        base[
            [
                "row_index",
                "db_name",
                "year",
                "position_group",
                "capital_bucket",
                "capital_weight",
                "value_residual",
                "pick_score_value",
            ]
        ],
        on="row_index",
        how="inner",
    )
    if events.empty:
        return []

    baseline_rows = []
    for keys, group in events.groupby(
        ["db_name", "year", "feature_type", "position_group", "capital_bucket"], dropna=False
    ):
        weights = group["capital_weight"].astype(float)
        residual = group["value_residual"].astype(float)
        mean = _weighted_mean(residual, weights)
        variance = max(_weighted_mean((residual - mean) ** 2, weights), 0.01)
        pick_mean = _weighted_mean(group["pick_score_value"].fillna(0).astype(float), weights)
        baseline_rows.append((*keys, mean, variance, pick_mean))
    baseline = pd.DataFrame(
        baseline_rows,
        columns=[
            "db_name",
            "year",
            "feature_type",
            "position_group",
            "capital_bucket",
            "expected_residual",
            "residual_variance",
            "expected_pick_score",
        ],
    )
    scored = events.merge(
        baseline, on=["db_name", "year", "feature_type", "position_group", "capital_bucket"], how="inner"
    )

    year_rows = []
    for keys, group in scored.groupby(["feature_type", "feature_value", "db_name", "year"], dropna=False):
        weights = group["capital_weight"].astype(float)
        observed = _weighted_mean(group["value_residual"].astype(float), weights)
        expected = _weighted_mean(group["expected_residual"].astype(float), weights)
        observed_pick = _weighted_mean(group["pick_score_value"].fillna(0).astype(float), weights)
        expected_pick = _weighted_mean(group["expected_pick_score"].astype(float), weights)
        variance = float(
            (weights * weights * group["residual_variance"].astype(float)).sum() / max(float(weights.sum()) ** 2, 1e-9)
        )
        year_rows.append(
            (
                *keys,
                len(group),
                float(weights.sum()),
                observed,
                expected,
                observed - expected,
                observed_pick,
                expected_pick,
                variance,
            )
        )
    candidate_year = pd.DataFrame(
        year_rows,
        columns=[
            "feature_type",
            "feature_value",
            "db_name",
            "year",
            "picks",
            "observed_capital",
            "observed_residual",
            "expected_residual",
            "excess_residual",
            "observed_pick_score",
            "expected_pick_score",
            "variance",
        ],
    )
    if candidate_year.empty:
        return []

    scope_db = "fleet" if df["db_name"].nunique() > 1 else str(df["db_name"].iloc[0])
    rollup_rows = []
    for keys, group in candidate_year.groupby(["feature_type", "feature_value"], dropna=False):
        picks = int(group["picks"].sum())
        years_seen = int(group[["db_name", "year"]].drop_duplicates().shape[0])
        if picks < min_picks or years_seen < min_years:
            continue
        weights = group["observed_capital"].astype(float)
        observed = _weighted_mean(group["observed_residual"].astype(float), weights)
        expected = _weighted_mean(group["expected_residual"].astype(float), weights)
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
        rollup_rows.append(
            {
                "db_name": scope_db,
                "scope_type": "league_inefficiency",
                "scope_key": scope_db,
                "scope_label": scope_db,
                "feature_type": str(keys[0]),
                "feature_value": str(keys[1]),
                "picks": picks,
                "years_seen": years_seen,
                "positive_years": positive_years,
                "negative_years": negative_years,
                "repeatability": round(float(repeatability), 4),
                "observed_capital": round(float(weights.sum()), 3),
                "observed_residual": round(float(observed), 4),
                "expected_residual": round(float(expected), 4),
                "excess_residual": round(float(excess), 4),
                "observed_pick_score": round(_weighted_mean(group["observed_pick_score"].astype(float), weights), 3),
                "expected_pick_score": round(_weighted_mean(group["expected_pick_score"].astype(float), weights), 3),
                "pick_score_delta": round(
                    _weighted_mean(group["observed_pick_score"].astype(float), weights)
                    - _weighted_mean(group["expected_pick_score"].astype(float), weights),
                    3,
                ),
                "value_z_score": round(float(z_score), 3),
                "confidence": "wide",
                "evidence_level": "explore",
                "model_version": MODEL_VERSION,
            }
        )
    return sorted(rollup_rows, key=lambda row: (abs(row["value_z_score"]), abs(row["excess_residual"])), reverse=True)[
        :limit
    ]


def _grade_outcomes(df, features) -> dict[tuple, tuple[int, int]]:
    """(manager_key, feature_type, feature_value) -> (hits, busts) from each
    matching pick's own draft grade. This is how we say 'worked / blew up'
    in plain terms without a LAMAR section."""
    import pandas as pd
    from multi_league.transformations.draft.nugget_renderer import grade_bucket

    if not features or "draft_grade" not in df.columns or "manager_key" not in df.columns:
        return {}
    events = pd.DataFrame(features)
    base = df[["manager_key", "draft_grade"]].reset_index(names="row_index")
    events = events.merge(base, on="row_index", how="inner").dropna(subset=["draft_grade"])
    if events.empty:
        return {}
    events["bucket"] = events["draft_grade"].map(grade_bucket)
    out: dict[tuple, tuple[int, int]] = {}
    for keys, grp in events.groupby(["manager_key", "feature_type", "feature_value"], dropna=False):
        counts = grp["bucket"].value_counts()
        out[keys] = (int(counts.get("hit", 0)), int(counts.get("bust", 0)))
    return out


def _annotate_with_registry(
    rows: list[dict[str, Any]],
    selection: RegistrySelection,
    outcomes: dict[tuple, tuple[int, int]] | None = None,
) -> list[dict[str, Any]]:
    """Attach product language + legibility + a rendered plain-language
    nugget (nugget contract: the sentence is the product; stats stay on the
    row for hovers)."""
    from multi_league.transformations.draft.nugget_renderer import (
        outcome_clause,
        render_nugget,
        surface_score,
    )

    outcomes = outcomes or {}
    for row in rows:
        meta = selection.lookup(str(row.get("feature_type", "")))
        if meta:
            row["feature_label"] = meta["feature_label"]
            row["legibility"] = meta["legibility"]
            row["importance"] = meta["importance"]
        rendered = render_nugget(row)
        if rendered:
            row.update(rendered)
        row.update(surface_score(row))
        # No sentence = no airtime, regardless of evidence.
        if "nugget_headline" not in row:
            row["surfaced"] = False
            continue
        # Outcome verdict — only for OVER-drafting tendencies (the manager
        # actually made these picks) on the manager scope.
        over_drafts = max(
            float(row.get("lift", 1) or 1), float(row.get("count_lift", 1) or 1)
        ) >= 1.2
        if row.get("scope_type") == "manager" and over_drafts:
            key = (row.get("scope_key"), row.get("feature_type"), row.get("feature_value"))
            counts = outcomes.get(key)
            if counts:
                clause = outcome_clause(*counts)
                if clause:
                    row["outcome_hits"], row["outcome_busts"] = counts
                    headline = row["nugget_headline"].rstrip(".")
                    row["nugget_headline"] = f"{headline}, {clause}."

    # Different columns can render the IDENTICAL sentence (nfl_team twins,
    # receiving_yards vs pts_rec_yd) — same sentence = same nugget. Keep the
    # strongest, silence the rest.
    best_by_headline: dict[tuple, dict[str, Any]] = {}
    for row in rows:
        if not row.get("surfaced") or "nugget_headline" not in row:
            continue
        key = (row.get("scope_key"), row["nugget_headline"])
        current = best_by_headline.get(key)
        if current is None:
            best_by_headline[key] = row
        elif row.get("surface_score", 0) > current.get("surface_score", 0):
            current["surfaced"] = False
            best_by_headline[key] = row
        else:
            row["surfaced"] = False
    return rows


def _mine_dataframe(
    df,
    selection: RegistrySelection,
    *,
    min_support: int,
    min_picks: int,
    min_years: int,
    min_abs_z: float,
    limit: int,
) -> dict[str, list[dict[str, Any]]]:
    features = build_wide_features(df, min_support=min_support, selection=selection)
    outcomes = _grade_outcomes(df, features)
    return {
        "manager_tendencies": _annotate_with_registry(
            _score_manager_affinities(
                df,
                features,
                min_picks=min_picks,
                min_abs_z=min_abs_z,
                limit=limit,
            ),
            selection,
            outcomes,
        ),
        "league_inefficiencies": _annotate_with_registry(
            _score_league_inefficiencies(
                df,
                features,
                min_picks=min_picks,
                min_years=min_years,
                min_abs_z=min_abs_z,
                limit=limit,
            ),
            selection,
        ),
    }


FLEET_TIMING_POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF")


def _similar_rules_cohort(
    flags_by_db: dict[str, dict[str, bool]] | None,
    db_name: str,
    candidates,
    *,
    min_fleet_leagues: int,
):
    """Restrict comparisons to leagues with SIMILAR RULES (research-mode
    style): a superflex league drafts QBs early by rule, so comparing it
    against 1QB leagues is a confounder, not a nugget. Cohort widens
    (drop tep, then idp, then superflex) when too thin.

    Returns (cohort_df, similar_rules: bool)."""
    if not flags_by_db or db_name not in flags_by_db:
        return candidates, False
    target = flags_by_db[db_name]

    def matches(keys):
        keep = [
            db for db in candidates["db_name"]
            if db in flags_by_db
            and all(flags_by_db[db].get(k, False) == target.get(k, False) for k in keys)
        ]
        return candidates[candidates["db_name"].isin(keep)]

    for keys in (("superflex", "idp", "tep"), ("superflex", "idp"), ("superflex",)):
        cohort = matches(keys)
        if len(cohort) >= min_fleet_leagues:
            return cohort, True
    return candidates, False


def _fleet_signals_from_frame(
    fleet_df,
    db_name: str,
    flags_by_db: dict[str, dict[str, bool]] | None = None,
    *,
    min_fleet_leagues: int = 100,
    min_years: int = 3,
) -> list[dict[str, Any]]:
    """League-vs-fleet personality: where does THIS league's draft timing sit
    among comparable leagues? Copy stays GENERIC (no fleet counts, no
    percentages — we don't advertise how much data we have); the exact
    percentile and cohort size live on the row for internal use.

    fleet_df columns: db_name, pos, stage (mean pick/max_pick, lower =
    earlier), yrs. Emits signals only at the extremes — 'a bit early on
    RBs' is not a nugget."""
    signals: list[dict[str, Any]] = []
    for pos in FLEET_TIMING_POSITIONS:
        pos_df = fleet_df[fleet_df["pos"] == pos]
        target = pos_df[pos_df["db_name"] == db_name]
        if target.empty:
            continue
        target_row = target.iloc[0]
        if int(target_row["yrs"]) < min_years:
            continue
        others = pos_df[pos_df["db_name"] != db_name]
        others, similar_rules = _similar_rules_cohort(
            flags_by_db, db_name, others, min_fleet_leagues=min_fleet_leagues
        )
        if len(others) < min_fleet_leagues:
            continue
        stage = float(target_row["stage"])
        pct_earlier = float((others["stage"] < stage).mean())  # share drafting EARLIER
        n = len(others)
        pos_word = {"K": "kickers", "DEF": "defenses"}.get(pos, f"{pos}s")
        cohort_phrase = "leagues with similar rules" if similar_rules else "other leagues"
        if pct_earlier <= 0.08:
            qualifier = (
                f"earlier than almost any {cohort_phrase.replace('leagues', 'league')} we've seen"
                if pct_earlier <= 0.03
                else f"much earlier than most {cohort_phrase}"
            )
            headline = f"This league drafts {pos_word} {qualifier}."
        elif pct_earlier >= 0.92:
            qualifier = (
                f"longer than almost any {cohort_phrase.replace('leagues', 'league')} we've seen"
                if pct_earlier >= 0.97
                else f"much longer than most {cohort_phrase}"
            )
            headline = f"This league waits on {pos_word} {qualifier}."
        else:
            continue
        extremity = abs(pct_earlier - 0.5) * 2
        signals.append({
            "db_name": db_name,
            "scope_type": "league_fleet",
            "scope_key": db_name,
            "scope_label": db_name,
            "feature_type": "fleet.position_timing",
            "feature_value": pos,
            "feature_label": "Draft Timing vs Fleet",
            "legibility": "A",
            "importance": 3,
            "similar_rules_cohort": similar_rules,
            "fleet_leagues": n,
            "fleet_percentile_earlier": round(pct_earlier, 4),
            "years_seen": int(target_row["yrs"]),
            "picks": int(target_row.get("picks", 0) or 0),
            "nugget_headline": headline,
            "nugget_evidence": (
                f"{int(target_row['yrs'])} drafted seasons, compared with "
                + cohort_phrase
            ),
            "surface_score": round(8.0 * extremity, 3),
            "surfaced": True,
            "confidence": "wide",
            "evidence_level": "explore",
            "model_version": MODEL_VERSION,
        })
    return signals


def run_league_fleet_personality(db_name: str) -> list[dict[str, Any]]:
    """Fleet-relative league timing (Fly path only — needs every league)."""
    import pandas as pd
    from multi_league.core.readers.fly_reader import FlyReader

    sql = """
    WITH picks AS (
        SELECT
            db_name,
            year,
            CASE
                WHEN UPPER(COALESCE(position, '')) IN ('DEF', 'D/ST', 'DST', 'D') THEN 'DEF'
                WHEN UPPER(COALESCE(position, '')) IN ('K', 'PK') THEN 'K'
                ELSE UPPER(COALESCE(position, ''))
            END AS pos,
            pick,
            MAX(pick) OVER (PARTITION BY db_name, year) AS max_pick
        FROM ___leagues.public.draft
        WHERE COALESCE(is_keeper, 0) = 0
          AND COALESCE(pick, 0) > 0
          AND year IS NOT NULL
    )
    SELECT db_name, pos,
           AVG(pick * 1.0 / max_pick) AS stage,
           COUNT(DISTINCT year) AS yrs,
           COUNT(*) AS picks
    FROM picks
    WHERE pos IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF') AND max_pick > 1
    GROUP BY db_name, pos
    """
    reader = FlyReader()
    rows = reader.query(sql, database="___leagues")
    fleet_df = pd.DataFrame(rows)
    if fleet_df.empty:
        return []

    flags_by_db: dict[str, dict[str, bool]] = {}
    try:
        flags_sql = """
        WITH latest AS (
            SELECT db_name, MAX(year) AS year FROM public.league_settings GROUP BY db_name
        )
        SELECT s.db_name,
               COALESCE(s."roster_SUPER_FLEX", 0) AS sf_slots,
               COALESCE(s."roster_QB", 0) AS qb_slots,
               COALESCE(s."roster_LB", 0) + COALESCE(s."roster_DL", 0)
                 + COALESCE(s."roster_DB", 0) + COALESCE(s."roster_IDP", 0)
                 + COALESCE(s."roster_DB_LB", 0) + COALESCE(s."roster_DL_LB", 0) AS idp_slots,
               COALESCE(s.scoring_bonus_rec_te, 0) AS te_bonus
        FROM public.league_settings s
        JOIN latest l ON s.db_name = l.db_name AND s.year = l.year
        """
        for row in reader.query(flags_sql, database="___leagues"):
            flags_by_db[str(row["db_name"])] = {
                "superflex": float(row.get("sf_slots") or 0) > 0 or float(row.get("qb_slots") or 0) >= 2,
                "idp": float(row.get("idp_slots") or 0) > 0,
                "tep": float(row.get("te_bonus") or 0) > 0,
            }
    except Exception as exc:  # pragma: no cover - cohort falls back to all leagues
        log(f"[wide-miner] fleet rule flags unavailable: {exc}")

    return _fleet_signals_from_frame(fleet_df, db_name, flags_by_db or None)


def run_wide_correlation_miner(
    conn,
    db_name: str,
    *,
    min_support: int = 4,
    min_picks: int = 6,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 200,
    max_features: int = 300,
) -> dict[str, list[dict[str, Any]]]:
    """Run registry-contracted manager and league-wide discovery for one league."""

    configure_table_catalog(conn)
    settings_row = fetch_league_settings_row(
        lambda sql: conn.execute(sql).fetchdf(), db_name
    )
    selection = load_registry_selection(settings_row, max_features=max_features)
    df = conn.execute(build_wide_base_sql(db_name, selection)).fetchdf()
    if df.empty:
        return {"manager_tendencies": [], "league_inefficiencies": []}
    return _mine_dataframe(
        df, selection,
        min_support=min_support, min_picks=min_picks, min_years=min_years,
        min_abs_z=min_abs_z, limit=limit,
    )


def run_wide_correlation_miner_fly(
    db_name: str,
    *,
    min_support: int = 4,
    min_picks: int = 6,
    min_years: int = 2,
    min_abs_z: float = 2.0,
    limit: int = 200,
    max_features: int = 300,
) -> dict[str, list[dict[str, Any]]]:
    """Run the wide miner using the read-only Fly API."""
    import pandas as pd
    from multi_league.core.readers.fly_reader import FlyReader

    reader = FlyReader()

    def fetch_df(sql: str):
        return pd.DataFrame(reader.query(sql, database="___leagues"))

    settings_row = fetch_league_settings_row(fetch_df, db_name)
    selection = load_registry_selection(settings_row, max_features=max_features)
    prune_selection_to_live_schema(fetch_df, selection)
    df = fetch_df(build_wide_base_sql(db_name, selection))
    if df.empty:
        return {"manager_tendencies": [], "league_inefficiencies": [], "league_vs_fleet": []}
    result = _mine_dataframe(
        df, selection,
        min_support=min_support, min_picks=min_picks, min_years=min_years,
        min_abs_z=min_abs_z, limit=limit,
    )
    try:
        result["league_vs_fleet"] = run_league_fleet_personality(db_name)
    except Exception as exc:  # pragma: no cover - fleet pass is additive
        log(f"[wide-miner] fleet personality skipped for {db_name}: {exc}")
        result["league_vs_fleet"] = []
    try:
        from multi_league.transformations.draft.construction_miner import (
            run_construction_miner_fly,
        )
        result["construction_tendencies"] = run_construction_miner_fly(db_name)
    except Exception as exc:  # pragma: no cover - construction pass is additive
        log(f"[wide-miner] construction miner skipped for {db_name}: {exc}")
        result["construction_tendencies"] = []
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Mine wide-column draft correlations.")
    parser.add_argument("--context", type=Path, help="Path to league_context.json")
    parser.add_argument("--db", help="League db_name")
    parser.add_argument("--data-dir", default=None, help="Local data dir for DuckDB-backed runs")
    parser.add_argument("--min-support", type=int, default=4)
    parser.add_argument("--min-picks", type=int, default=6)
    parser.add_argument("--min-years", type=int, default=2)
    parser.add_argument("--min-abs-z", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--max-features", type=int, default=300,
                        help="Cap on registry feature concepts per run (tier A/B first)")
    parser.add_argument("--json", action="store_true", help="Print rows as JSON")
    args = parser.parse_args()

    db_name, _ = resolve_db_name(args)
    conn = None
    try:
        if args.data_dir:
            conn = get_pipeline_connection(db_name, data_dir=args.data_dir, qualified=True)
            result = run_wide_correlation_miner(
                conn,
                db_name,
                min_support=args.min_support,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
                max_features=args.max_features,
            )
        else:
            result = run_wide_correlation_miner_fly(
                db_name,
                min_support=args.min_support,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
                max_features=args.max_features,
            )

        if args.json:
            print(json.dumps(result, indent=2, default=str))
        else:
            print(
                f"manager_tendencies={len(result['manager_tendencies'])} "
                f"league_inefficiencies={len(result['league_inefficiencies'])}"
            )
            for row in result["manager_tendencies"][:10]:
                print(
                    f"T {row['scope_label']} {row['feature_type']}={row['feature_value']} "
                    f"z={row['capital_z_score']} repeat={row['repeatability']} picks={row['picks']}"
                )
            for row in result["league_inefficiencies"][:10]:
                print(
                    f"I {row['feature_type']}={row['feature_value']} "
                    f"z={row['value_z_score']} repeat={row['repeatability']} delta={row['excess_residual']}"
                )
        return 0
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    sys.exit(main())
