"""Rebuild NFL season/career aggregate fast-path tables in Fly DuckDB.

The weekly super table is the source of truth. These tables are derived caches:

* player_nfl_season      - regular season, grouped by NFL_player_id/year
* player_nfl_season_all  - regular + postseason, grouped by NFL_player_id/year
* player_nfl_career      - regular season, grouped by NFL_player_id
* player_nfl_career_all  - regular + postseason, grouped by NFL_player_id
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path

import requests

from multi_league.core.fly_writer import FlyWriter
from multi_league.data_fetchers.fantasy_points_calculator import (
    ALLTIME_FLEX_RANK_COLUMNS,
    ALLTIME_POSITION_RANK_COLUMNS,
    SEASON_FLEX_RANK_COLUMNS,
    SEASON_POSITION_RANK_COLUMNS,
)

SUPER_TABLE = "___ops.nfl_historical.nfl_player_stats_all"
SCHEMA = "___ops.nfl_historical"
PUBLIC = "___ops.public"

SEASON_TABLE = f"{SCHEMA}.player_nfl_season"
SEASON_ALL_TABLE = f"{SCHEMA}.player_nfl_season_all"
CAREER_TABLE = f"{SCHEMA}.player_nfl_career"
CAREER_ALL_TABLE = f"{SCHEMA}.player_nfl_career_all"
SEASON_FACT_TABLE = f"{SCHEMA}.nfl_player_season_stat_facts"

ROLLUP_DATE = os.environ.get("NFL_ROLLUP_DATE") or datetime.now(UTC).strftime("%Y%m%d")

NUMERIC_TYPES = {
    "BIGINT",
    "DOUBLE",
    "FLOAT",
    "HUGEINT",
    "INTEGER",
    "REAL",
    "SMALLINT",
    "TINYINT",
    "UBIGINT",
    "UINTEGER",
    "USMALLINT",
    "UTINYINT",
}

PPG_VARIANTS = [
    ("4pt", "0ppr", "fpts_4pt_0ppr"),
    ("4pt", "half", "fpts_4pt_half"),
    ("4pt", "ppr", "fpts_4pt_ppr"),
    ("5pt", "0ppr", "fpts_5pt_0ppr"),
    ("5pt", "half", "fpts_5pt_half"),
    ("5pt", "ppr", "fpts_5pt_ppr"),
    ("6pt", "0ppr", "fpts_6pt_0ppr"),
    ("6pt", "half", "fpts_6pt_half"),
    ("6pt", "ppr", "fpts_6pt_ppr"),
    ("4pt", "tep", "fpts_4pt_tep"),
    ("5pt", "tep", "fpts_5pt_tep"),
    ("6pt", "tep", "fpts_6pt_tep"),
    ("4pt", "ppfd", "fpts_4pt_ppfd"),
    ("5pt", "ppfd", "fpts_5pt_ppfd"),
    ("6pt", "ppfd", "fpts_6pt_ppfd"),
]

CONSISTENCY_EPSILON = 1e-9

# Aggregation-semantics declarations (rate/weighted/max/per-game sets and their component maps)
# now live in the ONE stat contract registry -- scripts/sota_recon/witness_gate/contracts/
# stat_contracts.v1.json (SOTA master plan §6, switched 2026-07-25). The generator proves parity
# against the previous inline sets before writing, so these loads are behavior-identical. The
# rationale comments that used to sit on each set (the 2026-06-29 recompute-never-average note,
# the Welker-2008 share-garbage note, the Kupp-2021 NGS 17x-inflation receipt) are preserved as
# adjudication receipts inside the registry.
from multi_league.core.stat_contracts_loader import aggregation_sets as _stat_contract_sets

_SETS = _stat_contract_sets()

PER_GAME_AVG_COLS: dict[str, str] = _SETS.per_game_avg_cols

DERIVED_RATE_COLS = _SETS.derived_rate_cols

# Share-family columns need the team_vol CTE joined into the aggregation query.
SHARE_FAMILY_COLS = _SETS.share_family_cols
TEAM_VOLUME_ALIAS = "tv"
# A team-year's volume only counts toward share denominators if the team tracked the volume in
# at least GREATEST(SHARE_TRACKED_MIN_GAMES, SHARE_TRACKED_MIN_FRACTION * its games) that year.
# The absolute floor keeps the CURRENT season speakable from week 3 of an in-season rebuild;
# the fraction kills partial-tracking noise: the 14 pre-1978 stray games (1-2 targets each)
# and the 1999-2008 partial air-yards seasons (~1-5 tracked games/team), which minted
# impossible season shares (target_share=1.0, air_yards_share>1 on 150-target players).
SHARE_TRACKED_MIN_GAMES = 3
SHARE_TRACKED_MIN_FRACTION = 0.5
# Air yards 1999-2008 are also partial WITHIN games (values for ~1 targeted player per game),
# which game-count gates cannot see: the team denominator sums to a sliver of the true total
# and mints shares near/above 1.0 (Keyshawn 2002 = 1.036). A year's air yards only count once
# at least this fraction of targeted players in that year carry a nonzero value (12% in
# 1999-2002 -> gated; ~96% from 2009 -> speakable).
SHARE_AIR_YEAR_MIN_COVERAGE = 0.5

# shares divide the player's summed volume by the team's summed volume (team_vol CTE);
# the weekly share columns are intentionally NOT dependencies (see the registry share notes).
DERIVED_RATE_DEPENDENCIES = _SETS.derived_rate_dependencies

WEIGHTED_AVG_COLS = _SETS.weighted_avg_cols

MAX_COLS = _SETS.max_cols

PBP_SCORING_AGG_COLS = {
    "completions_40plus",
    "completions_50plus",
    "passing_tds_40plus",
    "passing_tds_50plus",
    "receptions_0_4",
    "receptions_5_9",
    "receptions_10_19",
    "receptions_20_29",
    "receptions_30_39",
    "receptions_40plus",
    "receiving_tds_40plus",
    "receiving_tds_50plus",
    "special_teams_tackles_solo",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
    "fum_rec_yds",
}

# Precomputed weekly NFL totals that must be promoted into the season/career
# fast-path tables even when those older table schemas do not contain them yet.
REQUIRED_AGGREGATE_COLS = {
    "fumbles_lost",
    "def_blk_kick",
    "fg_made_60_plus_canonical",
}

# These totals are already canonical at player-game grain in the super table.
# They must be collapsed to one value per player/week before season or career
# aggregation; summing raw duplicate source rows would overstate the total.
CANONICAL_WEEKLY_TOTAL_COLS = {
    "fumbles_lost",
    "fg_made_60_plus_canonical",
}

SPECIAL_PREFIXES = (
    "avg_pts_next_year_",
    "consistency_",
    "lamar_ppg_",
    "ppg_",
    "rank_",
    "rolling_",
    "weighted_ppg_",
)

SPECIAL_NAMES = {
    "nfl_franchise_number",
    "opponent_nfl_franchise_number",
    "week",
    "year",
}


class LongFlyWriter(FlyWriter):
    """Fly writer with a longer timeout for aggregate rebuild queries."""

    def execute(self, sql: str, database: str = "___leagues") -> list[dict]:
        timeout = int(os.environ.get("FLY_QUERY_TIMEOUT_SECONDS", "600"))
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self.url}/query-rw",
                    json={"sql": sql, "database": database},
                    headers=self._headers(),
                    timeout=timeout,
                )
            except self.RETRY_EXCEPTIONS:
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self._retry_delay(attempt))
                    continue
                raise

            if resp.status_code in self.RETRY_STATUS and attempt < self.MAX_RETRIES - 1:
                time.sleep(self._retry_delay(attempt))
                continue

            if resp.status_code != 200:
                raise RuntimeError(f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'}")

            return resp.json()

        raise RuntimeError("Query exhausted retries")


class LocalDuckDBWriter:
    """Minimal FlyWriter-compatible adapter for a writable attached ``___ops`` file.

    The aggregate SQL intentionally keeps its production-qualified relation
    names (``___ops.nfl_historical.*``).  A local builder attaches the complete
    candidate artifact under that catalog name, so the exact same aggregation
    queries can be verified before any file is promoted to Fly.
    """

    def __init__(self, connection):
        self.connection = connection

    def execute(self, sql: str, database: str = "___ops") -> list[dict]:
        del database
        cursor = self.connection.execute(sql)
        if not cursor.description:
            return []
        columns = [str(column[0]) for column in cursor.description]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    ordinal_position: int


@dataclass(frozen=True)
class RankSpec:
    col: str
    scope: str
    positions: tuple[str, ...]
    points_col: str

    @property
    def cte_name(self) -> str:
        return f"r_{self.col}"


def load_env() -> None:
    root = Path(__file__).resolve().parents[3]
    env_path = root / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def q(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got {raw!r}") from exc
    return max(1, value)


def distinct_text_list_expr(col: str) -> str:
    clean = f"NULLIF(TRIM(CAST({col} AS VARCHAR)), '')"
    return f"STRING_AGG(DISTINCT {clean}, ', ' ORDER BY {clean})"


def distinct_text_count_expr(col: str) -> str:
    clean = f"NULLIF(TRIM(CAST({col} AS VARCHAR)), '')"
    return f"COUNT(DISTINCT {clean})"


def fetch_rows(writer: FlyWriter, sql: str) -> list[dict]:
    return writer.execute(sql, database="___ops")


def fetch_scalar(writer: FlyWriter, sql: str, key: str = "n") -> int:
    rows = fetch_rows(writer, sql)
    return int(rows[0].get(key) or 0) if rows else 0


def table_exists(writer: FlyWriter, full_name: str) -> bool:
    schema_name, table_name = full_name.split(".")[-2:]
    return bool(
        fetch_scalar(
            writer,
            f"""
            SELECT COUNT(*) AS n
            FROM information_schema.tables
            WHERE table_catalog = '___ops'
              AND table_schema = {q(schema_name)}
              AND table_name = {q(table_name)}
            """,
        )
    )


def fetch_columns(writer: FlyWriter, table_name: str) -> list[ColumnInfo]:
    schema_name, bare_table = table_name.split(".")[-2:]
    rows = fetch_rows(
        writer,
        f"""
        SELECT column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_catalog = '___ops'
          AND table_schema = {q(schema_name)}
          AND table_name = {q(bare_table)}
        ORDER BY ordinal_position
        """,
    )
    return [
        ColumnInfo(
            name=str(row["column_name"]),
            data_type=str(row["data_type"]).upper(),
            ordinal_position=int(row["ordinal_position"]),
        )
        for row in rows
    ]


def is_numeric(col: ColumnInfo) -> bool:
    dtype = col.data_type.upper()
    return dtype in NUMERIC_TYPES or dtype.startswith("DECIMAL")


def is_special_weekly_col(name: str) -> bool:
    if name in SPECIAL_NAMES or name in DERIVED_RATE_COLS:
        return True
    if name.startswith(SPECIAL_PREFIXES):
        return True
    if name.startswith("is_") or name.startswith("has_"):
        return True
    if name == "pts_def_high":
        return True
    return False


def is_extra_scoring_col(name: str) -> bool:
    return name.startswith("pts_") or name.startswith("fpts_") or name.startswith("bonus_")


def discover_aggregate_columns(writer: FlyWriter) -> tuple[list[str], list[str], list[str], list[str]]:
    weekly_cols = fetch_columns(writer, SUPER_TABLE)
    weekly_by_name = {col.name: col for col in weekly_cols}
    weekly_numeric = {col.name for col in weekly_cols if is_numeric(col)}

    # The weekly super table is the source of truth.  Previously this started from the
    # intersection with the *old* season/career schemas, which meant a newly promoted
    # sortable stat could exist weekly yet never be materialized into either fast path.
    # Keep the semantic exclusions below, but discover every aggregatable numeric weekly
    # column so schema evolution cannot silently create a binder 503 in research mode.
    old_numeric = [
        col.name
        for col in weekly_cols
        if col.name in weekly_numeric and not is_special_weekly_col(col.name) and not col.name.startswith("lamar_")
    ]

    extra_scoring = [
        col.name
        for col in weekly_cols
        if col.name in weekly_numeric and is_extra_scoring_col(col.name) and not is_special_weekly_col(col.name)
    ]
    pbp_scoring = [
        col.name
        for col in weekly_cols
        if col.name in weekly_numeric and col.name in PBP_SCORING_AGG_COLS and not is_special_weekly_col(col.name)
    ]
    required_aggregates = [
        col.name
        for col in weekly_cols
        if col.name in weekly_numeric
        and col.name in REQUIRED_AGGREGATE_COLS
        and not is_special_weekly_col(col.name)
    ]

    derived_rates = [
        col for col, deps in DERIVED_RATE_DEPENDENCIES.items() if all(dep in weekly_numeric for dep in deps)
    ]
    weighted_avgs = [col for col in WEIGHTED_AVG_COLS if col in weekly_numeric]
    per_game_avgs = [col for col in PER_GAME_AVG_COLS if col in weekly_numeric]

    derived_order = {name: idx for idx, name in enumerate(sorted(DERIVED_RATE_COLS))}

    def aggregate_sort_key(name: str) -> int:
        weekly_col = weekly_by_name.get(name)
        if weekly_col is not None:
            return weekly_col.ordinal_position
        return 100_000 + derived_order.get(name, 0)

    aggregate_cols = sorted(
        set(old_numeric)
        | set(extra_scoring)
        | set(pbp_scoring)
        | set(required_aggregates)
        | set(derived_rates)
        | set(weighted_avgs)
        | set(per_game_avgs),
        key=aggregate_sort_key,
    )

    lamar_cols = [
        col.name
        for col in weekly_cols
        if col.name.startswith("lamar_") and not col.name.startswith("lamar_ppg_") and col.name in weekly_numeric
    ]
    fpts_cols = [col.name for col in weekly_cols if col.name.startswith("fpts_") and col.name in weekly_numeric]
    bonus_cols = [col.name for col in weekly_cols if col.name.startswith("bonus_") and col.name in weekly_numeric]
    return aggregate_cols, lamar_cols, fpts_cols, bonus_cols


def stage_table_for(table: str) -> str:
    return f"{PUBLIC}.{table.split('.')[-1]}_stage_{ROLLUP_DATE}"


def base_stage_table_for(table: str) -> str:
    return f"{PUBLIC}.{table.split('.')[-1]}_base_stage_{ROLLUP_DATE}"


def chunk_stage_table_for(table: str, kind: str, idx: int = 0) -> str:
    suffix = f"{kind}_{idx}" if idx else kind
    return f"{PUBLIC}.{table.split('.')[-1]}_{suffix}_stage_{ROLLUP_DATE}"


def ranked_stage_table_for(table: str) -> str:
    return f"{PUBLIC}.{table.split('.')[-1]}_ranked_stage_{ROLLUP_DATE}"


def aggregate_stage_table_for(table: str) -> str:
    return f"{PUBLIC}.{table.split('.')[-1]}_agg_stage_{ROLLUP_DATE}"


def ppg_cols_for_variant(td: str, ppr: str) -> tuple[str, str, str, str]:
    return (
        f"ppg_season_{td}_{ppr}",
        f"consistency_{td}_{ppr}",
        f"weighted_ppg_{td}_{ppr}",
        f"avg_pts_next_year_{td}_{ppr}",
    )


def ppg_alltime_col(td: str, ppr: str) -> str:
    return f"ppg_alltime_{td}_{ppr}"


def consistency_expr(points_col: str) -> str:
    value = f"CAST({q_ident(points_col)} AS DOUBLE)"
    avg = f"AVG({value})"
    return f"CASE WHEN {avg} > {CONSISTENCY_EPSILON} THEN ROUND(COALESCE(STDDEV({value}), 0) / {avg}, 3) ELSE 0 END"


def weighted_expr(points_col: str) -> str:
    terms = []
    weights = []
    for rn in range(1, 6):
        weight = 6 - rn
        terms.append(f"CASE WHEN rn_desc = {rn} THEN COALESCE({q_ident(points_col)}, 0) * {weight} ELSE 0 END")
        weights.append(f"CASE WHEN rn_desc = {rn} AND {q_ident(points_col)} IS NOT NULL THEN {weight} ELSE 0 END")
    return f"ROUND(SUM({' + '.join(terms)}) / NULLIF(SUM({' + '.join(weights)}), 0), 2)"


def col_ref(col: str, source_alias: str | None = None) -> str:
    ident = q_ident(col)
    return f"{source_alias}.{ident}" if source_alias else ident


def adjusted_sum_expr(col: str, source_alias: str | None = None, adjustment_alias: str | None = None) -> str:
    base = f"SUM(COALESCE({col_ref(col, source_alias)}, 0))"
    if adjustment_alias:
        return f"({base} + COALESCE(MAX({adjustment_alias}.{q_ident(col)}), 0))"
    return base


def _team_share_expr(volume_col: str, team_col: str, source_alias: str | None = None, rounded: bool = True) -> str:
    """Season/career share = Sigma(player volume) / Sigma(team volume) over the player's games,
    reading the team denominator from the team_vol CTE join (see team_volume_cte_sql). Games where
    the team volume is untracked or gated (team_col <= 0) contribute to neither side, so
    pre-availability eras yield NULL while tracked eras yield the true share -- including a real
    0.0 for a player with tracked team volume and none of his own."""
    v = f"COALESCE(TRY_CAST({col_ref(volume_col, source_alias)} AS DOUBLE), 0)"
    tv = f"{TEAM_VOLUME_ALIAS}.{q_ident(team_col)}"
    num = f"SUM(CASE WHEN {tv} > 0 THEN {v} ELSE 0 END)"
    den = f"SUM(CASE WHEN {tv} > 0 THEN {tv} ELSE 0 END)"
    ratio = f"CAST({num} AS DOUBLE) / {den}"
    inner = f"ROUND({ratio}, 4)" if rounded else ratio
    return f"CASE WHEN {den} > 0 THEN {inner} ELSE NULL END"


def team_volume_cte_sql(*, include_playoffs: bool, year: int | None = None) -> str:
    """CTEs providing per-team-game volume sums for the share family, gated so a team-year only
    counts once it has SHARE_TRACKED_MIN_GAMES games with tracked volume (kills stray pre-1978
    single-game rows and one-game partial tracking that would otherwise mint garbage shares).
    The filter must match the aggregation's build_filtered_where so numerator rows and
    denominator rows describe the same population."""
    where = build_filtered_where(include_playoffs, year)
    return f"""
    team_vol_raw AS (
      SELECT
        nfl_team,
        year,
        week,
        season_type,
        SUM(COALESCE(TRY_CAST(targets AS DOUBLE), 0)) AS team_targets,
        SUM(COALESCE(TRY_CAST(receiving_air_yards AS DOUBLE), 0)) AS team_air_yards
      FROM {SUPER_TABLE}
      WHERE {where} AND nfl_team IS NOT NULL
      GROUP BY nfl_team, year, week, season_type
    ),
    team_tracked AS (
      SELECT
        nfl_team,
        year,
        COUNT(CASE WHEN team_targets > 0 THEN 1 END)
          >= GREATEST({SHARE_TRACKED_MIN_GAMES}, {SHARE_TRACKED_MIN_FRACTION} * COUNT(*)) AS targets_tracked,
        COUNT(CASE WHEN team_air_yards > 0 THEN 1 END)
          >= GREATEST({SHARE_TRACKED_MIN_GAMES}, {SHARE_TRACKED_MIN_FRACTION} * COUNT(*)) AS air_yards_tracked
      FROM team_vol_raw
      GROUP BY nfl_team, year
    ),
    year_air_coverage AS (
      SELECT
        year,
        COUNT(CASE WHEN COALESCE(TRY_CAST(targets AS DOUBLE), 0) > 0 THEN 1 END) AS target_rows,
        COUNT(CASE WHEN COALESCE(TRY_CAST(targets AS DOUBLE), 0) > 0
                    AND COALESCE(TRY_CAST(receiving_air_yards AS DOUBLE), 0) <> 0 THEN 1 END) AS air_rows
      FROM {SUPER_TABLE}
      WHERE {where} AND nfl_team IS NOT NULL
      GROUP BY year
    ),
    team_vol AS (
      SELECT
        v.nfl_team,
        v.year,
        v.week,
        v.season_type,
        CASE WHEN t.targets_tracked THEN v.team_targets ELSE 0 END AS team_targets,
        CASE WHEN t.air_yards_tracked
              AND yc.target_rows > 0
              AND yc.air_rows >= {SHARE_AIR_YEAR_MIN_COVERAGE} * yc.target_rows
             THEN v.team_air_yards ELSE 0 END AS team_air_yards
      FROM team_vol_raw AS v
      JOIN team_tracked AS t
        ON t.nfl_team = v.nfl_team AND t.year = v.year
      JOIN year_air_coverage AS yc
        ON yc.year = v.year
    )
    """


def team_volume_join_sql(source_alias: str) -> str:
    a = source_alias
    tv = TEAM_VOLUME_ALIAS
    return (
        f"LEFT JOIN team_vol AS {tv} ON {tv}.nfl_team = {a}.nfl_team "
        f"AND {tv}.year = {a}.year AND {tv}.week = {a}.week AND {tv}.season_type = {a}.season_type"
    )


def derived_rate_expr(col: str, source_alias: str | None = None, adjustment_alias: str | None = None) -> str:
    if col == "fg_pct":
        made = adjusted_sum_expr("fg_made", source_alias, adjustment_alias)
        att = adjusted_sum_expr("fg_att", source_alias, adjustment_alias)
        return f"CASE WHEN {att} > 0 THEN ROUND(CAST({made} AS DOUBLE) / {att}, 4) ELSE NULL END"
    if col == "comp_pct":
        comp = adjusted_sum_expr("completions", source_alias, adjustment_alias)
        att = adjusted_sum_expr("attempts", source_alias, adjustment_alias)
        return f"CASE WHEN {att} > 0 THEN ROUND(CAST({comp} AS DOUBLE) / {att}, 4) ELSE NULL END"
    if col == "yards_per_attempt":
        yards = adjusted_sum_expr("passing_yards", source_alias, adjustment_alias)
        att = adjusted_sum_expr("attempts", source_alias, adjustment_alias)
        return f"CASE WHEN {att} > 0 THEN ROUND(CAST({yards} AS DOUBLE) / {att}, 2) ELSE NULL END"
    if col == "pacr":
        yards = adjusted_sum_expr("passing_yards", source_alias, adjustment_alias)
        air = adjusted_sum_expr("passing_air_yards", source_alias, adjustment_alias)
        return f"CASE WHEN {air} <> 0 THEN ROUND(CAST({yards} AS DOUBLE) / {air}, 4) ELSE NULL END"
    if col == "passer_rating":
        comp = adjusted_sum_expr("completions", source_alias, adjustment_alias)
        att = adjusted_sum_expr("attempts", source_alias, adjustment_alias)
        yards = adjusted_sum_expr("passing_yards", source_alias, adjustment_alias)
        tds = adjusted_sum_expr("passing_tds", source_alias, adjustment_alias)
        ints = adjusted_sum_expr("passing_interceptions", source_alias, adjustment_alias)
        return f"""
        CASE WHEN {att} >= 1 THEN ROUND(
          (
            LEAST(2.375, GREATEST(0, (CAST({comp} AS DOUBLE) / {att} - 0.3) * 5))
            + LEAST(2.375, GREATEST(0, (CAST({yards} AS DOUBLE) / {att} - 3) * 0.25))
            + LEAST(2.375, GREATEST(0, (CAST({tds} AS DOUBLE) / {att}) * 20))
            + LEAST(2.375, GREATEST(0, 2.375 - (COALESCE(CAST({ints} AS DOUBLE), 0) / {att} * 25)))
          ) / 6 * 100,
          1
        ) ELSE NULL END
        """.strip()
    if col == "yards_per_carry":
        yards = adjusted_sum_expr("rushing_yards", source_alias, adjustment_alias)
        carries = adjusted_sum_expr("carries", source_alias, adjustment_alias)
        return f"CASE WHEN {carries} > 0 THEN ROUND(CAST({yards} AS DOUBLE) / {carries}, 2) ELSE NULL END"
    if col == "yards_per_reception":
        yards = adjusted_sum_expr("receiving_yards", source_alias, adjustment_alias)
        rec = adjusted_sum_expr("receptions", source_alias, adjustment_alias)
        return f"CASE WHEN {rec} > 0 THEN ROUND(CAST({yards} AS DOUBLE) / {rec}, 2) ELSE NULL END"
    if col == "catch_rate":
        rec = adjusted_sum_expr("receptions", source_alias, adjustment_alias)
        targets = adjusted_sum_expr("targets", source_alias, adjustment_alias)
        return f"CASE WHEN {targets} > 0 THEN ROUND(CAST({rec} AS DOUBLE) / {targets}, 4) ELSE NULL END"
    if col == "racr":
        yards = adjusted_sum_expr("receiving_yards", source_alias, adjustment_alias)
        air = adjusted_sum_expr("receiving_air_yards", source_alias, adjustment_alias)
        return f"CASE WHEN {air} <> 0 THEN ROUND(CAST({yards} AS DOUBLE) / {air}, 4) ELSE NULL END"
    # --- added 2026-06-29: simple num/den efficiency rates (recompute from summed components) ---
    if col == "yards_per_target":
        yards = adjusted_sum_expr("receiving_yards", source_alias, adjustment_alias)
        tgt = adjusted_sum_expr("targets", source_alias, adjustment_alias)
        return f"CASE WHEN {tgt} > 0 THEN ROUND(CAST({yards} AS DOUBLE) / {tgt}, 2) ELSE NULL END"
    if col == "rec_td_pct":
        tds = adjusted_sum_expr("receiving_tds", source_alias, adjustment_alias)
        tgt = adjusted_sum_expr("targets", source_alias, adjustment_alias)
        return f"CASE WHEN {tgt} > 0 THEN ROUND(CAST({tds} AS DOUBLE) / {tgt}, 4) ELSE NULL END"
    if col == "rush_td_pct":
        tds = adjusted_sum_expr("rushing_tds", source_alias, adjustment_alias)
        car = adjusted_sum_expr("carries", source_alias, adjustment_alias)
        return f"CASE WHEN {car} > 0 THEN ROUND(CAST({tds} AS DOUBLE) / {car}, 4) ELSE NULL END"
    if col == "yards_per_touch":
        yards = f"({adjusted_sum_expr('rushing_yards', source_alias, adjustment_alias)} + {adjusted_sum_expr('receiving_yards', source_alias, adjustment_alias)})"
        touches = f"({adjusted_sum_expr('carries', source_alias, adjustment_alias)} + {adjusted_sum_expr('receptions', source_alias, adjustment_alias)})"
        return f"CASE WHEN {touches} > 0 THEN ROUND(CAST({yards} AS DOUBLE) / {touches}, 2) ELSE NULL END"
    if col == "passing_td_pct":
        tds = adjusted_sum_expr("passing_tds", source_alias, adjustment_alias)
        att = adjusted_sum_expr("attempts", source_alias, adjustment_alias)
        return f"CASE WHEN {att} > 0 THEN ROUND(CAST({tds} AS DOUBLE) / {att}, 4) ELSE NULL END"
    if col == "passing_int_pct":
        ints = adjusted_sum_expr("passing_interceptions", source_alias, adjustment_alias)
        att = adjusted_sum_expr("attempts", source_alias, adjustment_alias)
        return f"CASE WHEN {att} > 0 THEN ROUND(CAST({ints} AS DOUBLE) / {att}, 4) ELSE NULL END"
    if col == "adot":
        air = adjusted_sum_expr("passing_air_yards", source_alias, adjustment_alias)
        att = adjusted_sum_expr("attempts", source_alias, adjustment_alias)
        return f"CASE WHEN {att} > 0 THEN ROUND(CAST({air} AS DOUBLE) / {att}, 2) ELSE NULL END"
    if col == "receiving_adot":
        # receiver average depth of target = receiving_air_yards / targets.
        # (The `adot` column above is the QB/passing aDOT and is meaningless on
        #  receiver rows; this is the receiving analogue.)
        air = adjusted_sum_expr("receiving_air_yards", source_alias, adjustment_alias)
        tgt = adjusted_sum_expr("targets", source_alias, adjustment_alias)
        return f"CASE WHEN {tgt} > 0 THEN ROUND(CAST({air} AS DOUBLE) / {tgt}, 2) ELSE NULL END"
    if col == "sack_pct":
        sacks = adjusted_sum_expr("sacks_suffered", source_alias, adjustment_alias)
        att = adjusted_sum_expr("attempts", source_alias, adjustment_alias)
        dropbacks = f"({att} + {sacks})"
        return f"CASE WHEN {dropbacks} > 0 THEN ROUND(CAST({sacks} AS DOUBLE) / {dropbacks}, 4) ELSE NULL END"
    if col == "xp_pct":
        made = adjusted_sum_expr("pat_made", source_alias, adjustment_alias)
        att = adjusted_sum_expr("pat_att", source_alias, adjustment_alias)
        return f"CASE WHEN {att} > 0 THEN ROUND(CAST({made} AS DOUBLE) / {att}, 4) ELSE NULL END"
    # --- share metrics: Sigma(player volume) / Sigma(team volume) via the team_vol CTE join ---
    if col == "target_share":
        return _team_share_expr("targets", "team_targets", source_alias)
    if col == "air_yards_share":
        return _team_share_expr("receiving_air_yards", "team_air_yards", source_alias)
    if col == "wopr":
        ts = _team_share_expr("targets", "team_targets", source_alias, rounded=False)
        ays = _team_share_expr("receiving_air_yards", "team_air_yards", source_alias, rounded=False)
        # wopr = 1.5*target_share + 0.7*air_yards_share; NULL when no target data
        return f"CASE WHEN ({ts}) IS NULL THEN NULL ELSE ROUND(1.5*({ts}) + 0.7*COALESCE(({ays}),0), 4) END"
    raise ValueError(f"Unknown derived rate column: {col}")


def weighted_avg_expr(col: str, weight_col: str, source_alias: str | None = None) -> str:
    value = f"TRY_CAST({col_ref(col, source_alias)} AS DOUBLE)"
    weight = f"TRY_CAST({col_ref(weight_col, source_alias)} AS DOUBLE)"
    denom = (
        f"SUM(CASE WHEN {value} IS NOT NULL AND {weight} IS NOT NULL "
        f"AND isfinite({value}) AND isfinite({weight}) "
        f"THEN {weight} ELSE 0 END)"
    )
    numer = (
        f"SUM(CASE WHEN {value} IS NOT NULL AND {weight} IS NOT NULL "
        f"AND isfinite({value}) AND isfinite({weight}) "
        f"THEN {value} * {weight} ELSE 0 END)"
    )
    return f"CASE WHEN {denom} > 0 THEN ROUND(CAST({numer} AS DOUBLE) / {denom}, 4) ELSE NULL END"


def finite_avg_expr(col: str, source_alias: str | None = None) -> str:
    value = f"TRY_CAST({col_ref(col, source_alias)} AS DOUBLE)"
    return f"AVG(CASE WHEN {value} IS NOT NULL AND isfinite({value}) THEN {value} ELSE NULL END)"


def aggregate_expr(col: str, source_alias: str | None = None, adjustment_alias: str | None = None) -> str:
    if col in DERIVED_RATE_COLS:
        return f"{derived_rate_expr(col, source_alias, adjustment_alias)} AS {q_ident(col)}"
    if col in WEIGHTED_AVG_COLS:
        return f"{weighted_avg_expr(col, WEIGHTED_AVG_COLS[col], source_alias)} AS {q_ident(col)}"
    if col in MAX_COLS:
        return f"MAX({col_ref(col, source_alias)}) AS {q_ident(col)}"
    if col in PER_GAME_AVG_COLS:
        return f"{finite_avg_expr(col, source_alias)} AS {q_ident(col)}"
    return f"ROUND({adjusted_sum_expr(col, source_alias, adjustment_alias)}, 4) AS {q_ident(col)}"


def build_filtered_where(include_playoffs: bool, year: int | None = None, alias: str | None = None) -> str:
    def c(name: str) -> str:
        return col_ref(name, alias)

    phase = f"{c('season_type')} IN ('REG', 'POST')" if include_playoffs else f"{c('season_type')} = 'REG'"
    year_filter = f" AND {c('year')} = {int(year)}" if year is not None else ""
    return f"{c('NFL_player_id')} IS NOT NULL AND {c('year')} IS NOT NULL AND {c('week')} IS NOT NULL AND {phase}{year_filter}"


def season_fact_adjustment_cols(cols: list[str]) -> list[str]:
    """Stats needed from the season-only fact sidecar for this aggregate chunk."""
    needed: set[str] = set()
    for col in cols:
        if col in DERIVED_RATE_COLS:
            needed.update(DERIVED_RATE_DEPENDENCIES[col])
        elif col in WEIGHTED_AVG_COLS or col in MAX_COLS or col in PER_GAME_AVG_COLS:
            continue
        else:
            needed.add(col)
    return sorted(needed)


def season_fact_adjustment_cte(
    cols: list[str],
    *,
    include_playoffs: bool,
    season: bool,
    year: int | None = None,
) -> str:
    key_select = "NFL_player_id, CAST(year AS INTEGER) AS year" if season else "NFL_player_id"
    group_by = "NFL_player_id, year" if season else "NFL_player_id"
    phase = "season_type IN ('REG', 'POST')" if include_playoffs else "season_type = 'REG'"
    year_filter = f" AND year = {int(year)}" if season and year is not None else ""
    stat_filter = ", ".join(q(col) for col in cols)
    selects = ",\n        ".join(
        f"ROUND(SUM(CASE WHEN stat_name = {q(col)} THEN COALESCE(adjustment_value, 0) ELSE 0 END), 4) AS {q_ident(col)}"
        for col in cols
    )
    return f"""
    season_adj AS (
      SELECT
        {key_select},
        {selects}
      FROM {SEASON_FACT_TABLE}
      WHERE arbitration_status = {q("season_only_no_weekly_distribution")}
        AND {phase}
        AND stat_name IN ({stat_filter})
        {year_filter}
      GROUP BY {group_by}
    )
    """


def build_season_base_sql(table: str, *, include_playoffs: bool, year: int | None = None) -> str:
    stage_table = base_stage_table_for(table)
    where = build_filtered_where(include_playoffs, year)
    return f"""
    CREATE OR REPLACE TABLE {stage_table} AS
    WITH filtered AS (
      SELECT
        *,
        CAST(year AS BIGINT) * 1000000
          + CAST(week AS BIGINT) * 1000
          + ROW_NUMBER() OVER (
              PARTITION BY NFL_player_id, year, week
              ORDER BY player_week
            ) AS row_sort
      FROM {SUPER_TABLE}
      WHERE {where}
    )
    SELECT
      NFL_player_id,
      CAST(year AS INTEGER) AS year,
      ARG_MAX(player, row_sort) AS player,
      MODE(position) AS position,
      MODE(COALESCE(nfl_position, position)) AS nfl_position,
      ARG_MAX(nfl_team, row_sort) AS nfl_team,
      {distinct_text_list_expr("nfl_team")} AS nfl_teams,
      CAST({distinct_text_count_expr("nfl_team")} AS INTEGER) AS nfl_team_count,
      CAST(ARG_MAX(nfl_franchise_number, row_sort) AS INTEGER) AS nfl_franchise_number,
      ARG_MAX(headshot_url, row_sort) AS headshot_url,
      CAST(COUNT(*) AS INTEGER) AS games_played,
      CURRENT_TIMESTAMP AS last_updated
    FROM filtered
    GROUP BY NFL_player_id, year
    """


def build_career_base_sql(table: str, *, include_playoffs: bool) -> str:
    stage_table = base_stage_table_for(table)
    where = build_filtered_where(include_playoffs)
    return f"""
    CREATE OR REPLACE TABLE {stage_table} AS
    WITH filtered AS (
      SELECT
        *,
        CAST(year AS BIGINT) * 1000000
          + CAST(week AS BIGINT) * 1000
          + ROW_NUMBER() OVER (
              PARTITION BY NFL_player_id, year, week
              ORDER BY player_week
            ) AS row_sort
      FROM {SUPER_TABLE}
      WHERE {where}
    )
    SELECT
      NFL_player_id,
      ARG_MAX(player, row_sort) AS player,
      -- Career rank population uses the stored primary_position (a player attribute), matching
      -- build_full_ops; falls back to MODE(position) only if primary_position is absent.
      COALESCE(ANY_VALUE(primary_position), MODE(position)) AS position,
      MODE(COALESCE(nfl_position, position)) AS nfl_position,
      ARG_MAX(nfl_team, row_sort) AS nfl_team,
      -- A team DST is ONE franchise across relocations; collapse the historical code list to
      -- its current code so it never reads as "played for multiple teams". Individual players
      -- keep the full list. (nfl_team singular already = the final/current code.)
      CASE WHEN COALESCE(ANY_VALUE(primary_position), MODE(position)) = 'DEF'
           THEN ARG_MAX(nfl_team, row_sort) ELSE {distinct_text_list_expr("nfl_team")} END AS nfl_teams,
      CAST(CASE WHEN COALESCE(ANY_VALUE(primary_position), MODE(position)) = 'DEF'
                THEN 1 ELSE {distinct_text_count_expr("nfl_team")} END AS INTEGER) AS nfl_team_count,
      CAST(ARG_MAX(nfl_franchise_number, row_sort) AS INTEGER) AS nfl_franchise_number,
      ARG_MAX(headshot_url, row_sort) AS headshot_url,
      CAST(MIN(year) AS INTEGER) AS first_year,
      CAST(MAX(year) AS INTEGER) AS last_year,
      CAST(COUNT(DISTINCT year) AS INTEGER) AS years_active,
      CAST(COUNT(*) AS INTEGER) AS games_played,
      CURRENT_TIMESTAMP AS last_updated
    FROM filtered
    GROUP BY NFL_player_id
    """


def add_double_columns(writer: FlyWriter, table: str, cols: list[str]) -> None:
    for col in cols:
        writer.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {q_ident(col)} DOUBLE", database="___ops")


def update_lamar_columns(
    writer: FlyWriter,
    table: str,
    *,
    lamar_cols: list[str],
    include_playoffs: bool,
    season: bool,
    year: int | None = None,
) -> None:
    if not lamar_cols:
        return
    stage_table = stage_table_for(table)
    agg_stage = aggregate_stage_table_for(table)
    lamar_ppg_cols = [col.replace("lamar_", "lamar_ppg_", 1) for col in lamar_cols]
    add_double_columns(writer, stage_table, lamar_cols + lamar_ppg_cols)
    where = build_filtered_where(include_playoffs, year if season else None)
    key_select = "NFL_player_id, CAST(year AS INTEGER) AS year" if season else "NFL_player_id"
    group_by = "NFL_player_id, year" if season else "NFL_player_id"
    selects = ",\n        ".join(
        [f"ROUND(SUM(COALESCE({q_ident(col)}, 0)), 4) AS {q_ident(col)}" for col in lamar_cols]
        + [f"AVG({q_ident(col)}) AS {q_ident(ppg_col)}" for col, ppg_col in zip(lamar_cols, lamar_ppg_cols)]
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {agg_stage} AS
        SELECT
          {key_select},
          {selects}
        FROM {SUPER_TABLE}
        WHERE {where}
        GROUP BY {group_by}
        """,
        database="___ops",
    )
    set_sql = ", ".join(f"{q_ident(col)} = st.{q_ident(col)}" for col in lamar_cols + lamar_ppg_cols)
    where_sql = (
        "t.NFL_player_id = st.NFL_player_id AND t.year = st.year" if season else "t.NFL_player_id = st.NFL_player_id"
    )
    writer.execute(
        f"""
        UPDATE {stage_table} AS t
        SET {set_sql}
        FROM {agg_stage} AS st
        WHERE {where_sql}
        """,
        database="___ops",
    )


def create_aggregate_chunk_table(
    writer: FlyWriter,
    table: str,
    *,
    idx: int,
    cols: list[str],
    include_playoffs: bool,
    season: bool,
    year: int | None = None,
    use_season_fact_adjustments: bool = False,
) -> tuple[str, list[str]]:
    chunk_table = chunk_stage_table_for(table, "agg", idx)
    where = build_filtered_where(include_playoffs, year if season else None, alias="s")
    key_select = "s.NFL_player_id, CAST(s.year AS INTEGER) AS year" if season else "s.NFL_player_id"
    group_by = "s.NFL_player_id, s.year" if season else "s.NFL_player_id"
    canonical_weekly_cols = [col for col in cols if col in CANONICAL_WEEKLY_TOTAL_COLS]
    standard_cols = [col for col in cols if col not in CANONICAL_WEEKLY_TOTAL_COLS]
    adjustment_cols = season_fact_adjustment_cols(standard_cols) if use_season_fact_adjustments else []
    adjustment_alias = "season_adj" if adjustment_cols else None
    agg_selects = ",\n        ".join(
        aggregate_expr(col, source_alias="s", adjustment_alias=adjustment_alias) for col in standard_cols
    )
    needs_team_volume = any(col in SHARE_FAMILY_COLS for col in standard_cols)
    cte_parts: list[str] = []
    join_parts: list[str] = []
    if adjustment_cols:
        cte_parts.append(
            season_fact_adjustment_cte(
                adjustment_cols,
                include_playoffs=include_playoffs,
                season=season,
                year=year,
            )
        )
        join_parts.append(
            "LEFT JOIN season_adj ON season_adj.NFL_player_id = s.NFL_player_id "
            "AND season_adj.year = CAST(s.year AS INTEGER)"
            if season
            else "LEFT JOIN season_adj ON season_adj.NFL_player_id = s.NFL_player_id"
        )
    if needs_team_volume:
        cte_parts.append(team_volume_cte_sql(include_playoffs=include_playoffs, year=year if season else None))
        join_parts.append(team_volume_join_sql("s"))
    join_sql = "\n        ".join(join_parts)

    if canonical_weekly_cols:
        canonical_where = build_filtered_where(include_playoffs, year if season else None)
        def canonical_weekly_expr(col: str) -> str:
            column = q_ident(col)
            if col == "fg_made_60_plus_canonical":
                # The live transition column is sparse: a NULL bucket means no
                # 60+ make when ordinary FG attempts prove kicker-stat coverage.
                # Keep NULL only when the source itself is unavailable.
                return (
                    f"MAX(CASE WHEN fg_att IS NOT NULL "
                    f"THEN COALESCE({column}, 0) ELSE {column} END) AS {column}"
                )
            return f"MAX({column}) AS {column}"

        weekly_values = ",\n          ".join(
            # MAX collapses duplicate copies of the same canonical weekly total.
            # DuckDB MAX(NULL...) and the later SUM(NULL...) both remain NULL,
            # preserving "not covered" rather than manufacturing a zero.
            canonical_weekly_expr(col) for col in canonical_weekly_cols
        )
        canonical_values = ",\n          ".join(
            f"ROUND(SUM({q_ident(col)}), 4) AS {q_ident(col)}" for col in canonical_weekly_cols
        )
        canonical_key_select = "NFL_player_id, CAST(year AS INTEGER) AS year" if season else "NFL_player_id"
        canonical_group_by = "NFL_player_id, year" if season else "NFL_player_id"
        cte_parts.extend(
            [
                f"""
                canonical_weekly AS (
                  SELECT
                    NFL_player_id,
                    CAST(year AS INTEGER) AS year,
                    week,
                    {weekly_values}
                  FROM {SUPER_TABLE}
                  WHERE {canonical_where}
                  GROUP BY NFL_player_id, year, week
                )
                """,
                f"""
                canonical_aggregate AS (
                  SELECT
                    {canonical_key_select},
                    {canonical_values}
                  FROM canonical_weekly
                  GROUP BY {canonical_group_by}
                )
                """,
            ]
        )

    if standard_cols and canonical_weekly_cols:
        cte_parts.append(
            f"""
            standard_aggregate AS (
              SELECT
                {key_select},
                {agg_selects}
              FROM {SUPER_TABLE} AS s
              {join_sql}
              WHERE {where}
              GROUP BY {group_by}
            )
            """
        )
        join_keys = (
            "c.NFL_player_id = a.NFL_player_id AND c.year = a.year"
            if season
            else "c.NFL_player_id = a.NFL_player_id"
        )
        final_select = (
            "SELECT a.*, "
            + ", ".join(f"c.{q_ident(col)}" for col in canonical_weekly_cols)
            + f" FROM standard_aggregate AS a LEFT JOIN canonical_aggregate AS c ON {join_keys}"
        )
    elif canonical_weekly_cols:
        final_select = "SELECT * FROM canonical_aggregate"
    else:
        final_select = f"""
        SELECT
          {key_select},
          {agg_selects}
        FROM {SUPER_TABLE} AS s
        {join_sql}
        WHERE {where}
        GROUP BY {group_by}
        """

    cte_sql = "WITH " + ", ".join(cte_parts) if cte_parts else ""
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {chunk_table} AS
        {cte_sql}
        {final_select}
        """,
        database="___ops",
    )
    actual_cols = {col.name for col in fetch_columns(writer, chunk_table)}
    missing_cols = [col for col in cols if col not in actual_cols]
    if missing_cols:
        raise RuntimeError(f"{chunk_table} missing expected aggregate columns after rebuild: {', '.join(missing_cols)}")
    return chunk_table, cols


def create_lamar_table(
    writer: FlyWriter,
    table: str,
    *,
    lamar_cols: list[str],
    include_playoffs: bool,
    season: bool,
    year: int | None = None,
) -> tuple[str, list[str]] | None:
    if not lamar_cols:
        return None
    lamar_table = chunk_stage_table_for(table, "lamar")
    lamar_ppg_cols = [col.replace("lamar_", "lamar_ppg_", 1) for col in lamar_cols]
    where = build_filtered_where(include_playoffs, year if season else None)
    key_select = "NFL_player_id, CAST(year AS INTEGER) AS year" if season else "NFL_player_id"
    group_by = "NFL_player_id, year" if season else "NFL_player_id"
    selects = ",\n        ".join(
        [f"ROUND(SUM(COALESCE({q_ident(col)}, 0)), 4) AS {q_ident(col)}" for col in lamar_cols]
        + [f"AVG({q_ident(col)}) AS {q_ident(ppg_col)}" for col, ppg_col in zip(lamar_cols, lamar_ppg_cols)]
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {lamar_table} AS
        SELECT
          {key_select},
          {selects}
        FROM {SUPER_TABLE}
        WHERE {where}
        GROUP BY {group_by}
        """,
        database="___ops",
    )
    return lamar_table, lamar_cols + lamar_ppg_cols


def create_season_ppg_table(
    writer: FlyWriter,
    table: str,
    *,
    include_playoffs: bool,
    year: int | None = None,
) -> tuple[str, list[str]]:
    ppg_table = chunk_stage_table_for(table, "ppg")
    where = build_filtered_where(include_playoffs, year)
    points_select = ", ".join(q_ident(points_col) for _, _, points_col in PPG_VARIANTS)
    ppg_selects = ",\n        ".join(
        f"ROUND(AVG(CAST({q_ident(points_col)} AS DOUBLE)), 2) AS {q_ident(ppg_col)},\n"
        f"        {consistency_expr(points_col)} AS {q_ident(consistency_col)}"
        for td, ppr, points_col in PPG_VARIANTS
        for ppg_col, consistency_col, _, _ in [ppg_cols_for_variant(td, ppr)]
    )
    weighted_selects = ",\n        ".join(
        f"{weighted_expr(points_col)} AS {q_ident(weighted_col)}"
        for td, ppr, points_col in PPG_VARIANTS
        for _, _, weighted_col, _ in [ppg_cols_for_variant(td, ppr)]
    )
    next_selects = ",\n      ".join(
        f"next_g.{q_ident(ppg_col)} AS {q_ident(next_col)}"
        for td, ppr, _ in PPG_VARIANTS
        for ppg_col, _, _, next_col in [ppg_cols_for_variant(td, ppr)]
    )
    weighted_cols = [
        weighted_col for td, ppr, _ in PPG_VARIANTS for _, _, weighted_col, _ in [ppg_cols_for_variant(td, ppr)]
    ]
    cols: list[str] = []
    for td, ppr, _ in PPG_VARIANTS:
        cols.extend(ppg_cols_for_variant(td, ppr))
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {ppg_table} AS
        WITH filtered AS (
          SELECT
            NFL_player_id,
            CAST(year AS INTEGER) AS year,
            week,
            player_week,
            {points_select},
            CAST(year AS BIGINT) * 1000000
              + CAST(week AS BIGINT) * 1000
              + ROW_NUMBER() OVER (
                  PARTITION BY NFL_player_id, year, week
                  ORDER BY player_week
                ) AS row_sort
          FROM {SUPER_TABLE}
          WHERE {where}
        ),
        grouped AS (
          SELECT
            NFL_player_id,
            year,
            {ppg_selects}
          FROM filtered
          GROUP BY NFL_player_id, year
        ),
        ordered AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY NFL_player_id, year
              ORDER BY row_sort DESC
            ) AS rn_desc
          FROM filtered
        ),
        weighted AS (
          SELECT
            NFL_player_id,
            year,
            {weighted_selects}
          FROM ordered
          WHERE rn_desc <= 5
          GROUP BY NFL_player_id, year
        )
        SELECT
          g.*,
          {", ".join(f"w.{q_ident(col)}" for col in weighted_cols)},
          {next_selects}
        FROM grouped AS g
        LEFT JOIN weighted AS w
          ON w.NFL_player_id = g.NFL_player_id
         AND w.year = g.year
        LEFT JOIN grouped AS next_g
          ON next_g.NFL_player_id = g.NFL_player_id
         AND next_g.year = g.year + 1
        """,
        database="___ops",
    )
    return ppg_table, cols


def create_career_ppg_table(
    writer: FlyWriter,
    table: str,
    *,
    include_playoffs: bool,
) -> tuple[str, list[str]]:
    ppg_table = chunk_stage_table_for(table, "ppg")
    where = build_filtered_where(include_playoffs)
    cols = [ppg_alltime_col(td, ppr) for td, ppr, _ in PPG_VARIANTS]
    selects = ",\n          ".join(
        f"ROUND(AVG(CAST({q_ident(points_col)} AS DOUBLE)), 2) AS {q_ident(ppg_alltime_col(td, ppr))}"
        for td, ppr, points_col in PPG_VARIANTS
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {ppg_table} AS
        SELECT
          NFL_player_id,
          {selects}
        FROM {SUPER_TABLE}
        WHERE {where}
        GROUP BY NFL_player_id
        """,
        database="___ops",
    )
    return ppg_table, cols


def build_final_stage_table(
    writer: FlyWriter,
    table: str,
    *,
    season: bool,
    join_specs: list[tuple[str, str, list[str]]],
) -> None:
    base_table = base_stage_table_for(table)
    stage_table = stage_table_for(table)
    selects = ["b.*"]
    joins = []
    for alias, join_table, cols in join_specs:
        selects.extend(f"{alias}.{q_ident(col)} AS {q_ident(col)}" for col in cols)
        if season:
            joins.append(
                f"""
                LEFT JOIN {join_table} AS {alias}
                  ON {alias}.NFL_player_id = b.NFL_player_id
                 AND {alias}.year = b.year
                """
            )
        else:
            joins.append(
                f"""
                LEFT JOIN {join_table} AS {alias}
                  ON {alias}.NFL_player_id = b.NFL_player_id
                """
            )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {stage_table} AS
        SELECT
          {", ".join(selects)}
        FROM {base_table} AS b
        {" ".join(joins)}
        """,
        database="___ops",
    )


def update_season_ppg_columns(
    writer: FlyWriter,
    table: str,
    *,
    include_playoffs: bool,
    year: int | None = None,
) -> None:
    stage_table = stage_table_for(table)
    agg_stage = aggregate_stage_table_for(table)
    ppg_cols: list[str] = []
    for td, ppr, _ in PPG_VARIANTS:
        ppg_cols.extend(ppg_cols_for_variant(td, ppr))
    add_double_columns(writer, stage_table, ppg_cols)

    where = build_filtered_where(include_playoffs, year)
    ppg_selects = ",\n        ".join(
        f"ROUND(AVG(CAST({q_ident(points_col)} AS DOUBLE)), 2) AS {q_ident(ppg_col)},\n"
        f"        {consistency_expr(points_col)} AS {q_ident(consistency_col)}"
        for td, ppr, points_col in PPG_VARIANTS
        for ppg_col, consistency_col, _, _ in [ppg_cols_for_variant(td, ppr)]
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {agg_stage} AS
        SELECT
          NFL_player_id,
          CAST(year AS INTEGER) AS year,
          {ppg_selects}
        FROM {SUPER_TABLE}
        WHERE {where}
        GROUP BY NFL_player_id, year
        """,
        database="___ops",
    )
    direct_cols = [
        col
        for td, ppr, _ in PPG_VARIANTS
        for ppg_col, consistency_col, _, _ in [ppg_cols_for_variant(td, ppr)]
        for col in (ppg_col, consistency_col)
    ]
    set_sql = ", ".join(f"{q_ident(col)} = st.{q_ident(col)}" for col in direct_cols)
    writer.execute(
        f"""
        UPDATE {stage_table} AS t
        SET {set_sql}
        FROM {agg_stage} AS st
        WHERE t.NFL_player_id = st.NFL_player_id
          AND t.year = st.year
        """,
        database="___ops",
    )

    weighted_selects = ",\n        ".join(
        f"{weighted_expr(points_col)} AS {q_ident(weighted_col)}"
        for td, ppr, points_col in PPG_VARIANTS
        for _, _, weighted_col, _ in [ppg_cols_for_variant(td, ppr)]
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {agg_stage} AS
        WITH filtered AS (
          SELECT
            NFL_player_id,
            CAST(year AS INTEGER) AS year,
            {", ".join(q_ident(points_col) for _, _, points_col in PPG_VARIANTS)},
            CAST(year AS BIGINT) * 1000000
              + CAST(week AS BIGINT) * 1000
              + ROW_NUMBER() OVER (
                  PARTITION BY NFL_player_id, year, week
                  ORDER BY player_week
                ) AS row_sort
          FROM {SUPER_TABLE}
          WHERE {where}
        ),
        ordered AS (
          SELECT
            *,
            ROW_NUMBER() OVER (
              PARTITION BY NFL_player_id, year
              ORDER BY row_sort DESC
            ) AS rn_desc
          FROM filtered
        )
        SELECT
          NFL_player_id,
          year,
          {weighted_selects}
        FROM ordered
        WHERE rn_desc <= 5
        GROUP BY NFL_player_id, year
        """,
        database="___ops",
    )
    weighted_cols = [ppg_cols_for_variant(td, ppr)[2] for td, ppr, _ in PPG_VARIANTS]
    set_weighted = ", ".join(f"{q_ident(col)} = st.{q_ident(col)}" for col in weighted_cols)
    writer.execute(
        f"""
        UPDATE {stage_table} AS t
        SET {set_weighted}
        FROM {agg_stage} AS st
        WHERE t.NFL_player_id = st.NFL_player_id
          AND t.year = st.year
        """,
        database="___ops",
    )

    for td, ppr, _ in PPG_VARIANTS:
        ppg_col, _, _, next_col = ppg_cols_for_variant(td, ppr)
        writer.execute(
            f"""
            UPDATE {stage_table} AS t
            SET {q_ident(next_col)} = next_t.{q_ident(ppg_col)}
            FROM {stage_table} AS next_t
            WHERE t.NFL_player_id = next_t.NFL_player_id
              AND t.year + 1 = next_t.year
            """,
            database="___ops",
        )


def update_career_ppg_columns(
    writer: FlyWriter,
    table: str,
    *,
    include_playoffs: bool,
) -> None:
    stage_table = stage_table_for(table)
    agg_stage = aggregate_stage_table_for(table)
    ppg_cols = [ppg_alltime_col(td, ppr) for td, ppr, _ in PPG_VARIANTS]
    add_double_columns(writer, stage_table, ppg_cols)
    where = build_filtered_where(include_playoffs)
    selects = ",\n        ".join(
        f"ROUND(AVG(CAST({q_ident(points_col)} AS DOUBLE)), 2) AS {q_ident(ppg_alltime_col(td, ppr))}"
        for td, ppr, points_col in PPG_VARIANTS
    )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {agg_stage} AS
        SELECT
          NFL_player_id,
          {selects}
        FROM {SUPER_TABLE}
        WHERE {where}
        GROUP BY NFL_player_id
        """,
        database="___ops",
    )
    set_sql = ", ".join(f"{q_ident(col)} = st.{q_ident(col)}" for col in ppg_cols)
    writer.execute(
        f"""
        UPDATE {stage_table} AS t
        SET {set_sql}
        FROM {agg_stage} AS st
        WHERE t.NFL_player_id = st.NFL_player_id
        """,
        database="___ops",
    )


def build_stage_table(
    writer: FlyWriter,
    table: str,
    *,
    aggregate_cols: list[str],
    lamar_cols: list[str],
    include_playoffs: bool,
    season: bool,
    year: int | None = None,
    use_season_fact_adjustments: bool = False,
) -> None:
    print(f"  [base {table.split('.')[-1]}]", flush=True)
    if season:
        writer.execute(build_season_base_sql(table, include_playoffs=include_playoffs, year=year), database="___ops")
    else:
        writer.execute(build_career_base_sql(table, include_playoffs=include_playoffs), database="___ops")

    join_specs: list[tuple[str, str, list[str]]] = []
    aggregate_chunk_size = env_int("NFL_AGGREGATE_CHUNK_SIZE", 32)
    for idx, cols in enumerate(chunk_names(aggregate_cols, aggregate_chunk_size), start=1):
        print(f"  [agg {table.split('.')[-1]} chunk {idx}] {cols[0]} .. {cols[-1]} ({len(cols)} cols)", flush=True)
        chunk_table, chunk_cols = create_aggregate_chunk_table(
            writer,
            table,
            idx=idx,
            cols=cols,
            include_playoffs=include_playoffs,
            season=season,
            year=year,
            use_season_fact_adjustments=use_season_fact_adjustments,
        )
        join_specs.append((f"a{idx}", chunk_table, chunk_cols))
    print(f"  [ppg {table.split('.')[-1]}]", flush=True)
    if season:
        ppg_table, ppg_cols = create_season_ppg_table(writer, table, include_playoffs=include_playoffs, year=year)
    else:
        ppg_table, ppg_cols = create_career_ppg_table(writer, table, include_playoffs=include_playoffs)
    join_specs.append(("ppg", ppg_table, ppg_cols))
    print(f"  [lamar {table.split('.')[-1]}]", flush=True)
    lamar_spec = create_lamar_table(
        writer,
        table,
        lamar_cols=lamar_cols,
        include_playoffs=include_playoffs,
        season=season,
        year=year,
    )
    if lamar_spec is not None:
        lamar_table, lamar_join_cols = lamar_spec
        join_specs.append(("lm", lamar_table, lamar_join_cols))
    print(f"  [final {table.split('.')[-1]}]", flush=True)
    build_final_stage_table(writer, table, season=season, join_specs=join_specs)


def chunk_names(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def rank_specs_for_scope(scope: str) -> list[RankSpec]:
    prefix = "rank_season" if scope == "season" else "rank_alltime"
    specs: list[RankSpec] = [
        RankSpec(f"{prefix}_qb_4pt", scope, ("QB",), "fpts_4pt_half"),
        RankSpec(f"{prefix}_qb_5pt", scope, ("QB",), "fpts_5pt_half"),
        RankSpec(f"{prefix}_qb_6pt", scope, ("QB",), "fpts_6pt_half"),
    ]

    for pos in ("RB", "WR", "TE"):
        lower = pos.lower()
        # Fullbacks are eligible running backs in every supported fantasy
        # platform.  Keep that position family in the shared rank spec rather
        # than letting a persisted FB primary_position fall out of the RB
        # season/career population during a live OPS rebuild.
        positions = ("RB", "FB") if pos == "RB" else (pos,)
        specs.extend(
            [
                RankSpec(f"{prefix}_{lower}_0ppr", scope, positions, "fpts_4pt_0ppr"),
                RankSpec(f"{prefix}_{lower}_half", scope, positions, "fpts_4pt_half"),
                RankSpec(f"{prefix}_{lower}_ppr", scope, positions, "fpts_4pt_ppr"),
            ]
        )
    specs.append(RankSpec(f"{prefix}_te_tep", scope, ("TE",), "fpts_4pt_tep"))
    for pos in ("RB", "WR", "TE"):
        positions = ("RB", "FB") if pos == "RB" else (pos,)
        specs.append(RankSpec(f"{prefix}_{pos.lower()}_ppfd", scope, positions, "fpts_4pt_ppfd"))
    specs.extend(
        [
            RankSpec(f"{prefix}_k", scope, ("K",), "pts_k_std"),  # distance-tier (modal Sleeper), not yardage
            RankSpec(f"{prefix}_def", scope, ("DEF",), "pts_def_std"),
        ]
    )

    lb_positions = ("LB", "ILB", "OLB", "MLB")
    dl_positions = ("DL", "DE", "DT", "NT", "ED", "EDGE")
    db_positions = ("DB", "CB", "S", "SS", "FS", "SAF")
    for slot, positions in (("lb", lb_positions), ("dl", dl_positions), ("db", db_positions)):
        specs.extend(
            [
                RankSpec(f"{prefix}_{slot}_std", scope, positions, "pts_idp_std"),
                RankSpec(f"{prefix}_{slot}_premium", scope, positions, "pts_idp_premium"),
                RankSpec(f"{prefix}_{slot}_tackle_heavy", scope, positions, "pts_idp_tackle_heavy"),
                RankSpec(f"{prefix}_{slot}_big_play", scope, positions, "pts_idp_big_play"),
            ]
        )

    for ppr, points_col in (
        ("0ppr", "fpts_4pt_0ppr"),
        ("half", "fpts_4pt_half"),
        ("ppr", "fpts_4pt_ppr"),
        ("ppfd", "fpts_4pt_ppfd"),
    ):
        specs.append(RankSpec(f"{prefix}_flex_{ppr}", scope, ("RB", "WR", "TE"), points_col))
        specs.append(RankSpec(f"{prefix}_recflex_{ppr}", scope, ("WR", "TE"), points_col))
        specs.append(RankSpec(f"{prefix}_wrflex_{ppr}", scope, ("RB", "WR"), points_col))
        specs.append(RankSpec(f"{prefix}_rtflex_{ppr}", scope, ("RB", "TE"), points_col))

    specs.append(RankSpec(f"{prefix}_flex_tep", scope, ("RB", "WR", "TE"), "fpts_4pt_tep"))
    specs.append(RankSpec(f"{prefix}_recflex_tep", scope, ("WR", "TE"), "fpts_4pt_tep"))

    for td in ("4pt", "5pt", "6pt"):
        for ppr in ("0ppr", "half", "ppr"):
            specs.append(RankSpec(f"{prefix}_sflex_{td}_{ppr}", scope, ("QB", "RB", "WR", "TE"), f"fpts_{td}_{ppr}"))
    # SUPERFLEX x TE-premium (the #1 population gap) + SUPERFLEX x PPFD
    for td in ("4pt", "6pt"):
        specs.append(RankSpec(f"{prefix}_sflex_{td}_tep", scope, ("QB", "RB", "WR", "TE"), f"fpts_{td}_tep"))
        specs.append(RankSpec(f"{prefix}_sflex_{td}_ppfd", scope, ("QB", "RB", "WR", "TE"), f"fpts_{td}_ppfd"))

    idp_flex_positions = lb_positions + dl_positions + db_positions
    specs.extend(
        [
            RankSpec(f"{prefix}_idp_flex_std", scope, idp_flex_positions, "pts_idp_std"),
            RankSpec(f"{prefix}_idp_flex_premium", scope, idp_flex_positions, "pts_idp_premium"),
            RankSpec(f"{prefix}_idp_flex_tackle_heavy", scope, idp_flex_positions, "pts_idp_tackle_heavy"),
            RankSpec(f"{prefix}_idp_flex_big_play", scope, idp_flex_positions, "pts_idp_big_play"),
        ]
    )
    return specs


def validate_rank_specs() -> None:
    expected_season = set(SEASON_POSITION_RANK_COLUMNS) | set(SEASON_FLEX_RANK_COLUMNS)
    expected_alltime = set(ALLTIME_POSITION_RANK_COLUMNS) | set(ALLTIME_FLEX_RANK_COLUMNS)
    season = {spec.col for spec in rank_specs_for_scope("season")}
    alltime = {spec.col for spec in rank_specs_for_scope("alltime")}
    if season != expected_season:
        raise ValueError(
            f"Season rank spec mismatch: missing={sorted(expected_season - season)}, extra={sorted(season - expected_season)}"
        )
    if alltime != expected_alltime:
        raise ValueError(
            f"All-time rank spec mismatch: missing={sorted(expected_alltime - alltime)}, extra={sorted(alltime - expected_alltime)}"
        )


def positions_sql(positions: tuple[str, ...]) -> str:
    return ", ".join(q(pos) for pos in positions)


def positions_list_sql(positions: tuple[str, ...]) -> str:
    return "[" + ", ".join(q(pos) for pos in positions) + "]"


def position_has_any_sql(column: str, positions: tuple[str, ...]) -> str:
    return f"list_has_any(string_split(COALESCE({column}, ''), ','), {positions_list_sql(positions)})"


def chunked(items: list[RankSpec], size: int) -> list[list[RankSpec]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def build_rank_stage_sql(table: str, specs: list[RankSpec], scope: str, stage: str) -> str:
    points_cols = sorted({spec.points_col for spec in specs})
    point_select = ",\n        ".join(q_ident(col) for col in points_cols)
    season = scope == "season"
    key_select = "NFL_player_id, year" if season else "NFL_player_id"
    ctes = [
        f"""
        base AS (
          SELECT
            {key_select},
            position AS rank_pos,
            {point_select}
          FROM {table}
          WHERE NFL_player_id IS NOT NULL
        )
        """
    ]
    for spec in specs:
        partition = "PARTITION BY year" if season else ""
        key_cols = "NFL_player_id, year" if season else "NFL_player_id"
        ctes.append(
            f"""
            {spec.cte_name} AS (
              SELECT
                {key_cols},
                ROW_NUMBER() OVER (
                  {partition}
                  ORDER BY {q_ident(spec.points_col)} DESC, NFL_player_id ASC
                ) AS {q_ident(spec.col)}
              FROM base
              WHERE {position_has_any_sql("rank_pos", spec.positions)}
            )
            """
        )

    selects = ["b.NFL_player_id"]
    if season:
        selects.append("b.year")
    joins = []
    for spec in specs:
        selects.append(f"{spec.cte_name}.{q_ident(spec.col)}")
        if season:
            joins.append(
                f"""
                LEFT JOIN {spec.cte_name}
                  ON {spec.cte_name}.NFL_player_id = b.NFL_player_id
                 AND {spec.cte_name}.year = b.year
                """
            )
        else:
            joins.append(
                f"""
                LEFT JOIN {spec.cte_name}
                  ON {spec.cte_name}.NFL_player_id = b.NFL_player_id
                """
            )
    return f"""
    CREATE OR REPLACE TABLE {stage} AS
    WITH {", ".join(ctes)}
    SELECT
      {", ".join(selects)}
    FROM base AS b
    {" ".join(joins)}
    """


def recompute_ranks(writer: FlyWriter, table: str, specs: list[RankSpec], scope: str, chunk_size: int = 24) -> None:
    rank_join_specs: list[tuple[str, str, list[RankSpec]]] = []
    effective_chunk_size = env_int("NFL_RANK_CHUNK_SIZE", chunk_size)
    for idx, chunk in enumerate(chunked(specs, effective_chunk_size), start=1):
        stage = chunk_stage_table_for(table, "rank", idx)
        print(f"  [rank {table.split('.')[-1]} chunk {idx}] {chunk[0].col} .. {chunk[-1].col}", flush=True)
        writer.execute(build_rank_stage_sql(table, chunk, scope, stage), database="___ops")
        rank_join_specs.append((f"r{idx}", stage, chunk))

    print(f"  [rank final {table.split('.')[-1]}]", flush=True)
    base_cols = [col.name for col in fetch_columns(writer, table) if not col.name.startswith("rank_")]
    ranked_stage = ranked_stage_table_for(table)
    selects = [f"b.{q_ident(col)}" for col in base_cols]
    joins = []
    for alias, rank_stage, chunk in rank_join_specs:
        selects.extend(f"CAST({alias}.{q_ident(spec.col)} AS INTEGER) AS {q_ident(spec.col)}" for spec in chunk)
        if scope == "season":
            joins.append(
                f"""
                LEFT JOIN {rank_stage} AS {alias}
                  ON {alias}.NFL_player_id = b.NFL_player_id
                 AND {alias}.year = b.year
                """
            )
        else:
            joins.append(
                f"""
                LEFT JOIN {rank_stage} AS {alias}
                  ON {alias}.NFL_player_id = b.NFL_player_id
                """
            )
    writer.execute(
        f"""
        CREATE OR REPLACE TABLE {ranked_stage} AS
        SELECT
          {", ".join(selects)}
        FROM {table} AS b
        {" ".join(joins)}
        """,
        database="___ops",
    )
    writer.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {ranked_stage}", database="___ops")


def backup_table(writer: FlyWriter, table: str) -> None:
    backup = f"{PUBLIC}.{table.split('.')[-1]}_backup_{ROLLUP_DATE}"
    if table_exists(writer, backup):
        rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {backup}")
        print(f"[backup] {backup} exists rows={rows:,}")
        return
    writer.execute(f"CREATE TABLE {backup} AS SELECT * FROM {table}", database="___ops")
    rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {backup}")
    print(f"[backup] {backup} created rows={rows:,}")


def swap_stage_to_live(writer: FlyWriter, table: str) -> None:
    stage = f"{PUBLIC}.{table.split('.')[-1]}_stage_{ROLLUP_DATE}"
    rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {stage}")
    if rows <= 0:
        raise RuntimeError(f"Refusing to replace {table}: stage {stage} has no rows")
    writer.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT * FROM {stage}", database="___ops")
    live_rows = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {table}")
    print(f"[swap] {table} rows={live_rows:,}")


def verify_count(writer: FlyWriter, table: str, *, include_playoffs: bool, season: bool) -> int:
    if season:
        expected_sql = f"""
          SELECT COUNT(*) AS n
          FROM (
            SELECT DISTINCT NFL_player_id, year
            FROM {SUPER_TABLE}
            WHERE {build_filtered_where(include_playoffs)}
          )
        """
    else:
        expected_sql = f"""
          SELECT COUNT(DISTINCT NFL_player_id) AS n
          FROM {SUPER_TABLE}
          WHERE {build_filtered_where(include_playoffs)}
        """
    expected = fetch_scalar(writer, expected_sql)
    actual = fetch_scalar(writer, f"SELECT COUNT(*) AS n FROM {table}")
    if expected != actual:
        raise RuntimeError(f"{table} row count mismatch: expected={expected}, actual={actual}")
    return actual


def update_aggregates(
    year: int | None = None,
    week: int | None = None,
    *,
    rebuild_all_years: bool = True,
    writer: LongFlyWriter | LocalDuckDBWriter | None = None,
    make_backups: bool = True,
) -> dict[str, int]:
    """Rebuild Fly NFL aggregate tables from the weekly super table.

    `year` is accepted for API compatibility. By default this rebuilds all
    years so career tables remain correct after any historical repair.
    """
    del week
    print("[rollup] load env + validate rank specs", flush=True)
    load_env()
    validate_rank_specs()
    writer = writer or LongFlyWriter()
    print("[rollup] discover weekly/aggregate columns", flush=True)
    aggregate_cols, lamar_cols, fpts_cols, bonus_cols = discover_aggregate_columns(writer)
    use_season_fact_adjustments = table_exists(writer, SEASON_FACT_TABLE)
    target_year = None if rebuild_all_years else year

    print("[rollup] columns:", flush=True)
    print(f"  aggregate numeric cols={len(aggregate_cols)}", flush=True)
    print(f"  fpts cols={len(fpts_cols)}", flush=True)
    print(f"  bonus cols={len(bonus_cols)}", flush=True)
    print(f"  lamar cols={len(lamar_cols)}", flush=True)
    print(f"  season-only fact adjustments={'on' if use_season_fact_adjustments else 'off'}", flush=True)

    if make_backups:
        for table in (SEASON_TABLE, SEASON_ALL_TABLE, CAREER_TABLE, CAREER_ALL_TABLE):
            backup_table(writer, table)

    print("[build] season regular", flush=True)
    build_stage_table(
        writer,
        SEASON_TABLE,
        aggregate_cols=aggregate_cols,
        lamar_cols=lamar_cols,
        include_playoffs=False,
        season=True,
        year=target_year,
        use_season_fact_adjustments=use_season_fact_adjustments,
    )
    swap_stage_to_live(writer, SEASON_TABLE)

    print("[build] season all-games", flush=True)
    build_stage_table(
        writer,
        SEASON_ALL_TABLE,
        aggregate_cols=aggregate_cols,
        lamar_cols=lamar_cols,
        include_playoffs=True,
        season=True,
        year=target_year,
        use_season_fact_adjustments=use_season_fact_adjustments,
    )
    swap_stage_to_live(writer, SEASON_ALL_TABLE)

    print("[build] career regular", flush=True)
    build_stage_table(
        writer,
        CAREER_TABLE,
        aggregate_cols=aggregate_cols,
        lamar_cols=lamar_cols,
        include_playoffs=False,
        season=False,
        use_season_fact_adjustments=use_season_fact_adjustments,
    )
    swap_stage_to_live(writer, CAREER_TABLE)

    print("[build] career all-games", flush=True)
    build_stage_table(
        writer,
        CAREER_ALL_TABLE,
        aggregate_cols=aggregate_cols,
        lamar_cols=lamar_cols,
        include_playoffs=True,
        season=False,
        use_season_fact_adjustments=use_season_fact_adjustments,
    )
    swap_stage_to_live(writer, CAREER_ALL_TABLE)

    season_specs = rank_specs_for_scope("season")
    alltime_specs = rank_specs_for_scope("alltime")
    print("[rank] season regular", flush=True)
    recompute_ranks(writer, SEASON_TABLE, season_specs, "season")
    print("[rank] season all-games", flush=True)
    recompute_ranks(writer, SEASON_ALL_TABLE, season_specs, "season")
    print("[rank] career regular", flush=True)
    recompute_ranks(writer, CAREER_TABLE, alltime_specs, "alltime")
    print("[rank] career all-games", flush=True)
    recompute_ranks(writer, CAREER_ALL_TABLE, alltime_specs, "alltime")

    counts = {
        "season": verify_count(writer, SEASON_TABLE, include_playoffs=False, season=True),
        "season_all": verify_count(writer, SEASON_ALL_TABLE, include_playoffs=True, season=True),
        "career": verify_count(writer, CAREER_TABLE, include_playoffs=False, season=False),
        "career_all": verify_count(writer, CAREER_ALL_TABLE, include_playoffs=True, season=False),
    }
    print("[rollup] counts:", counts)
    return counts
