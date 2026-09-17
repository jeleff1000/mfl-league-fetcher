#!/usr/bin/env python3
"""Offline NFL ops artifact builder (WS-B v1) — splits the wide super table
into update-scoped sidecars and packages promotion bundles for /merge-ops.

Never writes to Fly. The output is a validated local artifact:

    nfl_player_stats_base    weekly facts + weekly-varying derived cols
    nfl_weekly_ranks         weekly rank columns          (key: player_week)
    nfl_season_scoped        season-constant columns      (key: player_week, v1)
    nfl_career_scoped        career-constant columns      (key: player_week, v1)
    nfl_player_stats_all_compat   view reproducing the wide table exactly

v1 uses an EXACT vertical partition: every sidecar keeps player_week grain, so
the compatibility view reproduces the wide table byte-for-byte even where live
data has within-group inconsistencies (observed 2026-07-05: duplicate week-17
rows with NULL/zeroed season values). The win is update cost, not storage: a
season-rank recompute rewrites a ~40-column sidecar instead of the ~350-column
wide table, and a new week inserts partitions instead of mutating rows.

`--grain natural` additionally normalizes season/career sidecars to their true
grain ((NFL_player_id, year) / (NFL_player_id,)) but first PROVES constancy per
group and fails loudly listing violations — usable once source data is clean.

Weekly updates (WS-B rank wiring, 2026-07-06): `apply_weekly_update()` takes a
new week of BASE FACTS (raw stats + per-row fpts composites from the fetch
plane) and moves every derived family with partition-scoped writes only:

    base          INSERT (year, week) rows; 47 rolling/window cols computed here
    weekly ranks  INSERT (year, week) partition
    season        DELETE+INSERT year partitions {Y-1, Y} (Y-1 owns avg_pts_next_year)
    career        full DELETE+INSERT (career ranks/PPG shift globally)

Prior-week base rows are never touched. The recompute semantics reproduce the
scripts that built the live post-v26 table (golden-diffed against it):
weekly ranks = sota_recon wave40 (`ROW_NUMBER OVER (PARTITION BY year, week,
season_type ORDER BY pts DESC, NFL_player_id)`, population by comma-eligibility
`position`, `pts IS NOT NULL`); PPG/rolling surface = wave42 canonical UPDATEs;
season/career ranks = v26 rank jobs (REG-only natural-grain aggregates,
games-played gates for the _ppg twins, DEF-excluded overall ranks) denormalized
back onto weekly rows exactly like wave48.

primary_position maintenance (WS-C, 2026-07-07): the raw fetch plane emits the
weekly `position` but not the denormalized per-player `primary_position` the
career aggregate reads, and rookies have no prior row to inherit it from.
`apply_weekly_update(..., bio_ref=...)` runs `maintain_primary_position` first —
existing players keep their stored value (constant per player), rookies get
COALESCE(bio.nfl_position, dominant weekly position), the one-time
build_primary_position_v26 rule made incremental.

Usage:
    python build_full_ops.py --source ops_snapshot.duckdb --out artifacts/
    python build_full_ops.py --source ops_snapshot.duckdb --out artifacts/ --grain natural
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, UTC
from pathlib import Path

import duckdb

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    _d = Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    sys.path.insert(0, str(_d.parent))
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.core.delta_publish import _row_hash_expr, _sha256_file, qident  # noqa: E402

WIDE_TABLE = "nfl_player_stats_all"
IDENTITY_COLUMNS = ["player_week", "NFL_player_id", "year", "week"]

SIDECAR_TABLES = {
    "base": "nfl_player_stats_base",
    "weekly_rank": "nfl_weekly_ranks",
    "season": "nfl_season_scoped",
    "career": "nfl_career_scoped",
}
COMPAT_VIEW = "nfl_player_stats_all_compat"

_SEASON_PPG_PREFIXES = ("ppg_season_", "consistency_", "weighted_ppg_", "avg_pts_next_year_")
_CAREER_PPG_PREFIXES = ("ppg_alltime_",)


def _rank_constant_sets() -> dict[str, set[str]]:
    """Column families from the calculators — the single source of truth."""
    from multi_league.data_fetchers.fantasy_points_calculator import (
        ALLTIME_FLEX_RANK_COLUMNS,
        ALLTIME_POSITION_RANK_COLUMNS,
        FLEX_RANK_COLUMNS,
        POSITION_RANK_COLUMNS,
        SEASON_FLEX_RANK_COLUMNS,
        SEASON_POSITION_RANK_COLUMNS,
    )

    return {
        "weekly_rank": set(POSITION_RANK_COLUMNS) | set(FLEX_RANK_COLUMNS),
        "season_rank": set(SEASON_POSITION_RANK_COLUMNS) | set(SEASON_FLEX_RANK_COLUMNS),
        "career_rank": set(ALLTIME_POSITION_RANK_COLUMNS) | set(ALLTIME_FLEX_RANK_COLUMNS),
    }


def classify_wide_columns(columns: list[str]) -> dict[str, list[str]]:
    """Assign every wide column to exactly one sidecar family (order-preserving).

    Prefix rules lead and the calculator constants back them up: the live wide
    table (1,010 columns as of 2026-07-05) carries rank families — e.g. 190
    ``rank_season_*_ppg`` variants — that exist in no calculator constant list,
    so name shape is the only classification that survives schema growth.
    Classification only chooses a column's sidecar; vertical grain stays exact
    regardless, so a miscategorized column costs update efficiency, never
    correctness.
    """
    families = _rank_constant_sets()
    out: dict[str, list[str]] = {"identity": [], "base": [], "weekly_rank": [], "season": [], "career": []}
    for col in columns:
        if col in IDENTITY_COLUMNS:
            out["identity"].append(col)
        elif col.startswith("rank_alltime_") or col in families["career_rank"] or col.startswith(_CAREER_PPG_PREFIXES):
            out["career"].append(col)
        elif col.startswith("rank_season_") or col in families["season_rank"] or col.startswith(_SEASON_PPG_PREFIXES):
            out["season"].append(col)
        elif col.startswith("rank_") or col in families["weekly_rank"]:
            out["weekly_rank"].append(col)
        else:
            out["base"].append(col)
    return out


def _wide_columns(conn: duckdb.DuckDBPyConnection, source_ref: str) -> list[str]:
    return [row[0] for row in conn.execute(f"DESCRIBE {source_ref}").fetchall()]


def _natural_grain_violations(
    conn: duckdb.DuckDBPyConnection, source_ref: str, cols: list[str], group_keys: list[str], limit: int = 20
) -> list[tuple]:
    """Groups where a 'constant' column takes more than one value."""
    if not cols:
        return []
    distinct_checks = ", ".join(f"COUNT(DISTINCT COALESCE(CAST({qident(c)} AS VARCHAR), '<NULL>'))" for c in cols)
    keys = ", ".join(qident(k) for k in group_keys)
    return conn.execute(
        f"""
        SELECT {keys}
        FROM {source_ref}
        GROUP BY {keys}
        HAVING GREATEST({distinct_checks}) > 1
        LIMIT {limit}
        """
    ).fetchall()


def split_wide_to_sidecars(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_ref: str = f"nfl_historical.{WIDE_TABLE}",
    target_schema: str = "nfl_historical",
    grain: str = "vertical",
) -> dict[str, int]:
    """Create the four sidecar tables from the wide table. Returns row counts."""
    columns = _wide_columns(conn, source_ref)
    if "player_week" not in columns:
        raise RuntimeError(f"{source_ref} has no player_week column")
    fam = classify_wide_columns(columns)
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(target_schema)}")

    # The live wide table is NOT unique on player_week (12 duplicate keys as of
    # 2026-07-05, all 1920s-era rows). A bare player_week join would fan out,
    # so every sidecar carries a deterministic _row_uid tie-broken by row
    # content, and reconstruction joins on (player_week, _row_uid).
    # delta_publish._row_hash_expr is the repo's canonical row-content hash
    # (concat_ws keeps the expression tree flat; chained || operators exceed
    # DuckDB's max_expression_depth at the live table's 1,010-column width).
    row_hash = _row_hash_expr(columns)
    conn.execute("DROP TABLE IF EXISTS _wide_aug")
    conn.execute(
        f"""
        CREATE TEMP TABLE _wide_aug AS
        SELECT *, row_number() OVER (
            PARTITION BY player_week ORDER BY {row_hash}
        ) AS _row_uid
        FROM {source_ref}
        """
    )
    source_ref = "_wide_aug"

    if grain == "natural":
        season_bad = _natural_grain_violations(conn, source_ref, fam["season"], ["NFL_player_id", "year"])
        career_bad = _natural_grain_violations(conn, source_ref, fam["career"], ["NFL_player_id"])
        if season_bad or career_bad:
            raise RuntimeError(
                "natural grain violated — season/career columns vary within their group; "
                f"season examples: {season_bad[:5]}, career examples: {career_bad[:5]}. "
                "Use --grain vertical until source data is cleaned."
            )
    elif grain != "vertical":
        raise ValueError(f"Unknown grain: {grain!r}")

    counts: dict[str, int] = {}

    def create(table: str, select_cols: list[str], distinct_keys: list[str] | None = None) -> None:
        ref = f"{qident(target_schema)}.{qident(table)}"
        cols_sql = ", ".join(qident(c) for c in select_cols)
        conn.execute(f"DROP TABLE IF EXISTS {ref}")
        if distinct_keys is None:
            conn.execute(f"CREATE TABLE {ref} AS SELECT {cols_sql} FROM {source_ref}")
        else:
            keys_sql = ", ".join(qident(k) for k in distinct_keys)
            value_cols = [c for c in select_cols if c not in distinct_keys]
            aggs = ", ".join(f"ANY_VALUE({qident(c)}) AS {qident(c)}" for c in value_cols)
            conn.execute(f"CREATE TABLE {ref} AS SELECT {keys_sql}, {aggs} FROM {source_ref} GROUP BY {keys_sql}")
        counts[table] = int(conn.execute(f"SELECT COUNT(*) FROM {ref}").fetchone()[0])

    # year/week ride along on every vertical sidecar so the publish plane can
    # scope DELETE+INSERT by partition without joining back through base.
    create(SIDECAR_TABLES["base"], fam["identity"] + ["_row_uid"] + fam["base"])
    create(SIDECAR_TABLES["weekly_rank"], ["player_week", "_row_uid", "year", "week"] + fam["weekly_rank"])
    if grain == "vertical":
        create(SIDECAR_TABLES["season"], ["player_week", "_row_uid", "year"] + fam["season"])
        create(SIDECAR_TABLES["career"], ["player_week", "_row_uid", "year"] + fam["career"])
    else:
        create(SIDECAR_TABLES["season"], ["NFL_player_id", "year"] + fam["season"], ["NFL_player_id", "year"])
        create(SIDECAR_TABLES["career"], ["NFL_player_id"] + fam["career"], ["NFL_player_id"])
    return counts


def create_compatibility_view(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_ref: str = f"nfl_historical.{WIDE_TABLE}",
    target_schema: str = "nfl_historical",
    view_name: str = COMPAT_VIEW,
    grain: str = "vertical",
) -> None:
    """View over the sidecars reproducing the wide table's exact column order."""
    columns = _wide_columns(conn, source_ref)
    fam = classify_wide_columns(columns)
    s = SIDECAR_TABLES

    def src(col: str) -> str:
        if col in fam["identity"] or col in fam["base"]:
            return f"b.{qident(col)}"
        if col in fam["weekly_rank"]:
            return f"w.{qident(col)}"
        if col in fam["season"]:
            return f"se.{qident(col)}"
        return f"c.{qident(col)}"

    select_sql = ", ".join(f"{src(col)} AS {qident(col)}" for col in columns)
    if grain == "vertical":
        season_join = "se ON b.player_week = se.player_week AND b._row_uid = se._row_uid"
        career_join = "c ON b.player_week = c.player_week AND b._row_uid = c._row_uid"
    else:
        season_join = f'se ON b."NFL_player_id" = se."NFL_player_id" AND b.year = se.year'
        career_join = f'c ON b."NFL_player_id" = c."NFL_player_id"'
    conn.execute(
        f"""
        CREATE OR REPLACE VIEW {qident(target_schema)}.{qident(view_name)} AS
        SELECT {select_sql}
        FROM {qident(target_schema)}.{qident(s["base"])} b
        LEFT JOIN {qident(target_schema)}.{qident(s["weekly_rank"])} w
            ON b.player_week = w.player_week AND b._row_uid = w._row_uid
        LEFT JOIN {qident(target_schema)}.{qident(s["season"])} {season_join}
        LEFT JOIN {qident(target_schema)}.{qident(s["career"])} {career_join}
        """
    )


def validate_compatibility(
    conn: duckdb.DuckDBPyConnection,
    *,
    source_ref: str = f"nfl_historical.{WIDE_TABLE}",
    target_schema: str = "nfl_historical",
    view_name: str = COMPAT_VIEW,
) -> int:
    """Row-level symmetric diff between the wide table and the compat view."""
    view_ref = f"{qident(target_schema)}.{qident(view_name)}"
    return int(
        conn.execute(
            f"""
            SELECT COUNT(*) FROM (
                (SELECT * FROM {source_ref} EXCEPT ALL SELECT * FROM {view_ref})
                UNION ALL
                (SELECT * FROM {view_ref} EXCEPT ALL SELECT * FROM {source_ref})
            )
            """
        ).fetchone()[0]
        or 0
    )


# ---------------------------------------------------------------------------
# Weekly wiring (WS-B): partition-scoped recompute of every derived family.
#
# Every rule below reproduces the exact script that built the live post-v26
# table. Provenance per family:
#   weekly ranks   scripts/sota_recon/build_weekly_ranks_v26.py   (wave40)
#   ppg/rolling    scripts/sota_recon/build_weekly_ppg_all_v26.py (wave42)
#   season/career  scripts/sota_recon/build_season_career_ranks_v26.py +
#                  build_denormalize_ranks_v26.py                 (wave48)
# ---------------------------------------------------------------------------

PPG_TDS = ("4pt", "5pt", "6pt")
PPG_PPRS = ("0ppr", "half", "ppr", "tep", "ppfd")
OVERALL_RULESETS = [f"{td}_{ppr}" for td in PPG_TDS for ppr in ("0ppr", "half", "ppr", "tep", "ppfd")]
IDP_POSITIONS = ("LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "EDGE", "DB", "CB", "S", "SS", "FS", "SAF")
DEF_POSITIONS = ("DEF", "DST")
OFFENSE_K_POSITIONS = ("QB", "RB", "WR", "TE", "OL", "K", "P")
SEASON_GAMES_PCT = 0.5  # season _ppg rank gate: played >= 50% of that season's max games
CAREER_MIN_GAMES = 16  # career _ppg rank gate

# The only base-family columns derived from history rather than fetched facts.
WINDOW_BASE_COLUMNS = [
    f"{fam}_{td}_{ppr}" for fam in ("rolling_3", "rolling_5", "rolling_total") for td in PPG_TDS for ppr in PPG_PPRS
] + ["rolling_total_def", "rolling_total_k"]


def _positions_expr(positions: tuple[str, ...], col: str = "a.position") -> str:
    lst = "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in positions) + "]"
    return f"list_has_any(string_split(COALESCE({col}, ''), ','), {lst})"


def weekly_rank_specs() -> list[tuple[str, str, tuple[str, ...]]]:
    """(rank_col, points_col, positions) — wave40: canonical season specs, prefix-stripped."""
    from multi_league.data_fetchers.aggregate_nfl_stats_fly import rank_specs_for_scope

    specs = [(s.col.replace("rank_season_", "rank_", 1), s.points_col, tuple(s.positions)) for s in rank_specs_for_scope("season")]
    expected = _rank_constant_sets()["weekly_rank"]
    got = {col for col, _, _ in specs}
    if got != expected:
        raise RuntimeError(f"weekly rank specs drifted from calculator constants: missing={sorted(expected - got)}, extra={sorted(got - expected)}")
    return specs


def _overall_metric_expr(ruleset: str) -> str:
    """v26 overall player rank metric -- the ALL-POSITIONS raw-points leaderboard: every position
    scores by its OWN points so nobody is excluded (positional scarcity lives in LAMAR, not here):
      DEF/DST     -> pts_def_std
      K (not skill) -> pts_k_std     (a kicker's fpts is ~0, so it must use its own scoring)
      IDP-only    -> pts_idp_std
      everyone else (offense) -> fpts_{ruleset}
    """
    return (
        "CASE "
        f'WHEN {_positions_expr(DEF_POSITIONS)} THEN COALESCE(a."pts_def_std", 0) '
        f"WHEN {_positions_expr(('K',))} AND NOT {_positions_expr(('QB', 'RB', 'WR', 'TE'))} "
        f'THEN COALESCE(a."pts_k_std", 0) '
        f"WHEN {_positions_expr(IDP_POSITIONS)} AND NOT {_positions_expr(OFFENSE_K_POSITIONS)} "
        f'THEN COALESCE(a."pts_idp_std", 0) '
        f'ELSE COALESCE(a.{qident("fpts_" + ruleset)}, 0) END'
    )


def scoped_rank_jobs(scope: str) -> list[tuple[str, str, str]]:
    """(rank_col, where_sql, order_sql) for one scope, aliases: a = aggregate row, yg = per-year max games.

    Total + _ppg-twin jobs per canonical RankSpec, then overall (+_ppg) per ruleset.
    """
    from multi_league.data_fetchers.aggregate_nfl_stats_fly import rank_specs_for_scope

    if scope == "season":
        gate = f"a.games_played IS NOT NULL AND a.games_played > 0 AND a.games_played >= {SEASON_GAMES_PCT} * yg.mg"
    else:
        gate = f"a.games_played IS NOT NULL AND a.games_played >= {CAREER_MIN_GAMES}"
    prefix = "rank_season" if scope == "season" else "rank_alltime"

    jobs: list[tuple[str, str, str]] = []
    for spec in rank_specs_for_scope(scope):
        pop = _positions_expr(tuple(spec.positions))
        pts = f"a.{qident(spec.points_col)}"
        jobs.append((spec.col, f"{pop} AND {pts} IS NOT NULL", pts))
        jobs.append((f"{spec.col}_ppg", f"{pop} AND {pts} IS NOT NULL AND {gate}", f"({pts} * 1.0 / NULLIF(a.games_played, 0))"))
    # ALL positions in the overall leaderboard (DEF/DST + K + IDP + offense), each by its own
    # points via _overall_metric_expr; gated only to players with a positive metric.
    for rs in OVERALL_RULESETS:
        m = _overall_metric_expr(rs)
        jobs.append((f"{prefix}_overall_{rs}", f"({m}) > 0", m))
        jobs.append((f"{prefix}_overall_{rs}_ppg", f"({m}) > 0 AND {gate}", f"(({m}) * 1.0 / NULLIF(a.games_played, 0))"))
    return jobs


def _scoped_points_columns(scope: str) -> list[str]:
    from multi_league.data_fetchers.aggregate_nfl_stats_fly import rank_specs_for_scope

    cols = {spec.points_col for spec in rank_specs_for_scope(scope)}
    cols.update(f"fpts_{rs}" for rs in OVERALL_RULESETS)
    cols.add("pts_idp_std")
    return sorted(cols)


def _table_columns(conn: duckdb.DuckDBPyConnection, ref: str) -> list[str]:
    return [row[0] for row in conn.execute(f"DESCRIBE {ref}").fetchall()]


def _sidecar_ref(target_schema: str, key: str) -> str:
    return f"{qident(target_schema)}.{qident(SIDECAR_TABLES[key])}"


def _assert_covered(kind: str, family_cols: list[str], produced: set[str]) -> None:
    missing = sorted(set(family_cols) - produced)
    if missing:
        raise RuntimeError(f"{kind}: no recompute rule produces sidecar columns {missing} — schema grew past the wiring; add rules before publishing")


def _require_columns(kind: str, have: set[str], needed: set[str]) -> None:
    missing = sorted(needed - have)
    if missing:
        raise RuntimeError(f"{kind}: base sidecar is missing required source columns {missing}")


def stage_new_week(
    conn: duckdb.DuckDBPyConnection,
    source_ref: str,
    year: int,
    week: int,
    *,
    target_schema: str = "nfl_historical",
    mode: str = "append",
) -> int:
    """Insert a new week of base facts, then compute its 47 window columns.

    ``source_ref`` must hold exactly the base fact columns: the base sidecar's
    schema minus ``_row_uid`` and the window columns (those are derived here).
    """
    base = _sidecar_ref(target_schema, "base")
    base_cols = _table_columns(conn, base)
    window_cols = [c for c in WINDOW_BASE_COLUMNS if c in base_cols]
    fact_cols = [c for c in base_cols if c != "_row_uid" and c not in window_cols]

    src_cols = _table_columns(conn, source_ref)
    if sorted(src_cols) != sorted(fact_cols):
        extra = sorted(set(src_cols) - set(fact_cols))
        missing = sorted(set(fact_cols) - set(src_cols))
        raise RuntimeError(f"stage_new_week: source columns must equal base fact columns; extra={extra}, missing={missing}")

    existing = conn.execute(f"SELECT COUNT(*) FROM {base} WHERE year = ? AND week = ?", [year, week]).fetchone()[0]
    if existing:
        if mode == "append":
            raise RuntimeError(f"stage_new_week: base already has {existing} rows for year={year} week={week}; use mode='replace'")
        conn.execute(f"DELETE FROM {base} WHERE year = ? AND week = ?", [year, week])

    off_scope = conn.execute(f"SELECT COUNT(*) FROM {source_ref} WHERE year IS DISTINCT FROM ? OR week IS DISTINCT FROM ?", [year, week]).fetchone()[0]
    if off_scope:
        raise RuntimeError(f"stage_new_week: source has {off_scope} rows outside (year={year}, week={week})")

    row_hash = _row_hash_expr(fact_cols)
    insert_cols = ", ".join(qident(c) for c in fact_cols + ["_row_uid"])
    select_cols = ", ".join(qident(c) for c in fact_cols)
    conn.execute(
        f"""
        INSERT INTO {base} ({insert_cols})
        SELECT {select_cols}, row_number() OVER (PARTITION BY player_week ORDER BY {row_hash}) AS _row_uid
        FROM {source_ref}
        """
    )
    inserted = conn.execute(f"SELECT COUNT(*) FROM {base} WHERE year = ? AND week = ?", [year, week]).fetchone()[0]
    _compute_window_columns_for_week(conn, base, base_cols, year, week)
    return int(inserted)


def _compute_window_columns_for_week(
    conn: duckdb.DuckDBPyConnection, base: str, base_cols: list[str], year: int, week: int
) -> None:
    """wave42 window semantics, written only onto the new (year, week) rows.

    Trailing windows never change historical rows, so stamping the new rows is
    equivalent to wave42's full-table UPDATE for every prior row.
    """
    have = set(base_cols)
    for td in PPG_TDS:
        for ppr in PPG_PPRS:
            f = qident(f"fpts_{td}_{ppr}")
            if f"fpts_{td}_{ppr}" not in have:
                if any(f"{fam}_{td}_{ppr}" in have for fam in ("rolling_3", "rolling_5", "rolling_total")):
                    raise RuntimeError(f"window columns for {td}_{ppr} exist but fpts_{td}_{ppr} is missing from base")
                continue
            hist = f"""
                SELECT player_week, _row_uid, year, week, {f} AS f,
                       "NFL_player_id" AS pid
                FROM {base}
                WHERE {f} IS NOT NULL
                  AND "NFL_player_id" IN (SELECT DISTINCT "NFL_player_id" FROM {base} WHERE year = {int(year)} AND week = {int(week)})
            """
            updates: list[tuple[str, str]] = []
            if f"rolling_total_{td}_{ppr}" in have:
                updates.append((
                    f"rolling_total_{td}_{ppr}",
                    f"""SELECT player_week, _row_uid, ROUND(SUM(f) OVER (PARTITION BY pid, year
                        ORDER BY week, player_week, _row_uid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS v
                        FROM ({hist})""",
                ))
            if f"rolling_3_{td}_{ppr}" in have:
                updates.append((
                    f"rolling_3_{td}_{ppr}",
                    f"""SELECT player_week, _row_uid, ROUND(AVG(f) OVER (PARTITION BY pid
                        ORDER BY year, week, player_week, _row_uid ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS v
                        FROM ({hist})""",
                ))
            if f"rolling_5_{td}_{ppr}" in have:
                updates.append((
                    f"rolling_5_{td}_{ppr}",
                    f"""SELECT player_week, _row_uid, ROUND(AVG(f) OVER (PARTITION BY pid
                        ORDER BY year, week, player_week, _row_uid ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) AS v
                        FROM ({hist})""",
                ))
            for col, sub in updates:
                conn.execute(
                    f"""
                    UPDATE {base} t SET {qident(col)} = c.v
                    FROM ({sub}) c
                    WHERE t.player_week = c.player_week AND t._row_uid = c._row_uid
                      AND t.year = {int(year)} AND t.week = {int(week)}
                    """
                )
    for col, pts, pos in (("rolling_total_def", "pts_def_std", "DEF"), ("rolling_total_k", "pts_k_yds", "K")):
        if col not in have:
            continue
        if pts not in have or "nfl_position" not in have:
            raise RuntimeError(f"{col} exists but its sources ({pts}, nfl_position) are missing from base")
        conn.execute(
            f"""
            UPDATE {base} t SET {qident(col)} = c.v
            FROM (
              SELECT player_week, _row_uid, ROUND(SUM({qident(pts)}) OVER (PARTITION BY "NFL_player_id", year
                     ORDER BY week, player_week, _row_uid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS v
              FROM {base}
              WHERE {qident(pts)} IS NOT NULL AND nfl_position = '{pos}'
                AND "NFL_player_id" IN (SELECT DISTINCT "NFL_player_id" FROM {base} WHERE year = {int(year)} AND week = {int(week)})
            ) c
            WHERE t.player_week = c.player_week AND t._row_uid = c._row_uid
              AND t.year = {int(year)} AND t.week = {int(week)}
            """
        )


def recompute_base_windows(conn: duckdb.DuckDBPyConnection, *, target_schema: str = "nfl_historical") -> None:
    """Bulk (full-rebuild) recompute of every base window column over the whole
    base sidecar — the corrections / preseason path. wave42 semantics, one UPDATE
    per column. The per-week ``_compute_window_columns_for_week`` is the weekly
    incremental equivalent; this is its full-table form.
    """
    base = _sidecar_ref(target_schema, "base")
    have = set(_table_columns(conn, base))
    # player_week, _row_uid finalize the window ORDER BY so the trailing frame is
    # DETERMINISTIC even on the ~89 pre-1953 duplicate-(player,year,week) rows where
    # (year, week) alone is ambiguous. wave42 lacked this tiebreak, so live's frozen
    # values for those ancient rows are non-deterministic and won't be matched — but a
    # rebuild is now reproducible run-to-run.
    for td in PPG_TDS:
        for ppr in PPG_PPRS:
            f = qident(f"fpts_{td}_{ppr}")
            if f"fpts_{td}_{ppr}" not in have:
                continue
            specs = [
                (f"rolling_total_{td}_{ppr}",
                 f"""SELECT player_week, _row_uid, ROUND(SUM({f}) OVER (PARTITION BY "NFL_player_id", year
                     ORDER BY week, player_week, _row_uid ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS v
                     FROM {base} WHERE {f} IS NOT NULL"""),
                (f"rolling_3_{td}_{ppr}",
                 f"""SELECT player_week, _row_uid, ROUND(AVG({f}) OVER (PARTITION BY "NFL_player_id"
                     ORDER BY year, week, player_week, _row_uid ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS v
                     FROM {base} WHERE {f} IS NOT NULL"""),
                (f"rolling_5_{td}_{ppr}",
                 f"""SELECT player_week, _row_uid, ROUND(AVG({f}) OVER (PARTITION BY "NFL_player_id"
                     ORDER BY year, week, player_week, _row_uid ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), 2) AS v
                     FROM {base} WHERE {f} IS NOT NULL"""),
            ]
            for col, sub in specs:
                if col not in have:
                    continue
                conn.execute(
                    f"""UPDATE {base} t SET {qident(col)} = c.v FROM ({sub}) c
                        WHERE t.player_week = c.player_week AND t._row_uid = c._row_uid"""
                )
    for col, pts, pos in (("rolling_total_def", "pts_def_std", "DEF"), ("rolling_total_k", "pts_k_yds", "K")):
        if col not in have or pts not in have or "nfl_position" not in have:
            continue
        conn.execute(
            f"""
            UPDATE {base} t SET {qident(col)} = c.v
            FROM (
              SELECT player_week, _row_uid, ROUND(SUM({qident(pts)}) OVER (PARTITION BY "NFL_player_id", year
                     ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 2) AS v
              FROM {base} WHERE {qident(pts)} IS NOT NULL AND nfl_position = '{pos}'
            ) c
            WHERE t.player_week = c.player_week AND t._row_uid = c._row_uid
            """
        )


def rebuild_weekly_rank_partition(
    conn: duckdb.DuckDBPyConnection, year: int, week: int, *, target_schema: str = "nfl_historical"
) -> int:
    """wave40: DELETE+INSERT the (year, week) partition of the weekly rank sidecar."""
    base = _sidecar_ref(target_schema, "base")
    sidecar = _sidecar_ref(target_schema, "weekly_rank")
    rank_cols = [c for c in _table_columns(conn, sidecar) if c not in ("player_week", "_row_uid", "year", "week")]
    specs = [(col, pts, pos) for col, pts, pos in weekly_rank_specs() if col in set(rank_cols)]
    _assert_covered("weekly ranks", rank_cols, {col for col, _, _ in specs})
    base_have = set(_table_columns(conn, base))
    _require_columns("weekly ranks", base_have, {pts for _, pts, _ in specs} | {"position", "season_type", "NFL_player_id"})

    conn.execute("DROP TABLE IF EXISTS _wk_ranks")
    conn.execute(
        f"""
        CREATE TEMP TABLE _wk_ranks AS
        SELECT player_week, _row_uid, year, week, season_type, "NFL_player_id", position,
               {", ".join(sorted({qident(pts) for _, pts, _ in specs}))}
        FROM {base} WHERE year = {int(year)} AND week = {int(week)}
        """
    )
    for col, pts, positions in specs:
        conn.execute(f"ALTER TABLE _wk_ranks ADD COLUMN {qident(col)} INTEGER")
        conn.execute(
            f"""
            UPDATE _wk_ranks t SET {qident(col)} = c.r
            FROM (
              SELECT player_week, _row_uid,
                     CAST(ROW_NUMBER() OVER (PARTITION BY year, week, season_type
                          ORDER BY TRY_CAST(a.{qident(pts)} AS DOUBLE) DESC, a."NFL_player_id") AS INTEGER) AS r
              FROM _wk_ranks a
              WHERE {_positions_expr(positions)} AND a.{qident(pts)} IS NOT NULL
            ) c
            WHERE t.player_week = c.player_week AND t._row_uid = c._row_uid
            """
        )
    conn.execute(f"DELETE FROM {sidecar} WHERE year = ? AND week = ?", [year, week])
    out_cols = ", ".join(qident(c) for c in ["player_week", "_row_uid", "year", "week"] + rank_cols)
    conn.execute(f"INSERT INTO {sidecar} ({out_cols}) SELECT {out_cols} FROM _wk_ranks")
    n = conn.execute(f"SELECT COUNT(*) FROM {sidecar} WHERE year = ? AND week = ?", [year, week]).fetchone()[0]
    conn.execute("DROP TABLE IF EXISTS _wk_ranks")
    return int(n)


def _build_scoped_aggregate(
    conn: duckdb.DuckDBPyConnection, base: str, scope: str, years: list[int] | None
) -> None:
    """Natural-grain REG-season aggregate (_scoped_agg) + per-year max games (_scoped_yg)."""
    base_have = set(_table_columns(conn, base))
    points = [c for c in _scoped_points_columns(scope) if c in base_have]
    # SUM(COALESCE(c, 0)) matches production aggregate_nfl_stats_fly.adjusted_sum_expr:
    # a player whose only games have NULL points aggregates to 0 (not NULL), so the
    # rank population still includes them (live ranks 0-point players by NFL_player_id).
    sums = ",\n            ".join(f"ROUND(SUM(COALESCE({qident(c)}, 0)), 4) AS {qident(c)}" for c in points)
    reg_filter = """
        "NFL_player_id" IS NOT NULL AND year IS NOT NULL AND week IS NOT NULL AND season_type = 'REG'
    """
    conn.execute("DROP TABLE IF EXISTS _scoped_agg")
    conn.execute("DROP TABLE IF EXISTS _scoped_yg")
    if scope == "season":
        # Season: position is constant within a (player, year), so ANY_VALUE is exact and
        # cheap (golden-diffed: 0 mismatch on the 65 canonical season families).
        _require_columns("season aggregate", base_have, {"position", "season_type", "NFL_player_id"})
        year_filter = f"AND CAST(year AS INTEGER) IN ({', '.join(str(int(y)) for y in years)})" if years else ""
        conn.execute(
            f"""
            CREATE TEMP TABLE _scoped_agg AS
            SELECT "NFL_player_id", CAST(year AS INTEGER) AS year,
                   ANY_VALUE(position) AS position,
                   CAST(COUNT(*) AS INTEGER) AS games_played,
                   {sums}
            FROM {base}
            WHERE {reg_filter} {year_filter}
            GROUP BY 1, 2
            """
        )
        conn.execute("CREATE TEMP TABLE _scoped_yg AS SELECT year, MAX(games_played) AS mg FROM _scoped_agg GROUP BY year")
    else:
        # Career: read the stored per-player primary_position (bio-anchored, from
        # build_primary_position_v26) instead of a MODE over every career game -- position
        # is a player attribute, not a per-rebuild computation. It is constant per player,
        # so ANY_VALUE returns it exactly. Two-way players rank by their bio primary slot.
        _require_columns("career aggregate", base_have, {"primary_position", "season_type", "NFL_player_id"})
        conn.execute(
            f"""
            CREATE TEMP TABLE _scoped_agg AS
            SELECT "NFL_player_id",
                   ANY_VALUE(primary_position) AS position,
                   CAST(COUNT(*) AS INTEGER) AS games_played,
                   {sums}
            FROM {base}
            WHERE {reg_filter}
            GROUP BY 1
            """
        )


def _rank_scoped_aggregate(conn: duckdb.DuckDBPyConnection, scope: str, wanted: set[str]) -> list[str]:
    """Run the v26 rank jobs against _scoped_agg; returns rank columns produced."""
    agg_have = set(_table_columns(conn, "_scoped_agg"))
    jobs = [(col, where, order) for col, where, order in scoped_rank_jobs(scope) if col in wanted]
    _assert_covered(f"{scope} ranks", sorted(wanted), {col for col, _, _ in jobs})
    season = scope == "season"
    key_select = 'a."NFL_player_id", a.year' if season else 'a."NFL_player_id"'
    partition = "PARTITION BY a.year" if season else ""
    frm = "_scoped_agg a JOIN _scoped_yg yg ON a.year = yg.year" if season else "_scoped_agg a"
    join_back = 't."NFL_player_id" = r."NFL_player_id"' + (" AND t.year = r.year" if season else "")
    for col, where, order in jobs:
        needed = {p for p in _scoped_points_columns(scope) if f"a.{qident(p)}" in where or f"a.{qident(p)}" in order}
        _require_columns(f"{scope} rank {col}", agg_have, needed)
        conn.execute(f"ALTER TABLE _scoped_agg ADD COLUMN {qident(col)} INTEGER")
        conn.execute(
            f"""
            UPDATE _scoped_agg t SET {qident(col)} = r.rn
            FROM (
              SELECT {key_select},
                     CAST(ROW_NUMBER() OVER ({partition} ORDER BY {order} DESC, a."NFL_player_id" ASC) AS INTEGER) AS rn
              FROM {frm}
              WHERE {where}
            ) r
            WHERE {join_back}
            """
        )
    return [col for col, _, _ in jobs]


def rebuild_season_partitions(
    conn: duckdb.DuckDBPyConnection, years: list[int], *, target_schema: str = "nfl_historical"
) -> int:
    """Recompute all season-family columns for the given year partitions (vertical grain)."""
    base = _sidecar_ref(target_schema, "base")
    sidecar = _sidecar_ref(target_schema, "season")
    family = [c for c in _table_columns(conn, sidecar) if c not in ("player_week", "_row_uid", "year")]
    base_have = set(_table_columns(conn, base))
    years = sorted({int(y) for y in years})
    years_sql = ", ".join(str(y) for y in years)

    rank_wanted = {c for c in family if c.startswith("rank_season_")}
    ppg_wanted = [c for c in family if c not in rank_wanted]
    produced: set[str] = set()

    conn.execute("DROP TABLE IF EXISTS _sea_new")
    conn.execute(
        f"""
        CREATE TEMP TABLE _sea_new AS
        SELECT player_week, _row_uid, CAST(year AS INTEGER) AS year, week, "NFL_player_id"
        FROM {base} WHERE CAST(year AS INTEGER) IN ({years_sql})
        """
    )

    # wave42 group/window PPG families, scoped to the target years.
    for td in PPG_TDS:
        for ppr in PPG_PPRS:
            v = f"{td}_{ppr}"
            f = qident(f"fpts_{v}")
            cols = {
                "ppg_season": f"ppg_season_{v}",
                "consistency": f"consistency_{v}",
                "weighted": f"weighted_ppg_{v}",
                "next_year": f"avg_pts_next_year_{v}",
            }
            variant_cols = [c for c in cols.values() if c in set(ppg_wanted)]
            if not variant_cols:
                continue
            if f"fpts_{v}" not in base_have:
                raise RuntimeError(f"season family needs fpts_{v} in base to compute {variant_cols}")
            for c in variant_cols:
                conn.execute(f"ALTER TABLE _sea_new ADD COLUMN {qident(c)} DOUBLE")
                produced.add(c)
            group_years_sql = ", ".join(str(y) for y in sorted({*years, *(y + 1 for y in years)}))
            conn.execute("DROP TABLE IF EXISTS _grp")
            conn.execute(
                f"""
                CREATE TEMP TABLE _grp AS
                SELECT "NFL_player_id", CAST(year AS INTEGER) AS year, AVG({f}) AS avg_pts,
                       CASE WHEN AVG({f}) > 0 THEN STDDEV({f}) / AVG({f}) ELSE 0 END AS cv
                FROM {base}
                WHERE {f} IS NOT NULL AND CAST(year AS INTEGER) IN ({group_years_sql})
                GROUP BY 1, 2
                """
            )
            if cols["ppg_season"] in produced:
                conn.execute(
                    f"""UPDATE _sea_new t SET {qident(cols["ppg_season"])} = ROUND(c.avg_pts, 2)
                        FROM _grp c WHERE t."NFL_player_id" = c."NFL_player_id" AND t.year = c.year"""
                )
            if cols["consistency"] in produced:
                conn.execute(
                    f"""UPDATE _sea_new t SET {qident(cols["consistency"])} = ROUND(c.cv, 3)
                        FROM _grp c WHERE t."NFL_player_id" = c."NFL_player_id" AND t.year = c.year"""
                )
            if cols["next_year"] in produced:
                conn.execute(
                    f"""UPDATE _sea_new t SET {qident(cols["next_year"])} = ROUND(c.avg_pts, 2)
                        FROM _grp c WHERE t."NFL_player_id" = c."NFL_player_id" AND t.year = c.year - 1"""
                )
            if cols["weighted"] in produced:
                conn.execute(
                    f"""
                    UPDATE _sea_new t SET {qident(cols["weighted"])} = ROUND(c.w, 2)
                    FROM (
                      SELECT player_week, _row_uid, (
                        COALESCE(LAG(f, 0) OVER w * 5, 0) + COALESCE(LAG(f, 1) OVER w * 4, 0) +
                        COALESCE(LAG(f, 2) OVER w * 3, 0) + COALESCE(LAG(f, 3) OVER w * 2, 0) +
                        COALESCE(LAG(f, 4) OVER w * 1, 0)) / NULLIF(
                        (CASE WHEN LAG(f, 0) OVER w IS NOT NULL THEN 5 ELSE 0 END) +
                        (CASE WHEN LAG(f, 1) OVER w IS NOT NULL THEN 4 ELSE 0 END) +
                        (CASE WHEN LAG(f, 2) OVER w IS NOT NULL THEN 3 ELSE 0 END) +
                        (CASE WHEN LAG(f, 3) OVER w IS NOT NULL THEN 2 ELSE 0 END) +
                        (CASE WHEN LAG(f, 4) OVER w IS NOT NULL THEN 1 ELSE 0 END), 0) AS w
                      FROM (
                        SELECT player_week, _row_uid, year, week, {f} AS f, "NFL_player_id"
                        FROM {base}
                        WHERE {f} IS NOT NULL AND "NFL_player_id" IN (
                          SELECT DISTINCT "NFL_player_id" FROM {base} WHERE CAST(year AS INTEGER) IN ({years_sql})
                        )
                      )
                      WINDOW w AS (PARTITION BY "NFL_player_id" ORDER BY year, week)
                    ) c
                    WHERE t.player_week = c.player_week AND t._row_uid = c._row_uid
                    """
                )
            conn.execute("DROP TABLE IF EXISTS _grp")
    _assert_covered("season ppg", ppg_wanted, produced)

    # v26 season rank jobs on the natural-grain aggregate, denormalized wave48-style.
    _build_scoped_aggregate(conn, base, "season", years)
    rank_cols = _rank_scoped_aggregate(conn, "season", rank_wanted)
    for chunk_start in range(0, len(rank_cols), 24):
        chunk = rank_cols[chunk_start : chunk_start + 24]
        for c in chunk:
            conn.execute(f"ALTER TABLE _sea_new ADD COLUMN {qident(c)} INTEGER")
        sets = ", ".join(f"{qident(c)} = r.{qident(c)}" for c in chunk)
        conn.execute(
            f"""UPDATE _sea_new t SET {sets} FROM _scoped_agg r
                WHERE t."NFL_player_id" = r."NFL_player_id" AND t.year = r.year"""
        )

    conn.execute(f"DELETE FROM {sidecar} WHERE CAST(year AS INTEGER) IN ({years_sql})")
    out_cols = ", ".join(qident(c) for c in ["player_week", "_row_uid", "year"] + family)
    conn.execute(f"INSERT INTO {sidecar} ({out_cols}) SELECT {out_cols} FROM _sea_new")
    n = conn.execute(f"SELECT COUNT(*) FROM {sidecar} WHERE CAST(year AS INTEGER) IN ({years_sql})").fetchone()[0]
    for t in ("_sea_new", "_scoped_agg", "_scoped_yg"):
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    return int(n)


def rebuild_career_sidecar(conn: duckdb.DuckDBPyConnection, *, target_schema: str = "nfl_historical") -> int:
    """Full rewrite of the career sidecar (career ranks/PPG shift globally each week)."""
    base = _sidecar_ref(target_schema, "base")
    sidecar = _sidecar_ref(target_schema, "career")
    family = [c for c in _table_columns(conn, sidecar) if c not in ("player_week", "_row_uid", "year")]
    base_have = set(_table_columns(conn, base))

    rank_wanted = {c for c in family if c.startswith("rank_alltime_")}
    ppg_wanted = [c for c in family if c not in rank_wanted]

    conn.execute("DROP TABLE IF EXISTS _car_new")
    conn.execute(
        f"""
        CREATE TEMP TABLE _car_new AS
        SELECT player_week, _row_uid, year, "NFL_player_id" FROM {base}
        """
    )
    produced: set[str] = set()
    for td in PPG_TDS:
        for ppr in PPG_PPRS:
            col = f"ppg_alltime_{td}_{ppr}"
            if col not in set(ppg_wanted):
                continue
            f = qident(f"fpts_{td}_{ppr}")
            if f"fpts_{td}_{ppr}" not in base_have:
                raise RuntimeError(f"career family needs fpts_{td}_{ppr} in base to compute {col}")
            conn.execute(f"ALTER TABLE _car_new ADD COLUMN {qident(col)} DOUBLE")
            conn.execute(
                f"""
                UPDATE _car_new t SET {qident(col)} = ROUND(c.avg_pts, 2)
                FROM (SELECT "NFL_player_id", AVG({f}) AS avg_pts FROM {base}
                      WHERE {f} IS NOT NULL GROUP BY 1) c
                WHERE t."NFL_player_id" = c."NFL_player_id"
                """
            )
            produced.add(col)
    _assert_covered("career ppg", ppg_wanted, produced)

    _build_scoped_aggregate(conn, base, "alltime", None)
    rank_cols = _rank_scoped_aggregate(conn, "alltime", rank_wanted)
    for chunk_start in range(0, len(rank_cols), 24):
        chunk = rank_cols[chunk_start : chunk_start + 24]
        for c in chunk:
            conn.execute(f"ALTER TABLE _car_new ADD COLUMN {qident(c)} INTEGER")
        sets = ", ".join(f"{qident(c)} = r.{qident(c)}" for c in chunk)
        conn.execute(f'UPDATE _car_new t SET {sets} FROM _scoped_agg r WHERE t."NFL_player_id" = r."NFL_player_id"')

    conn.execute(f"DELETE FROM {sidecar}")
    out_cols = ", ".join(qident(c) for c in ["player_week", "_row_uid", "year"] + family)
    conn.execute(f"INSERT INTO {sidecar} ({out_cols}) SELECT {out_cols} FROM _car_new")
    n = conn.execute(f"SELECT COUNT(*) FROM {sidecar}").fetchone()[0]
    for t in ("_car_new", "_scoped_agg", "_scoped_yg"):
        conn.execute(f"DROP TABLE IF EXISTS {t}")
    return int(n)


def maintain_primary_position(
    conn: duckdb.DuckDBPyConnection,
    source_ref: str,
    *,
    bio_ref: str,
    target_schema: str = "nfl_historical",
    out_ref: str = "_pp_staged_source",
) -> dict:
    """Stamp the stable per-player ``primary_position`` onto a new week's base facts.

    ``primary_position`` is a player ATTRIBUTE that the career aggregate reads via
    ``ANY_VALUE`` (rebuild_career_sidecar), so it MUST stay constant per player.
    This is the weekly, incremental equivalent of
    ``scripts/sota_recon/build_primary_position_v26`` (the one-time full pass):

      - an authoritative player-bio position corrects the stored value for that
        player across the base sidecar, so a real position change reaches career
        ranks without a full rebuild;
      - without a bio value, an existing player keeps their stored position;
      - a genuinely new player (rookie / first appearance) gets
        ``COALESCE(bio.nfl_position, MODE(position) over their new-week rows)`` —
        the exact one-time rule.

    ``bio_ref`` is any SQL-referenceable relation exposing ``NFL_player_id`` and
    ``nfl_position`` (e.g. ``___ops.nfl_historical.player_bio`` on Fly, or
    ``read_parquet('.../player_bio.parquet')`` offline). Writes ``out_ref`` =
    ``source_ref`` with a correct ``primary_position`` column (added if absent,
    replaced if present) so ``stage_new_week`` accepts it. Returns
    ``{"players", "existing", "new", "null_primary"}``.
    """
    base = _sidecar_ref(target_schema, "base")
    _require_columns("primary_position maintenance", set(_table_columns(conn, base)), {"primary_position", "NFL_player_id"})
    src_cols = _table_columns(conn, source_ref)
    missing_src = {"NFL_player_id", "position"} - set(src_cols)
    if missing_src:
        raise RuntimeError(f"maintain_primary_position: source is missing required columns {sorted(missing_src)}")

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _pp_resolved AS
        WITH ids AS (
            SELECT DISTINCT "NFL_player_id" AS nfl_id FROM {source_ref} WHERE "NFL_player_id" IS NOT NULL
        ),
        existing AS (
            SELECT "NFL_player_id" AS nfl_id, ANY_VALUE(primary_position) AS pp
            FROM {base}
            WHERE primary_position IS NOT NULL AND primary_position <> ''
            GROUP BY 1
        ),
        dom AS (
            SELECT "NFL_player_id" AS nfl_id, MODE(position) AS dompos
            FROM {source_ref}
            WHERE "NFL_player_id" IS NOT NULL AND position IS NOT NULL AND position <> ''
            GROUP BY 1
        ),
        bio AS (
            SELECT "NFL_player_id" AS nfl_id, NULLIF(nfl_position, '') AS bpos FROM {bio_ref}
        )
        SELECT i.nfl_id AS "NFL_player_id",
               COALESCE(b.bpos, e.pp, d.dompos) AS primary_position,
               CASE WHEN e.pp IS NOT NULL THEN 1 ELSE 0 END AS _is_existing
        FROM ids i
        LEFT JOIN existing e ON e.nfl_id = i.nfl_id
        LEFT JOIN bio b ON b.nfl_id = i.nfl_id
        LEFT JOIN dom d ON d.nfl_id = i.nfl_id
        """
    )

    # Keep the per-player attribute constant across existing and incoming rows.
    # This touches only player IDs present in the new weekly source.
    conn.execute(
        f"""
        UPDATE {base} AS target
        SET primary_position = resolved.primary_position
        FROM _pp_resolved AS resolved
        WHERE target."NFL_player_id" = resolved."NFL_player_id"
          AND resolved.primary_position IS NOT NULL
          AND target.primary_position IS DISTINCT FROM resolved.primary_position
        """
    )

    keep = "s.* EXCLUDE (primary_position)" if "primary_position" in src_cols else "s.*"
    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {out_ref} AS
        SELECT {keep}, p.primary_position AS primary_position
        FROM {source_ref} s
        LEFT JOIN _pp_resolved p ON p."NFL_player_id" = s."NFL_player_id"
        """
    )

    players = int(conn.execute("SELECT COUNT(*) FROM _pp_resolved").fetchone()[0])
    existing = int(conn.execute("SELECT COUNT(*) FROM _pp_resolved WHERE _is_existing = 1").fetchone()[0])
    null_primary = int(
        conn.execute(
            f"""SELECT COUNT(*) FROM {out_ref}
                WHERE position IS NOT NULL AND position <> ''
                  AND (primary_position IS NULL OR primary_position = '')"""
        ).fetchone()[0]
    )
    conn.execute("DROP TABLE IF EXISTS _pp_resolved")
    return {"players": players, "existing": existing, "new": players - existing, "null_primary": null_primary}


def apply_weekly_update(
    conn: duckdb.DuckDBPyConnection,
    source_ref: str,
    year: int,
    week: int,
    *,
    target_schema: str = "nfl_historical",
    mode: str = "append",
    bio_ref: str | None = None,
) -> dict:
    """One weekly NFL update against the sidecars. Returns a scope manifest.

    Write scope (everything else is untouched — the baseline diff can hold us
    to exactly this): base += (year, week); weekly_ranks = (year, week);
    season = year partitions {year-1, year}; career = full rewrite.

    ``bio_ref`` (WS-C): when given, ``maintain_primary_position`` stamps the
    stable per-player ``primary_position`` on the incoming week first — required
    because the raw fetch/build plane emits ``position`` but not the denormalized
    ``primary_position`` the career aggregate depends on, and rookies have no
    prior row to inherit it from. When ``None``, the source must already carry a
    correct ``primary_position`` (the full-rebuild / corrections path).
    """
    pp_stats: dict | None = None
    if bio_ref is not None:
        pp_stats = maintain_primary_position(
            conn, source_ref, bio_ref=bio_ref, target_schema=target_schema, out_ref="_pp_staged_source"
        )
        source_ref = "_pp_staged_source"
    inserted = stage_new_week(conn, source_ref, year, week, target_schema=target_schema, mode=mode)
    weekly_rows = rebuild_weekly_rank_partition(conn, year, week, target_schema=target_schema)
    season_years = [int(year) - 1, int(year)]
    base = _sidecar_ref(target_schema, "base")
    prior_years = {int(r[0]) for r in conn.execute(f"SELECT DISTINCT CAST(year AS INTEGER) FROM {base}").fetchall()}
    season_years = [y for y in season_years if y in prior_years]
    season_rows = rebuild_season_partitions(conn, season_years, target_schema=target_schema)
    career_rows = rebuild_career_sidecar(conn, target_schema=target_schema)
    manifest = {
        "year": int(year),
        "week": int(week),
        "base_rows_inserted": inserted,
        "weekly_rank_rows": weekly_rows,
        "season_years_rewritten": season_years,
        "season_rows": season_rows,
        "career_rows": career_rows,
    }
    if pp_stats is not None:
        manifest["primary_position"] = pp_stats
    return manifest


def build_ops_promotion_bundle(
    conn: duckdb.DuckDBPyConnection,
    *,
    tables: list[str],
    out_path: Path,
    source_schema: str = "nfl_historical",
) -> dict:
    """Write a .duckdb bundle for /merge-ops (per-table CREATE OR REPLACE)."""
    out_path = Path(out_path)
    out_path.unlink(missing_ok=True)
    conn.execute(f"ATTACH '{out_path.as_posix()}' AS _bundle")
    try:
        manifest_tables: dict[str, int] = {}
        for table in tables:
            src = f"{qident(source_schema)}.{qident(table)}"
            conn.execute(f"CREATE TABLE _bundle.{qident(table)} AS SELECT * FROM {src}")
            manifest_tables[table] = int(conn.execute(f"SELECT COUNT(*) FROM _bundle.{qident(table)}").fetchone()[0])
    finally:
        conn.execute("DETACH _bundle")

    manifest = {
        "created_at": datetime.now(UTC).isoformat(),
        "bundle_path": str(out_path),
        "sha256": _sha256_file(out_path),
        "size_mb": round(out_path.stat().st_size / (1024 * 1024), 2),
        "tables": manifest_tables,
    }
    manifest_path = out_path.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", required=True, help="Local .duckdb snapshot holding the wide table")
    parser.add_argument("--wide-schema", default="nfl_historical")
    parser.add_argument("--out", required=True, help="Output directory for the promotion bundle")
    parser.add_argument("--grain", choices=["vertical", "natural"], default="vertical")
    args = parser.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    source_ref = f"{qident(args.wide_schema)}.{qident(WIDE_TABLE)}"

    conn = duckdb.connect(args.source)
    try:
        t0 = time.perf_counter()
        counts = split_wide_to_sidecars(conn, source_ref=source_ref, target_schema=args.wide_schema, grain=args.grain)
        print(f"[build-full-ops] sidecars built in {time.perf_counter() - t0:.1f}s: {counts}")

        create_compatibility_view(conn, source_ref=source_ref, target_schema=args.wide_schema, grain=args.grain)
        diff = validate_compatibility(conn, source_ref=source_ref, target_schema=args.wide_schema)
        if diff:
            print(f"[build-full-ops] FAIL: compatibility view differs from wide table by {diff} rows")
            return 1
        print("[build-full-ops] compatibility view reproduces the wide table exactly (diff = 0)")

        manifest = build_ops_promotion_bundle(
            conn,
            tables=list(SIDECAR_TABLES.values()),
            out_path=out_dir / "ops_sidecars.duckdb",
            source_schema=args.wide_schema,
        )
        print(f"[build-full-ops] promotion bundle: {manifest['size_mb']} MB, sha256 {manifest['sha256'][:12]}...")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
