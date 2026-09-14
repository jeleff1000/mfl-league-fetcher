#!/usr/bin/env python3
"""Audit and optionally apply PFR usage/advanced player-game atoms.

This script compares the local PFR boxscore/context scrape against the live
Fly supertable, writes coverage/divergence artifacts, and can update only the
columns where PFR is the one clear source of truth:

* snap counts and starter flags from boxscore starters/snap tables
* PFR-only 2018+ advanced passing/rushing/receiving/defense atoms
* missing player_bio combine measurables by exact PFR id

It does not overwrite nflverse/PFR overlap atoms by default. Those are audited
for divergence and left alone unless a later reviewed package explicitly makes
that source-of-truth call.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))
sys.path.insert(0, str(ROOT))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402
from scripts.apply_supertable_raw_atom_packages import (  # noqa: E402
    q_ident,
    q_table,
    rows_values_sql,
    type_for_col,
)


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
CATALOG_ROOT = ORGANIZED_ROOT / "_catalog"
PFR_BOXSCORES_ROOT = ORGANIZED_ROOT / "pfr_boxscores"
PFR_CONTEXT_ROOT = ORGANIZED_ROOT / "pfr_context"
TARGET_TABLE = "nfl_historical.nfl_player_stats_all"
BIO_TABLE = "nfl_historical.player_bio"
CONFIRM_TOKEN = "APPLY_PFR_USAGE_ADVANCED"
EPSILON = 1e-9

TEAM_CODE_ALIASES = {
    "GNB": "GB",
    "KAN": "KC",
    "NOR": "NO",
    "NWE": "NE",
    "SFO": "SF",
    "TAM": "TB",
}

SCHEMA_ADDITIONS: dict[str, str] = {
    # Usage and starter context.
    "offense_snaps": "DOUBLE",
    "offense_snap_pct": "DOUBLE",
    "defense_snaps": "DOUBLE",
    "defense_snap_pct": "DOUBLE",
    "special_teams_snaps": "DOUBLE",
    "special_teams_snap_pct": "DOUBLE",
    "is_starter": "BOOLEAN",
    "starter_position": "VARCHAR",
    # PFR-only advanced receiving.
    "receiving_adot": "DOUBLE",
    "receiving_broken_tackles": "DOUBLE",
    "receiving_drops": "DOUBLE",
    "receiving_target_interceptions": "DOUBLE",
    "receiving_pass_rating": "DOUBLE",
    # PFR-only advanced rushing.
    "rushing_yards_before_contact": "DOUBLE",
    "rushing_yards_after_contact": "DOUBLE",
    "rushing_broken_tackles": "DOUBLE",
    # PFR-only advanced passing.
    "passing_drops": "DOUBLE",
    "passing_poor_throws": "DOUBLE",
    "passing_blitzed": "DOUBLE",
    "passing_hurried": "DOUBLE",
    "passing_hits": "DOUBLE",
    "passing_pressured": "DOUBLE",
    "rushing_scrambles": "DOUBLE",
    # PFR-only advanced defense/coverage/pressure.
    "def_targets_allowed": "DOUBLE",
    "def_completions_allowed": "DOUBLE",
    "def_completion_yards_allowed": "DOUBLE",
    "def_completion_tds_allowed": "DOUBLE",
    "def_passer_rating_allowed": "DOUBLE",
    "def_air_yards_allowed": "DOUBLE",
    "def_yards_after_catch_allowed": "DOUBLE",
    "def_blitzes": "DOUBLE",
    "def_hurries": "DOUBLE",
    "def_knockdowns": "DOUBLE",
    "def_pressures": "DOUBLE",
    "def_tackles_missed": "DOUBLE",
}

NEW_SUPERTABLE_COLUMNS = list(SCHEMA_ADDITIONS)

OVERLAP_COLUMNS: dict[str, str] = {
    "pfr_receiving_first_downs": "receiving_first_downs",
    "pfr_receiving_air_yards": "receiving_air_yards",
    "pfr_receiving_yards_after_catch": "receiving_yards_after_catch",
    "pfr_rushing_first_downs": "rushing_first_downs",
    "pfr_passing_first_downs": "passing_first_downs",
    "pfr_passing_air_yards": "passing_air_yards",
    "pfr_passing_yards_after_catch": "passing_yards_after_catch",
}

BIO_COMBINE_MAP: dict[str, str] = {
    "forty_yd": "forty",
    "bench_reps": "bench",
    "vertical": "vertical",
    "broad_jump": "broad_jump",
    "cone": "cone",
    "shuttle": "shuttle",
}


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def sql_path(path: Path) -> str:
    return str(path).replace("\\", "/").replace("'", "''")


def norm_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def pct_to_float(value: Any) -> float | None:
    text = norm_text(value)
    if not text:
        return None
    text = text.replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def q_lit(value: Any) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def latest_retab_dir() -> Path:
    dirs = sorted(CATALOG_ROOT.glob("pfr_boxscore_retabs_*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for path in dirs:
        if (path / "pfr_game_team_dim.parquet").exists() and (path / "pfr_player_game_position_hint.parquet").exists():
            return path
    raise FileNotFoundError("No pfr_boxscore_retabs_* dir with game/team and position hint parquet found")


def fetch_schema(reader: FlyReader, table_name: str) -> dict[str, str]:
    df = reader.query_df(f"DESCRIBE {table_name}", database="___ops")
    return dict(zip(df["column_name"].astype(str), df["column_type"].astype(str), strict=False))


def fetch_live_rows(reader: FlyReader, output_dir: Path, schema: dict[str, str], *, force: bool) -> Path:
    out = output_dir / "live_supertable_usage_compare.parquet"
    if out.exists() and not force:
        return out
    cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "nfl_team",
        "opponent_nfl_team",
        "nfl_franchise_number",
        "opponent_nfl_franchise_number",
        "year",
        "week",
        "season_type",
        "data_source",
        *OVERLAP_COLUMNS.values(),
        *[col for col in NEW_SUPERTABLE_COLUMNS if col in schema],
    ]
    cols = [col for col in dict.fromkeys(cols) if col in schema]
    select_sql = ", ".join(q_ident(col) for col in cols)
    parts: list[pd.DataFrame] = []
    for start in range(1920, 2026, 10):
        end = min(start + 9, 2025)
        sql = f"""
            SELECT {select_sql}
            FROM {q_table(TARGET_TABLE)}
            WHERE year BETWEEN {start} AND {end}
              AND player_week IS NOT NULL
        """
        df = reader.query_df(sql, database="___ops")
        parts.append(df)
        print(f"[live] {start}-{end}: {len(df):,} rows", flush=True)
    live = pd.concat(parts, ignore_index=True, sort=False) if parts else pd.DataFrame(columns=cols)
    live.to_parquet(out, index=False)
    return out


def fetch_player_bio(reader: FlyReader, output_dir: Path, *, force: bool) -> Path:
    out = output_dir / "live_player_bio_usage_compare.parquet"
    if out.exists() and not force:
        return out
    sql = """
        SELECT
            NFL_player_id,
            player,
            pfr_id,
            nfl_position,
            headshot_url,
            first_year,
            last_year,
            forty,
            bench,
            vertical,
            broad_jump,
            cone,
            shuttle
        FROM nfl_historical.player_bio
    """
    df = reader.query_df(sql, database="___ops")
    df.to_parquet(out, index=False)
    return out


def fetch_franchise_history(reader: FlyReader, output_dir: Path, *, force: bool) -> Path:
    out = output_dir / "live_franchise_history.parquet"
    if out.exists() and not force:
        return out
    sql = """
        SELECT
            nfl_franchise_number,
            abbrev,
            start_year,
            COALESCE(end_year, 9999) AS end_year,
            is_canonical
        FROM nfl_historical.nfl_franchise_history
    """
    df = reader.query_df(sql, database="___ops")
    df.to_parquet(out, index=False)
    return out


def build_pfr_usage_advanced_fact(retab_dir: Path, output_dir: Path) -> Path:
    out = output_dir / "pfr_usage_advanced_fact.parquet"
    game_dim = sql_path(retab_dir / "pfr_game_team_dim.parquet")
    hints = sql_path(retab_dir / "pfr_player_game_position_hint.parquet")
    tables = PFR_BOXSCORES_ROOT / "tables"
    paths = {
        "passing": sql_path(tables / "passing_advanced" / "_combined.parquet"),
        "receiving": sql_path(tables / "receiving_advanced" / "_combined.parquet"),
        "rushing": sql_path(tables / "rushing_advanced" / "_combined.parquet"),
        "defense": sql_path(tables / "defense_advanced" / "_combined.parquet"),
    }
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET preserve_insertion_order=false")
    con.execute(
        f"""
        COPY (
            WITH usage_raw AS (
                SELECT
                    h.boxscore_id,
                    h.game_date,
                    h.season,
                    h.team,
                    h.pfr_player_game_key,
                    h.pfr_id,
                    h.player,
                    h.pos_hint,
                    TRY_CAST(h.started AS INTEGER) AS is_starter,
                    TRY_CAST(h.offense_snaps AS DOUBLE) AS offense_snaps,
                    TRY_CAST(regexp_replace(CAST(h.offense_pct AS VARCHAR), '[^0-9.]+', '', 'g') AS DOUBLE) AS offense_snap_pct,
                    TRY_CAST(h.defense_snaps AS DOUBLE) AS defense_snaps,
                    TRY_CAST(regexp_replace(CAST(h.defense_pct AS VARCHAR), '[^0-9.]+', '', 'g') AS DOUBLE) AS defense_snap_pct,
                    TRY_CAST(h.st_snaps AS DOUBLE) AS special_teams_snaps,
                    TRY_CAST(regexp_replace(CAST(h.st_pct AS VARCHAR), '[^0-9.]+', '', 'g') AS DOUBLE) AS special_teams_snap_pct
                FROM read_parquet('{hints}') h
                WHERE h.pfr_id IS NOT NULL
            ),
            usage AS (
                SELECT
                    u.boxscore_id,
                    any_value(u.game_date) AS game_date,
                    any_value(u.season) AS season,
                    u.team,
                    u.pfr_id,
                    any_value(u.player) AS player,
                    max(u.is_starter) AS is_starter,
                    any_value(u.pos_hint) FILTER (WHERE u.is_starter = 1 AND u.pos_hint IS NOT NULL) AS starter_position,
                    max(u.offense_snaps) AS offense_snaps,
                    max(u.offense_snap_pct) AS offense_snap_pct,
                    max(u.defense_snaps) AS defense_snaps,
                    max(u.defense_snap_pct) AS defense_snap_pct,
                    max(u.special_teams_snaps) AS special_teams_snaps,
                    max(u.special_teams_snap_pct) AS special_teams_snap_pct
                FROM usage_raw u
                GROUP BY u.boxscore_id, u.team, u.pfr_id
            ),
            receiving AS (
                SELECT
                    boxscore_id,
                    any_value(CAST(game_date AS DATE)) AS game_date,
                    TRY_CAST(any_value(season) AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    NULLIF(CAST(player_link_ids AS VARCHAR), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(rec_first_down AS DOUBLE)) AS pfr_receiving_first_downs,
                    SUM(TRY_CAST(rec_air_yds AS DOUBLE)) AS pfr_receiving_air_yards,
                    SUM(TRY_CAST(rec_yac AS DOUBLE)) AS pfr_receiving_yards_after_catch,
                    MAX(TRY_CAST(rec_adot AS DOUBLE)) AS receiving_adot,
                    SUM(TRY_CAST(rec_broken_tackles AS DOUBLE)) AS receiving_broken_tackles,
                    SUM(TRY_CAST(rec_drops AS DOUBLE)) AS receiving_drops,
                    SUM(TRY_CAST(rec_target_int AS DOUBLE)) AS receiving_target_interceptions,
                    MAX(TRY_CAST(rec_pass_rating AS DOUBLE)) AS receiving_pass_rating
                FROM read_parquet('{paths["receiving"]}')
                WHERE NULLIF(CAST(player_link_ids AS VARCHAR), '') IS NOT NULL
                GROUP BY boxscore_id, team, pfr_id
            ),
            rushing AS (
                SELECT
                    boxscore_id,
                    any_value(CAST(game_date AS DATE)) AS game_date,
                    TRY_CAST(any_value(season) AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    NULLIF(CAST(player_link_ids AS VARCHAR), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(rush_first_down AS DOUBLE)) AS pfr_rushing_first_downs,
                    SUM(TRY_CAST(rush_yds_before_contact AS DOUBLE)) AS rushing_yards_before_contact,
                    SUM(TRY_CAST(rush_yac AS DOUBLE)) AS rushing_yards_after_contact,
                    SUM(TRY_CAST(rush_broken_tackles AS DOUBLE)) AS rushing_broken_tackles
                FROM read_parquet('{paths["rushing"]}')
                WHERE NULLIF(CAST(player_link_ids AS VARCHAR), '') IS NOT NULL
                GROUP BY boxscore_id, team, pfr_id
            ),
            passing AS (
                SELECT
                    boxscore_id,
                    any_value(CAST(game_date AS DATE)) AS game_date,
                    TRY_CAST(any_value(season) AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    NULLIF(CAST(player_link_ids AS VARCHAR), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(pass_first_down AS DOUBLE)) AS pfr_passing_first_downs,
                    SUM(TRY_CAST(pass_air_yds AS DOUBLE)) AS pfr_passing_air_yards,
                    SUM(TRY_CAST(pass_yac AS DOUBLE)) AS pfr_passing_yards_after_catch,
                    SUM(TRY_CAST(pass_drops AS DOUBLE)) AS passing_drops,
                    SUM(TRY_CAST(pass_poor_throws AS DOUBLE)) AS passing_poor_throws,
                    SUM(TRY_CAST(pass_blitzed AS DOUBLE)) AS passing_blitzed,
                    SUM(TRY_CAST(pass_hurried AS DOUBLE)) AS passing_hurried,
                    SUM(TRY_CAST(pass_hits AS DOUBLE)) AS passing_hits,
                    SUM(TRY_CAST(pass_pressured AS DOUBLE)) AS passing_pressured,
                    SUM(TRY_CAST(rush_scrambles AS DOUBLE)) AS rushing_scrambles
                FROM read_parquet('{paths["passing"]}')
                WHERE NULLIF(CAST(player_link_ids AS VARCHAR), '') IS NOT NULL
                GROUP BY boxscore_id, team, pfr_id
            ),
            defense AS (
                SELECT
                    boxscore_id,
                    any_value(CAST(game_date AS DATE)) AS game_date,
                    TRY_CAST(any_value(season) AS INTEGER) AS season,
                    CAST(team AS VARCHAR) AS team,
                    NULLIF(CAST(player_link_ids AS VARCHAR), '') AS pfr_id,
                    any_value(CAST(player AS VARCHAR)) AS player,
                    SUM(TRY_CAST(def_targets AS DOUBLE)) AS def_targets_allowed,
                    SUM(TRY_CAST(def_cmp AS DOUBLE)) AS def_completions_allowed,
                    SUM(TRY_CAST(def_cmp_yds AS DOUBLE)) AS def_completion_yards_allowed,
                    SUM(TRY_CAST(def_cmp_td AS DOUBLE)) AS def_completion_tds_allowed,
                    MAX(TRY_CAST(def_pass_rating AS DOUBLE)) AS def_passer_rating_allowed,
                    SUM(TRY_CAST(def_air_yds AS DOUBLE)) AS def_air_yards_allowed,
                    SUM(TRY_CAST(def_yac AS DOUBLE)) AS def_yards_after_catch_allowed,
                    SUM(TRY_CAST(blitzes AS DOUBLE)) AS def_blitzes,
                    SUM(TRY_CAST(qb_hurry AS DOUBLE)) AS def_hurries,
                    SUM(TRY_CAST(qb_knockdown AS DOUBLE)) AS def_knockdowns,
                    SUM(TRY_CAST(pressures AS DOUBLE)) AS def_pressures,
                    SUM(TRY_CAST(tackles_missed AS DOUBLE)) AS def_tackles_missed
                FROM read_parquet('{paths["defense"]}')
                WHERE NULLIF(CAST(player_link_ids AS VARCHAR), '') IS NOT NULL
                GROUP BY boxscore_id, team, pfr_id
            ),
            keys AS (
                SELECT boxscore_id, team, pfr_id FROM usage
                UNION SELECT boxscore_id, team, pfr_id FROM receiving
                UNION SELECT boxscore_id, team, pfr_id FROM rushing
                UNION SELECT boxscore_id, team, pfr_id FROM passing
                UNION SELECT boxscore_id, team, pfr_id FROM defense
            )
            SELECT
                k.boxscore_id,
                COALESCE(u.game_date, rec.game_date, ru.game_date, pa.game_date, de.game_date) AS game_date,
                COALESCE(u.season, rec.season, ru.season, pa.season, de.season) AS season,
                k.team,
                g.opponent,
                g.pfr_week_num,
                k.pfr_id,
                COALESCE(u.player, rec.player, ru.player, pa.player, de.player) AS player,
                u.is_starter,
                u.starter_position,
                u.offense_snaps,
                u.offense_snap_pct,
                u.defense_snaps,
                u.defense_snap_pct,
                u.special_teams_snaps,
                u.special_teams_snap_pct,
                rec.pfr_receiving_first_downs,
                rec.pfr_receiving_air_yards,
                rec.pfr_receiving_yards_after_catch,
                rec.receiving_adot,
                rec.receiving_broken_tackles,
                rec.receiving_drops,
                rec.receiving_target_interceptions,
                rec.receiving_pass_rating,
                ru.pfr_rushing_first_downs,
                ru.rushing_yards_before_contact,
                ru.rushing_yards_after_contact,
                ru.rushing_broken_tackles,
                pa.pfr_passing_first_downs,
                pa.pfr_passing_air_yards,
                pa.pfr_passing_yards_after_catch,
                pa.passing_drops,
                pa.passing_poor_throws,
                pa.passing_blitzed,
                pa.passing_hurried,
                pa.passing_hits,
                pa.passing_pressured,
                pa.rushing_scrambles,
                de.def_targets_allowed,
                de.def_completions_allowed,
                de.def_completion_yards_allowed,
                de.def_completion_tds_allowed,
                de.def_passer_rating_allowed,
                de.def_air_yards_allowed,
                de.def_yards_after_catch_allowed,
                de.def_blitzes,
                de.def_hurries,
                de.def_knockdowns,
                de.def_pressures,
                de.def_tackles_missed
            FROM keys k
            LEFT JOIN usage u USING (boxscore_id, team, pfr_id)
            LEFT JOIN receiving rec USING (boxscore_id, team, pfr_id)
            LEFT JOIN rushing ru USING (boxscore_id, team, pfr_id)
            LEFT JOIN passing pa USING (boxscore_id, team, pfr_id)
            LEFT JOIN defense de USING (boxscore_id, team, pfr_id)
            LEFT JOIN read_parquet('{game_dim}') g
              ON k.boxscore_id = g.boxscore_id
             AND k.team = g.team
            ORDER BY game_date, boxscore_id, team, player
        ) TO '{sql_path(out)}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.close()
    return out


def pfr_id_like_series(series: pd.Series) -> pd.Series:
    return series.astype("string").str.match(r"^[A-Za-z].*[0-9][0-9]$", na=False)


def build_identity_bridge(bio: pd.DataFrame, live: pd.DataFrame | None = None) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in bio.to_dict("records"):
        nfl_id = norm_text(row.get("NFL_player_id"))
        if not nfl_id:
            continue
        pfr_id = norm_text(row.get("pfr_id"))
        first_year = pd.to_numeric(pd.Series([row.get("first_year")]), errors="coerce").iloc[0]
        last_year = pd.to_numeric(pd.Series([row.get("last_year")]), errors="coerce").iloc[0]
        if pfr_id:
            rows.append(
                {
                    "pfr_id_norm": pfr_id.lower(),
                    "bridge_NFL_player_id": nfl_id,
                    "bridge_player": row.get("player"),
                    "bridge_first_year": first_year,
                    "bridge_last_year": last_year,
                    "bridge_source": "bio_pfr_id",
                }
            )
        if re.match(r"^[A-Za-z].*[0-9][0-9]$", nfl_id):
            rows.append(
                {
                    "pfr_id_norm": nfl_id.lower(),
                    "bridge_NFL_player_id": nfl_id,
                    "bridge_player": row.get("player"),
                    "bridge_first_year": first_year,
                    "bridge_last_year": last_year,
                    "bridge_source": "bio_nfl_id_is_pfr",
                }
            )
    if live is not None and not live.empty and "NFL_player_id" in live.columns:
        live_ids = live[live["NFL_player_id"].notna()].copy()
        live_ids["NFL_player_id_text"] = live_ids["NFL_player_id"].astype("string").str.strip()
        live_ids = live_ids[pfr_id_like_series(live_ids["NFL_player_id_text"])].copy()
        if not live_ids.empty:
            grouped = (
                live_ids.groupby("NFL_player_id_text", dropna=True)
                .agg(
                    bridge_player=("player", "first"),
                    bridge_first_year=("year", "min"),
                    bridge_last_year=("year", "max"),
                )
                .reset_index()
            )
            for row in grouped.to_dict("records"):
                nfl_id = norm_text(row.get("NFL_player_id_text"))
                if not nfl_id:
                    continue
                rows.append(
                    {
                        "pfr_id_norm": nfl_id.lower(),
                        "bridge_NFL_player_id": nfl_id,
                        "bridge_player": row.get("bridge_player"),
                        "bridge_first_year": row.get("bridge_first_year"),
                        "bridge_last_year": row.get("bridge_last_year"),
                        "bridge_source": "live_direct_nfl_id",
                    }
                )
    bridge = pd.DataFrame(rows)
    if bridge.empty:
        return pd.DataFrame(
            columns=[
                "pfr_id_norm",
                "bridge_NFL_player_id",
                "bridge_player",
                "bridge_first_year",
                "bridge_last_year",
                "bridge_source",
            ]
        )
    source_priority = {"bio_pfr_id": 1, "bio_nfl_id_is_pfr": 2, "live_direct_nfl_id": 3}
    bridge["_source_priority"] = bridge["bridge_source"].map(source_priority).fillna(9)
    bridge = bridge.sort_values(["pfr_id_norm", "_source_priority", "bridge_NFL_player_id"])
    return (
        bridge.drop_duplicates(["pfr_id_norm", "bridge_NFL_player_id", "bridge_source"])
        .drop(columns=["_source_priority"])
        .reset_index(drop=True)
    )


def map_franchise_numbers(pfr: pd.DataFrame, franchise_history: pd.DataFrame) -> pd.DataFrame:
    hist = franchise_history.copy()
    hist["abbrev_norm"] = hist["abbrev"].astype(str).str.upper().map(lambda x: TEAM_CODE_ALIASES.get(x, x))
    hist["is_canonical_sort"] = hist["is_canonical"].fillna(False).astype(bool).astype(int)

    def mapped(col: str, out_col: str) -> pd.Series:
        left = pfr[["season", col]].copy()
        left["_row"] = left.index
        left["abbrev_norm"] = left[col].astype(str).str.upper().map(lambda x: TEAM_CODE_ALIASES.get(x, x))
        merged = left.merge(hist, on="abbrev_norm", how="left")
        merged = merged[
            (merged["nfl_franchise_number"].isna())
            | (
                (
                    pd.to_numeric(merged["season"], errors="coerce")
                    >= pd.to_numeric(merged["start_year"], errors="coerce")
                )
                & (
                    pd.to_numeric(merged["season"], errors="coerce")
                    <= pd.to_numeric(merged["end_year"], errors="coerce")
                )
            )
        ].copy()
        merged = merged.sort_values(
            ["_row", "is_canonical_sort", "nfl_franchise_number"], ascending=[True, False, True]
        )
        best = merged.drop_duplicates("_row").set_index("_row")["nfl_franchise_number"]
        return pfr.index.to_series().map(best).rename(out_col)

    pfr = pfr.copy()
    pfr["pfr_team_franchise_number"] = mapped("team", "pfr_team_franchise_number")
    pfr["pfr_opponent_franchise_number"] = mapped("opponent", "pfr_opponent_franchise_number")
    return pfr


def build_update_stage(
    pfr_fact_path: Path,
    bio_path: Path,
    live_path: Path,
    franchise_history_path: Path,
    output_dir: Path,
) -> dict[str, Any]:
    pfr = pd.read_parquet(pfr_fact_path)
    bio = pd.read_parquet(bio_path)
    live = pd.read_parquet(live_path)
    franchise_history = pd.read_parquet(franchise_history_path)

    pfr["pfr_id_norm"] = pfr["pfr_id"].astype("string").str.strip().str.lower()
    bridge = build_identity_bridge(bio, live)
    bridge.to_parquet(output_dir / "pfr_usage_identity_bridge.parquet", index=False)
    pfr = pfr.merge(bridge, on="pfr_id_norm", how="left")
    season_num = pd.to_numeric(pfr["season"], errors="coerce")
    first = pd.to_numeric(pfr["bridge_first_year"], errors="coerce")
    last = pd.to_numeric(pfr["bridge_last_year"], errors="coerce")
    in_range = pfr["bridge_NFL_player_id"].notna() & (
        first.isna() | last.isna() | ((season_num >= first - 1) & (season_num <= last + 1))
    )
    pfr["_bridge_in_range"] = in_range.astype(int)
    pfr["_bridge_priority"] = pfr["bridge_source"].map({"bio_pfr_id": 1, "bio_nfl_id_is_pfr": 2}).fillna(9)
    pfr = (
        pfr.sort_values(
            ["boxscore_id", "team", "pfr_id", "_bridge_in_range", "_bridge_priority"],
            ascending=[True, True, True, False, True],
        )
        .drop_duplicates(["boxscore_id", "team", "pfr_id"])
        .copy()
    )
    pfr = map_franchise_numbers(pfr, franchise_history)
    pfr["year"] = pd.to_numeric(pfr["season"], errors="coerce").astype("Int64")
    pfr["week"] = pd.to_numeric(pfr["pfr_week_num"], errors="coerce").astype("Int64")
    pfr["NFL_player_id"] = pfr["bridge_NFL_player_id"]
    pfr["player_week"] = (
        pfr["NFL_player_id"].astype("string") + "_" + pfr["year"].astype("string") + "_" + pfr["week"].astype("string")
    )
    pfr.loc[pfr["NFL_player_id"].isna() | pfr["year"].isna() | pfr["week"].isna(), "player_week"] = pd.NA
    pfr["nfl_team"] = (
        pfr["team"]
        .astype("string")
        .str.upper()
        .map(lambda x: TEAM_CODE_ALIASES.get(str(x), str(x)) if pd.notna(x) else x)
    )
    pfr["opponent_nfl_team"] = (
        pfr["opponent"]
        .astype("string")
        .str.upper()
        .map(lambda x: TEAM_CODE_ALIASES.get(str(x), str(x)) if pd.notna(x) else x)
    )

    live["player_week"] = live["player_week"].astype("string")
    stage = pfr.merge(
        live.add_prefix("live_"),
        left_on="player_week",
        right_on="live_player_week",
        how="left",
        indicator=True,
    )
    stage["matched_existing_row"] = stage["_merge"].eq("both")
    stage["strict_context_match"] = (
        stage["matched_existing_row"]
        & (pd.to_numeric(stage["live_year"], errors="coerce") == pd.to_numeric(stage["year"], errors="coerce"))
        & (pd.to_numeric(stage["live_week"], errors="coerce") == pd.to_numeric(stage["week"], errors="coerce"))
        & (
            stage["pfr_team_franchise_number"].isna()
            | stage["live_nfl_franchise_number"].isna()
            | (
                pd.to_numeric(stage["pfr_team_franchise_number"], errors="coerce")
                == pd.to_numeric(stage["live_nfl_franchise_number"], errors="coerce")
            )
        )
        & (
            stage["pfr_opponent_franchise_number"].isna()
            | stage["live_opponent_nfl_franchise_number"].isna()
            | (
                pd.to_numeric(stage["pfr_opponent_franchise_number"], errors="coerce")
                == pd.to_numeric(stage["live_opponent_nfl_franchise_number"], errors="coerce")
            )
        )
    )

    # Keep only stage rows whose player_week maps cleanly to one target row.
    dup_mask = (
        stage["strict_context_match"] & stage["player_week"].notna() & stage.duplicated("player_week", keep=False)
    )
    duplicate_stage = stage[dup_mask].copy()
    duplicate_stage.to_csv(output_dir / "duplicate_player_week_stage_rows.csv", index=False)

    update_stage = stage[stage["strict_context_match"] & ~dup_mask].copy()
    # Do not send rows that have no new field value at all.
    has_new_value = update_stage[NEW_SUPERTABLE_COLUMNS].notna().any(axis=1)
    update_stage = update_stage[has_new_value].copy()
    if "starter_position" in update_stage.columns:
        update_stage["starter_position"] = (
            update_stage["starter_position"].astype("string").str.replace(";", "/", regex=False)
        )

    identity_cols = [
        "player_week",
        "NFL_player_id",
        "player",
        "year",
        "week",
        "nfl_team",
        "opponent_nfl_team",
        "pfr_team_franchise_number",
        "pfr_opponent_franchise_number",
        "boxscore_id",
        "game_date",
        "pfr_id",
        "team",
        "opponent",
    ]
    update_stage[identity_cols + NEW_SUPERTABLE_COLUMNS + list(OVERLAP_COLUMNS)].to_parquet(
        output_dir / "pfr_usage_advanced_update_stage.parquet",
        index=False,
    )

    unmatched = stage[~stage["strict_context_match"]].copy()
    unmatched_cols = [
        "boxscore_id",
        "game_date",
        "season",
        "pfr_week_num",
        "team",
        "opponent",
        "pfr_id",
        "player",
        "bridge_NFL_player_id",
        "player_week",
        "matched_existing_row",
        "live_player",
        "live_nfl_team",
        "live_opponent_nfl_team",
    ]
    unmatched[unmatched_cols].to_csv(output_dir / "pfr_usage_advanced_unmatched_or_context_blocked.csv", index=False)

    coverage_rows = []
    for family, cols in {
        "usage_starter_snap": [
            "is_starter",
            "offense_snaps",
            "offense_snap_pct",
            "defense_snaps",
            "defense_snap_pct",
            "special_teams_snaps",
            "special_teams_snap_pct",
        ],
        "advanced_receiving": [
            "receiving_adot",
            "receiving_broken_tackles",
            "receiving_drops",
            "receiving_target_interceptions",
            "receiving_pass_rating",
        ],
        "advanced_rushing": [
            "rushing_yards_before_contact",
            "rushing_yards_after_contact",
            "rushing_broken_tackles",
        ],
        "advanced_passing": [
            "passing_drops",
            "passing_poor_throws",
            "passing_blitzed",
            "passing_hurried",
            "passing_hits",
            "passing_pressured",
            "rushing_scrambles",
        ],
        "advanced_defense": [
            "def_targets_allowed",
            "def_completions_allowed",
            "def_completion_yards_allowed",
            "def_completion_tds_allowed",
            "def_passer_rating_allowed",
            "def_air_yards_allowed",
            "def_yards_after_catch_allowed",
            "def_blitzes",
            "def_hurries",
            "def_knockdowns",
            "def_pressures",
            "def_tackles_missed",
        ],
    }.items():
        source_mask = stage[cols].notna().any(axis=1)
        coverage_rows.append(
            {
                "family": family,
                "pfr_rows_with_any_value": int(source_mask.sum()),
                "matched_existing_rows": int((source_mask & stage["strict_context_match"]).sum()),
                "update_rows_unique_player_week": int((source_mask & stage["strict_context_match"] & ~dup_mask).sum()),
                "unmatched_or_context_blocked": int((source_mask & ~stage["strict_context_match"]).sum()),
                "duplicate_player_week_blocked": int((source_mask & dup_mask).sum()),
            }
        )
    coverage = pd.DataFrame(coverage_rows)
    coverage.to_csv(output_dir / "pfr_usage_advanced_coverage_summary.csv", index=False)

    divergence_rows = []
    examples: list[pd.DataFrame] = []
    for pfr_col, live_col in OVERLAP_COLUMNS.items():
        if f"live_{live_col}" not in stage.columns:
            continue
        part = stage[stage["strict_context_match"] & stage[pfr_col].notna() & stage[f"live_{live_col}"].notna()].copy()
        p = pd.to_numeric(part[pfr_col], errors="coerce")
        s = pd.to_numeric(part[f"live_{live_col}"], errors="coerce")
        delta = p - s
        discrep = delta.abs() > EPSILON
        divergence_rows.append(
            {
                "pfr_field": pfr_col,
                "live_field": live_col,
                "compared_rows": int(len(part)),
                "pfr_total": float(p.sum(skipna=True)),
                "live_total": float(s.sum(skipna=True)),
                "delta_total": float(delta.sum(skipna=True)),
                "abs_delta_total": float(delta.abs().sum(skipna=True)),
                "discrepant_rows": int(discrep.sum()),
                "max_abs_delta": float(delta.abs().max(skipna=True) or 0),
            }
        )
        ex = part.loc[
            discrep,
            [
                "player_week",
                "player",
                "team",
                "opponent",
                "season",
                "pfr_week_num",
                "boxscore_id",
                pfr_col,
                f"live_{live_col}",
                "live_data_source",
            ],
        ].copy()
        if not ex.empty:
            ex["field"] = live_col
            ex["delta"] = pd.to_numeric(ex[pfr_col], errors="coerce") - pd.to_numeric(
                ex[f"live_{live_col}"], errors="coerce"
            )
            examples.append(ex.sort_values("delta", key=lambda x: x.abs(), ascending=False).head(50))
    divergence = pd.DataFrame(divergence_rows)
    divergence.to_csv(output_dir / "pfr_overlap_divergence_summary.csv", index=False)
    if examples:
        pd.concat(examples, ignore_index=True, sort=False).to_csv(
            output_dir / "pfr_overlap_divergence_examples.csv", index=False
        )
    else:
        pd.DataFrame().to_csv(output_dir / "pfr_overlap_divergence_examples.csv", index=False)

    return {
        "pfr_rows": int(len(pfr)),
        "stage_rows": int(len(stage)),
        "strict_matched_rows": int(stage["strict_context_match"].sum()),
        "update_stage_rows": int(len(update_stage)),
        "unmatched_or_context_blocked_rows": int((~stage["strict_context_match"]).sum()),
        "duplicate_player_week_blocked_rows": int(len(duplicate_stage)),
        "coverage": coverage.to_dict("records"),
        "divergence": divergence.to_dict("records"),
    }


def build_bio_combine_stage(bio_path: Path, output_dir: Path) -> dict[str, Any]:
    bio = pd.read_parquet(bio_path)
    combine = pd.read_parquet(PFR_CONTEXT_ROOT / "tables" / "combine" / "_combined.parquet")
    bio["pfr_id_norm"] = bio["pfr_id"].astype("string").str.strip().str.lower()
    combine["pfr_id_norm"] = combine["player_link_ids"].astype("string").str.strip().str.lower()
    combine = combine[combine["pfr_id_norm"].notna()].copy()
    combine_cols = ["pfr_id_norm", "player", *BIO_COMBINE_MAP.keys()]
    bio_cols = ["NFL_player_id", "player", "pfr_id_norm", *BIO_COMBINE_MAP.values()]
    joined = (
        combine[combine_cols]
        .rename(columns={col: f"pfr__{col}" for col in BIO_COMBINE_MAP})
        .merge(
            bio[bio_cols].rename(columns={col: f"bio__{col}" for col in BIO_COMBINE_MAP.values()}),
            on="pfr_id_norm",
            how="inner",
        )
    )
    stage_rows: list[dict[str, Any]] = []
    fill_counts: dict[str, int] = {}
    for _, row in joined.iterrows():
        out: dict[str, Any] = {
            "NFL_player_id": row["NFL_player_id"],
            "player": row.get("player_y") or row.get("player_x"),
        }
        any_fill = False
        for pfr_col, bio_col in BIO_COMBINE_MAP.items():
            pfr_val = row.get(f"pfr__{pfr_col}")
            bio_val = row.get(f"bio__{bio_col}")
            if norm_text(pfr_val) and not norm_text(bio_val):
                out[bio_col] = pfr_val
                fill_counts[bio_col] = fill_counts.get(bio_col, 0) + 1
                any_fill = True
            else:
                out[bio_col] = None
        if any_fill:
            stage_rows.append(out)
    stage = pd.DataFrame(stage_rows)
    if not stage.empty:
        stage = stage.drop_duplicates("NFL_player_id", keep="first")
    stage.to_parquet(output_dir / "pfr_player_bio_combine_fill_stage.parquet", index=False)
    summary = pd.DataFrame([{"field": k, "fill_rows": v} for k, v in sorted(fill_counts.items())])
    if summary.empty:
        summary = pd.DataFrame(columns=["field", "fill_rows"])
    summary.to_csv(
        output_dir / "pfr_player_bio_combine_fill_summary.csv",
        index=False,
    )
    return {"bio_combine_stage_rows": int(len(stage)), "bio_combine_fill_counts": fill_counts}


def create_stage_table(
    writer: FlyWriter, table: str, frame: pd.DataFrame, columns: list[str], schema_types: dict[str, str]
) -> None:
    defs = ", ".join(f"{q_ident(col)} {type_for_col(col, frame, schema_types)}" for col in columns)
    writer.execute(f"DROP TABLE IF EXISTS {q_table(table)}", database="___ops")
    writer.execute(f"CREATE TABLE {q_table(table)} ({defs})", database="___ops")


def insert_stage_rows(
    writer: FlyWriter,
    table: str,
    frame: pd.DataFrame,
    columns: list[str],
    schema_types: dict[str, str],
    *,
    batch_rows: int,
) -> None:
    if frame.empty:
        return
    type_by_col = {col: type_for_col(col, frame, schema_types) for col in columns}
    col_sql = ", ".join(q_ident(col) for col in columns)
    for start in range(0, len(frame), batch_rows):
        part = frame.iloc[start : start + batch_rows][columns].copy()
        values = rows_values_sql(part, columns, type_by_col)
        writer.execute(f"INSERT INTO {q_table(table)} ({col_sql}) VALUES {values}", database="___ops")
        print(f"[stage] {table}: inserted {min(start + batch_rows, len(frame)):,}/{len(frame):,}", flush=True)


def stage_frame(
    writer: FlyWriter,
    table: str,
    frame: pd.DataFrame,
    columns: list[str],
    schema_types: dict[str, str],
    *,
    batch_rows: int,
) -> None:
    create_stage_table(writer, table, frame, columns, schema_types)
    insert_stage_rows(writer, table, frame, columns, schema_types, batch_rows=batch_rows)


def apply_live_updates(
    output_dir: Path,
    schema: dict[str, str],
    bio_schema: dict[str, str],
    *,
    batch_rows: int,
) -> list[dict[str, Any]]:
    writer = FlyWriter()
    stamp = now_stamp().lower()
    executed: list[dict[str, Any]] = []

    def record(operation: str, detail: str, rows: int | None = None) -> None:
        executed.append(
            {
                "operation": operation,
                "detail": detail,
                "rows": rows,
                "completed_at_utc": datetime.now(UTC).isoformat(),
            }
        )
        pd.DataFrame(executed).to_csv(output_dir / "executed_operations.csv", index=False)

    # Add columns first. The affected-row backup then captures the pre-update
    # value state of these columns as NULL if they were just added.
    for col, sql_type in SCHEMA_ADDITIONS.items():
        writer.execute(
            f"ALTER TABLE {q_table(TARGET_TABLE)} ADD COLUMN IF NOT EXISTS {q_ident(col)} {sql_type}",
            database="___ops",
        )
        schema[col] = sql_type
    record("schema_add_columns", ",".join(SCHEMA_ADDITIONS), len(SCHEMA_ADDITIONS))

    stage_path = output_dir / "pfr_usage_advanced_update_stage.parquet"
    stage = pd.read_parquet(stage_path)
    stage_cols = [
        "player_week",
        "year",
        "week",
        "nfl_team",
        "opponent_nfl_team",
        *NEW_SUPERTABLE_COLUMNS,
    ]
    stage_cols = [col for col in stage_cols if col in stage.columns]
    stage = stage[stage_cols].copy()
    stage = stage[stage[NEW_SUPERTABLE_COLUMNS].notna().any(axis=1)].copy()
    stage_table = f"nfl_historical._pfr_usage_adv_stage_{stamp}"
    stage_frame(writer, stage_table, stage, stage_cols, schema, batch_rows=batch_rows)
    record("stage_usage_advanced", stage_table, len(stage))

    backup_table = f"nfl_historical.pfr_usage_adv_backup_{stamp}"
    backup_cols = ["player_week", *NEW_SUPERTABLE_COLUMNS]
    backup_cols = [col for col in backup_cols if col in schema]
    writer.execute(
        f"""
        CREATE TABLE {q_table(backup_table)} AS
        SELECT {", ".join("t." + q_ident(col) for col in backup_cols)}
        FROM {q_table(TARGET_TABLE)} t
        WHERE EXISTS (
            SELECT 1
            FROM {q_table(stage_table)} s
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
        )
        """,
        database="___ops",
    )
    record("backup_affected_supertable_rows", backup_table, len(stage))

    for idx, start in enumerate(range(0, len(NEW_SUPERTABLE_COLUMNS), 8), start=1):
        cols = NEW_SUPERTABLE_COLUMNS[start : start + 8]
        assignments = ", ".join(f"{q_ident(col)} = COALESCE(s.{q_ident(col)}, t.{q_ident(col)})" for col in cols)
        writer.execute(
            f"""
            UPDATE {q_table(TARGET_TABLE)} AS t
            SET {assignments}
            FROM {q_table(stage_table)} AS s
            WHERE t.{q_ident('player_week')} = s.{q_ident('player_week')}
              AND t.{q_ident('year')} = s.{q_ident('year')}
              AND t.{q_ident('week')} = s.{q_ident('week')}
            """,
            database="___ops",
        )
        record(f"update_supertable_usage_advanced_part_{idx}", ",".join(cols), len(stage))

    bio_stage = pd.read_parquet(output_dir / "pfr_player_bio_combine_fill_stage.parquet")
    if not bio_stage.empty:
        bio_cols = ["NFL_player_id", *BIO_COMBINE_MAP.values()]
        bio_stage = bio_stage[bio_cols].copy()
        bio_stage_table = f"nfl_historical._pfr_bio_combine_stage_{stamp}"
        bio_stage_schema = bio_schema | {col: "VARCHAR" for col in bio_cols}
        stage_frame(writer, bio_stage_table, bio_stage, bio_cols, bio_stage_schema, batch_rows=batch_rows)
        record("stage_player_bio_combine", bio_stage_table, len(bio_stage))

        bio_backup_table = f"nfl_historical.player_bio_combine_backup_{stamp}"
        writer.execute(
            f"""
            CREATE TABLE {q_table(bio_backup_table)} AS
            SELECT {", ".join("b." + q_ident(col) for col in bio_cols)}
            FROM {q_table(BIO_TABLE)} b
            WHERE EXISTS (
                SELECT 1
                FROM {q_table(bio_stage_table)} s
                WHERE b.{q_ident('NFL_player_id')} = s.{q_ident('NFL_player_id')}
            )
            """,
            database="___ops",
        )
        record("backup_player_bio_combine_rows", bio_backup_table, len(bio_stage))

        for col in BIO_COMBINE_MAP.values():
            writer.execute(
                f"""
                UPDATE {q_table(BIO_TABLE)} AS b
                SET {q_ident(col)} = s.{q_ident(col)}
                FROM {q_table(bio_stage_table)} AS s
                WHERE b.{q_ident('NFL_player_id')} = s.{q_ident('NFL_player_id')}
                  AND b.{q_ident(col)} IS NULL
                  AND s.{q_ident(col)} IS NOT NULL
                """,
                database="___ops",
            )
            record("update_player_bio_combine_missing_only_part", col, len(bio_stage))
    return executed


def write_report(output_dir: Path, manifest: dict[str, Any]) -> None:
    coverage = pd.read_csv(output_dir / "pfr_usage_advanced_coverage_summary.csv")
    divergence = pd.read_csv(output_dir / "pfr_overlap_divergence_summary.csv")
    bio_fill = pd.read_csv(output_dir / "pfr_player_bio_combine_fill_summary.csv")

    lines = [
        "# PFR Usage/Advanced Backfill Audit",
        "",
        f"Generated: {datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        "",
        "## Summary",
        "",
        f"- PFR usage/advanced rows: {manifest['pfr_rows']:,}",
        f"- Strict matched rows: {manifest['strict_matched_rows']:,}",
        f"- Existing-row update stage rows: {manifest['update_stage_rows']:,}",
        f"- Unmatched/context-blocked rows: {manifest['unmatched_or_context_blocked_rows']:,}",
        f"- Duplicate player_week rows blocked: {manifest['duplicate_player_week_blocked_rows']:,}",
        f"- Bio combine fill stage rows: {manifest['bio_combine_stage_rows']:,}",
        "",
        "## Coverage",
        "",
        coverage.to_csv(index=False),
        "",
        "## Overlap Divergence",
        "",
        "Overlap fields are audited only; this package does not overwrite them.",
        "",
        divergence.to_csv(index=False),
        "",
        "## player_bio Combine Fills",
        "",
        bio_fill.to_csv(index=False),
        "",
        "## Source-Of-Truth Decision",
        "",
        "- PFR is treated as source of truth for snap counts, starter flags, and PFR-only advanced fields because these atoms are absent from the current live supertable.",
        "- Existing nflverse/PFR overlap fields remain untouched pending row-level review of the divergence examples.",
        "- PFR combine is treated as source of truth only for missing player_bio combine cells matched by exact PFR id.",
    ]
    (output_dir / "PFR_USAGE_ADVANCED_BACKFILL_AUDIT.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retab-dir", type=Path, default=None)
    parser.add_argument("--output-root", type=Path, default=CATALOG_ROOT)
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--force-fetch", action="store_true")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--batch-rows", type=int, default=500)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()
    retab_dir = args.retab_dir or latest_retab_dir()
    output_dir = args.output_dir or (args.output_root / f"pfr_usage_advanced_backfill_{now_stamp()}")
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[audit] retab={retab_dir}", flush=True)
    print(f"[audit] out={output_dir}", flush=True)

    reader = FlyReader()
    schema = fetch_schema(reader, TARGET_TABLE)
    bio_schema = fetch_schema(reader, BIO_TABLE)
    pfr_fact = build_pfr_usage_advanced_fact(retab_dir, output_dir)
    live_path = fetch_live_rows(reader, output_dir, schema, force=args.force_fetch)
    bio_path = fetch_player_bio(reader, output_dir, force=args.force_fetch)
    franchise_history = fetch_franchise_history(reader, output_dir, force=args.force_fetch)
    manifest = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "retab_dir": str(retab_dir),
        "output_dir": str(output_dir),
        "schema_additions": SCHEMA_ADDITIONS,
        "overlap_columns": OVERLAP_COLUMNS,
    }
    manifest.update(build_update_stage(pfr_fact, bio_path, live_path, franchise_history, output_dir))
    manifest.update(build_bio_combine_stage(bio_path, output_dir))

    if args.execute:
        if args.confirm != CONFIRM_TOKEN:
            raise SystemExit(f"--execute requires --confirm {CONFIRM_TOKEN}")
        manifest["executed_operations"] = apply_live_updates(output_dir, schema, bio_schema, batch_rows=args.batch_rows)
    else:
        manifest["executed_operations"] = []

    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    write_report(output_dir, manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True), flush=True)
    print(f"[report] {output_dir / 'PFR_USAGE_ADVANCED_BACKFILL_AUDIT.md'}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
