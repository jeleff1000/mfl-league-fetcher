"""Re-run the bracket-tracer + manager-rankings pipeline for KMFFL 2013 only.

Mirrors what a reimport would do for KMFFL, but faster: pulls the existing
KMFFL state from Fly into a local DuckDB, carries `win`/`loss` from
staging.staging_matchup into public.matchup (this is exactly the field
schema_conform now preserves on fresh imports), then runs the real
pipeline functions:

  - multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer
        .trace_championship_bracket_sql
  - multi_league.transformations.aggregation.homepage_summary
        .compute_manager_rankings

Pushes the changed rows back to Fly via FlyWriter.execute() so we exercise
exactly the same SQL UPDATE / INSERT statements the production runner
would emit. No hardcoded "Gavi=champion" data — everything is derived by
calling the same functions that the post-upload pipeline calls.

Blast radius:
  - public.matchup, KMFFL year=2013 weeks 13-14: win/loss/champion etc.
  - public.homepage_manager_rankings, KMFFL: DELETE + INSERT.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FFS = ROOT / "fantasy_football_data_scripts"
if str(FFS) not in sys.path:
    sys.path.insert(0, str(FFS))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import duckdb
import pandas as pd

from multi_league.core.fly_writer import FlyWriter
from multi_league.core.readers.fly_reader import FlyReader
from multi_league.transformations.aggregation.aggregation_utils import ColumnCache
from multi_league.transformations.aggregation.homepage_summary import (
    compute_manager_rankings,
    configure_table_catalog,
)
from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
    trace_championship_bracket_sql,
)

DB_NAME = "kmffl"


def banner(msg: str) -> None:
    print("=" * 80)
    print(msg)
    print("=" * 80)


def _sql_lit(value) -> str:
    """SQL literal — string-escape strings, NULL for NaN, otherwise repr."""
    if value is None:
        return "NULL"
    try:
        if pd.isna(value):
            return "NULL"
    except (TypeError, ValueError):
        pass
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, (float,)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def main() -> int:
    banner(f"Re-run pipeline transforms for {DB_NAME} (2013 only)")

    reader = FlyReader()
    writer = FlyWriter()

    print("\n[1] Pulling KMFFL state from Fly...")
    matchup_df = reader.query_df(
        f"SELECT * FROM ___leagues.public.matchup WHERE db_name='{DB_NAME}'",
        database="___leagues",
    )
    settings_df = reader.query_df(
        f"SELECT * FROM ___leagues.public.league_settings WHERE db_name='{DB_NAME}' AND year=2013",
        database="___leagues",
    )
    staging_df = reader.query_df(
        f"SELECT * FROM ___leagues.staging.staging_matchup WHERE db_name='{DB_NAME}' AND CAST(year AS INT)=2013",
        database="___leagues",
    )
    print(f"  matchup rows: {len(matchup_df)}")
    print(f"  settings rows: {len(settings_df)}")
    print(f"  staging 2013 rows: {len(staging_df)}")

    print("\n[2] Building local DuckDB mirror of KMFFL...")
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    # Pre-cast year/week to int32 (INTEGER) so the EXCLUDE+TRY_CAST(year AS INT)
    # chain in _typed_matchup_dedupe_ctes_sql is INT->INT and doesn't trip the
    # DuckDB binder ("INTEGER != BIGINT"). When pandas Int64 lands in DuckDB as
    # BIGINT and is then re-cast to INT under the same column name, the binder
    # in some CTE chains can't disambiguate the two `year` references.
    matchup_df = matchup_df.copy()
    matchup_df["year"] = pd.to_numeric(matchup_df["year"], errors="coerce").astype("int32")
    matchup_df["week"] = pd.to_numeric(matchup_df["week"], errors="coerce").astype("int32")
    conn.register("v_matchup", matchup_df)
    conn.execute("CREATE TABLE public.matchup AS SELECT * FROM v_matchup")
    conn.unregister("v_matchup")

    print("\n[3] Carry win/loss from staging into local public.matchup...")
    # This mirrors what schema_conform's new OPTIONAL_SLOTS preservation
    # produces — staging.staging_matchup already has the right win/loss
    # values; we are just flowing them through into the canonical table.
    staging_keyed = staging_df.copy()
    staging_keyed["year"] = pd.to_numeric(staging_keyed["year"], errors="coerce").astype("Int64")
    staging_keyed["week"] = pd.to_numeric(staging_keyed["week"], errors="coerce").astype("Int64")
    staging_keyed["win_s"] = pd.to_numeric(staging_keyed["win"], errors="coerce").astype("Int64")
    staging_keyed["loss_s"] = pd.to_numeric(staging_keyed["loss"], errors="coerce").astype("Int64")
    staging_keyed = staging_keyed[["year", "week", "manager", "win_s", "loss_s"]]
    conn.register("v_staging", staging_keyed)
    conn.execute(
        """
        UPDATE public.matchup AS m
        SET win = COALESCE(m.win, (
                SELECT win_s FROM v_staging s
                WHERE s.year = m.year AND s.week = m.week AND s.manager = m.manager
            )),
            loss = COALESCE(m.loss, (
                SELECT loss_s FROM v_staging s
                WHERE s.year = m.year AND s.week = m.week AND s.manager = m.manager
            ))
        WHERE m.year = 2013 AND (m.win IS NULL OR m.loss IS NULL)
        """
    )
    conn.unregister("v_staging")
    n_filled = conn.execute(
        "SELECT COUNT(*) FROM public.matchup WHERE year=2013 AND week>=13 AND win IS NOT NULL"
    ).fetchone()[0]
    print(f"  win populated on {n_filled} KMFFL 2013 wk13+ rows")

    print("\n[4] Running trace_championship_bracket_sql() — real pipeline function...")
    sr = settings_df.iloc[0].to_dict()
    bt_settings = {
        "playoff_teams": int(sr.get("playoff_teams") or 4),
        "bye_teams": int(sr.get("bye_teams") or 0),
        "playoff_start_week": int(sr.get("playoff_start_week") or 13),
        "end_week": int(sr.get("end_week") or 14),
        "num_teams": int(sr.get("num_teams") or 8),
        "uses_playoff_reseeding": bool(sr.get("uses_playoff_reseeding") or False),
        "playoff_round_type": int(sr.get("playoff_round_type") or 0),
        "has_multiweek_championship": False,
        "uses_median_score": bool(sr.get("uses_median") or False),
    }
    result = trace_championship_bracket_sql(
        conn,
        2013,
        bt_settings,
        table="public.matchup",
        id_col="franchise_id",
        write_back=True,
        db_filter="db_name = 'kmffl'",
    )
    print(f"  champion: {result.get('champion')!r}")
    print(f"  championship_teams: {result.get('championship_teams')}")

    print("\n[5] Computing diff vs Fly state for KMFFL 2013 wk13+ rows...")
    locally_traced = conn.execute(
        """
        SELECT manager, week, franchise_id, win, loss,
               CAST(COALESCE(champion, 0) AS INTEGER) AS champion,
               COALESCE(is_playoffs, FALSE) AS is_playoffs,
               COALESCE(is_consolation, FALSE) AS is_consolation,
               COALESCE(is_championship, FALSE) AS is_championship,
               playoff_round, final_playoff_seed
        FROM public.matchup
        WHERE db_name='kmffl' AND year=2013 AND week >= 13
        ORDER BY week, manager
        """
    ).df()
    print(locally_traced.to_string(index=False))

    print("\n[6] Pushing matchup row updates to Fly via FlyWriter.execute()...")
    update_count = 0
    for _, r in locally_traced.iterrows():
        update_sql = (
            "UPDATE ___leagues.public.matchup SET "
            f"win = {_sql_lit(int(r['win']) if pd.notna(r['win']) else None)}, "
            f"loss = {_sql_lit(int(r['loss']) if pd.notna(r['loss']) else None)}, "
            f"champion = {_sql_lit(int(r['champion']))}, "
            f"is_playoffs = {_sql_lit(bool(r['is_playoffs']))}, "
            f"is_consolation = {_sql_lit(bool(r['is_consolation']))}, "
            f"is_championship = {_sql_lit(bool(r['is_championship']))}, "
            f"playoff_round = {_sql_lit(r['playoff_round'])}, "
            f"final_playoff_seed = {_sql_lit(int(r['final_playoff_seed']) if pd.notna(r['final_playoff_seed']) else None)} "
            f"WHERE db_name='kmffl' AND year=2013 AND week={int(r['week'])} "
            f"AND franchise_id={_sql_lit(r['franchise_id'])}"
        )
        writer.execute(update_sql, database="___leagues")
        update_count += 1
    print(f"  pushed {update_count} matchup row updates")

    print("\n[7] Re-running compute_manager_rankings() — real pipeline function...")
    # configure_table_catalog needs to be called so central_table() knows we
    # operate against our local in-memory schema.
    configure_table_catalog(conn)
    # ColumnCache's `db_name == "memory"` branch does bare `DESCRIBE matchup`
    # which misses our `public.matchup`; pre-populate one keyed on the column
    # names we already have in pandas so has_champion/has_power_rating etc.
    # all evaluate True.
    cache = ColumnCache(conn, "memory", schema="public")
    cache._columns["matchup"] = {c.lower() for c in matchup_df.columns}
    cache._exists["matchup"] = True
    rankings = compute_manager_rankings(conn, db_name=DB_NAME, cache=cache)
    print(
        rankings[["manager", "championships", "playoff_appearances", "seasons", "career_rank"]].to_string(index=False)
    )

    print("\n[8] Pushing rankings to Fly: DELETE + INSERT for kmffl...")
    # Discover Fly's column ordering so our INSERT matches the table DDL.
    fly_cols = reader.query_df(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name='homepage_manager_rankings' "
        "ORDER BY ordinal_position",
        database="___leagues",
    )["column_name"].tolist()
    print(f"  homepage_manager_rankings columns: {fly_cols}")

    rankings = rankings.copy()
    rankings["db_name"] = DB_NAME
    # Order columns to match Fly's DDL, fill NULLs for any column we didn't compute
    for c in fly_cols:
        if c not in rankings.columns:
            rankings[c] = None
    rankings = rankings[fly_cols]

    writer.execute(
        f"DELETE FROM ___leagues.public.homepage_manager_rankings WHERE db_name='{DB_NAME}'",
        database="___leagues",
    )
    insert_count = 0
    for _, r in rankings.iterrows():
        vals = ", ".join(_sql_lit(v) for v in r.tolist())
        insert_sql = (
            f"INSERT INTO ___leagues.public.homepage_manager_rankings ({', '.join(fly_cols)}) " f"VALUES ({vals})"
        )
        writer.execute(insert_sql, database="___leagues")
        insert_count += 1
    print(f"  inserted {insert_count} rows")

    print("\n[9] Verifying Fly state...")
    fly_check = reader.query_df(
        f"SELECT manager, championships, career_rank "
        f"FROM ___leagues.public.homepage_manager_rankings WHERE db_name='{DB_NAME}' "
        "ORDER BY career_rank",
        database="___leagues",
    )
    print(fly_check.to_string(index=False))

    fly_matchup = reader.query_df(
        "SELECT manager, week, win, loss, champion, is_championship "
        "FROM ___leagues.public.matchup "
        "WHERE db_name='kmffl' AND year=2013 AND week >= 13 "
        "ORDER BY week, manager",
        database="___leagues",
    )
    print("\n  Fly matchup KMFFL 2013 wk13+:")
    print(fly_matchup.to_string(index=False))

    gavi_titles = (
        int(fly_check[fly_check["manager"] == "Gavi"]["championships"].iloc[0])
        if len(fly_check[fly_check["manager"] == "Gavi"])
        else 0
    )
    yaacov_titles = (
        int(fly_check[fly_check["manager"] == "Yaacov"]["championships"].iloc[0])
        if len(fly_check[fly_check["manager"] == "Yaacov"])
        else 0
    )
    print(f"\n  Gavi titles on Fly: {gavi_titles}    (expected 1)")
    print(f"  Yaacov titles on Fly: {yaacov_titles}  (expected 0)")
    success = gavi_titles == 1 and yaacov_titles == 0
    print(f"\n=== {'OK' if success else 'FAIL'} ===")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
