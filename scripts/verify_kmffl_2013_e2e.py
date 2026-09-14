"""End-to-end verification of the KMFFL 2013 fixes against live Fly data.

Pulls staging.staging_matchup (raw external upload, has win/loss but VARCHAR
everything) and league_settings from Fly. Simulates the post-fix pipeline:

  1. schema_conform now preserves win/loss/tie (config + orchestrator change)
  2. canonical_matchup now name-backfills opponent_franchise_id
  3. bracket_tracer _sql_winner falls back to win count when points are NULL

Then runs trace_championship_bracket_sql against that data and asserts:
  - Gavi (1) wins semifinal vs Rubinstein (3) by win=1
  - Yaacov (2) wins semifinal vs Jesse (4) by win=1
  - Gavi beats Yaacov in championship (week 14) by win=1
  - champion column written for Gavi at week 14
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

from multi_league.core.readers.fly_reader import FlyReader
from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
    trace_championship_bracket_sql,
)


def banner(msg: str) -> None:
    print("=" * 80)
    print(msg)
    print("=" * 80)


def main() -> int:
    banner("KMFFL 2013 — end-to-end pipeline verification on Fly data")

    reader = FlyReader()

    print("\n[1] Pull staging.staging_matchup (preserves win/loss from upload)...")
    staging = reader.query_df(
        "SELECT * FROM ___leagues.staging.staging_matchup " "WHERE db_name='kmffl' AND CAST(year AS INT)=2013",
        database="___leagues",
    )
    print(f"    staging rows: {len(staging)}")

    print("[2] Pull public.matchup canonical 2013 rows (has franchise_id + canonical types)...")
    canon = reader.query_df(
        "SELECT * FROM ___leagues.public.matchup WHERE db_name='kmffl' AND year=2013",
        database="___leagues",
    )
    print(f"    canonical rows: {len(canon)}")

    print("[3] Pull league_settings 2013...")
    settings_df = reader.query_df(
        "SELECT * FROM ___leagues.public.league_settings WHERE db_name='kmffl' AND year=2013",
        database="___leagues",
    )

    print("[4] Backfill win/loss from staging into the canonical frame...")
    # staging is all-VARCHAR, public is typed. Match by (year, week, manager) and
    # carry win/loss across. This simulates what schema_conform's new OPTIONAL_SLOTS
    # config will do at the next reimport.
    staging_keyed = staging.copy()
    staging_keyed["year"] = pd.to_numeric(staging_keyed["year"], errors="coerce").astype("Int64")
    staging_keyed["week"] = pd.to_numeric(staging_keyed["week"], errors="coerce").astype("Int64")
    staging_keyed["win_s"] = pd.to_numeric(staging_keyed["win"], errors="coerce").astype("Int64")
    staging_keyed["loss_s"] = pd.to_numeric(staging_keyed["loss"], errors="coerce").astype("Int64")
    staging_keyed = staging_keyed[["year", "week", "manager", "win_s", "loss_s"]]

    merged = canon.merge(staging_keyed, on=["year", "week", "manager"], how="left")
    # Where canonical win is NULL, take staging's value
    merged["win"] = merged["win"].fillna(merged["win_s"])
    merged["loss"] = merged["loss"].fillna(merged["loss_s"])
    merged = merged.drop(columns=["win_s", "loss_s"])
    pre_carry = canon["win"].notna().sum()
    post_carry = merged["win"].notna().sum()
    print(f"    win column non-NULL: {pre_carry} -> {post_carry}")

    print("[5] Backfill opponent_franchise_id by name (canonical_matchup fix)...")
    name_to_fid: dict[tuple, str] = {}
    for _, row in merged.iterrows():
        mgr = row.get("manager")
        fid = row.get("franchise_id")
        if pd.notna(mgr) and pd.notna(fid):
            name_to_fid[(row["year"], row["week"], str(mgr).strip())] = str(fid)

    backfilled = 0

    def _opp_lookup(row):
        nonlocal backfilled
        existing = row.get("opponent_franchise_id")
        if pd.notna(existing) and str(existing).strip() not in ("", "nan", "None"):
            return existing
        opp = row.get("opponent")
        if pd.isna(opp) or not str(opp).strip():
            return existing
        result = name_to_fid.get((row["year"], row["week"], str(opp).strip()))
        if result is not None:
            backfilled += 1
        return result if result is not None else existing

    merged["opponent_franchise_id"] = merged.apply(_opp_lookup, axis=1)
    print(f"    backfilled opponent_franchise_id rows: {backfilled}")

    print("[6] Load into local DuckDB and run bracket_tracer (with new win-fallback)...")
    merged["playoff_round"] = merged["playoff_round"].astype("string")
    if "final_playoff_seed" in merged.columns:
        merged["final_playoff_seed"] = pd.to_numeric(merged["final_playoff_seed"], errors="coerce").astype("Int64")
    # Coerce win/loss to plain int (DuckDB Int64-nullable + UPDATE = fine, but
    # the CAST(win AS INTEGER) in _WINNER_SQL needs the column to exist).
    merged["win"] = merged["win"].astype("Int64")
    merged["loss"] = merged["loss"].astype("Int64")

    conn = duckdb.connect(":memory:")
    conn.register("v", merged)
    conn.execute("CREATE TABLE matchup AS SELECT * FROM v")

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
    print(f"    settings: {bt_settings}")

    result = trace_championship_bracket_sql(
        conn,
        2013,
        bt_settings,
        table="matchup",
        id_col="franchise_id",
        write_back=True,
        db_filter="db_name = 'kmffl'",
    )

    # Gavi's franchise_id is 'GHZOUGTBZIYGQ6QOMLOD4FZLGA', Yaacov is '2OUWRYUIY4HHEW72H5YVBHPILA'
    GAVI = "GHZOUGTBZIYGQ6QOMLOD4FZLGA"
    YAACOV = "2OUWRYUIY4HHEW72H5YVBHPILA"

    print(f"\n    champion: {result.get('champion')!r}  (expected {GAVI!r} = Gavi)")
    print(f"    runner_up: {result.get('runner_up')!r}  (expected {YAACOV!r} = Yaacov)")
    print(f"    classifications count: {len(result.get('classifications') or {})}")
    for rd in result.get("rounds") or []:
        print(f"    round {rd['round']} (week {rd['week']}):")
        for m in rd["matchups"]:
            winner = m["winner"][:8] + ".." if m["winner"] else "NONE"
            print(
                f"      {m['high_seed_id'][:8]}.. (seed {m['high_seed']}) vs "
                f"{m['low_seed_id'][:8]}.. (seed {m['low_seed']})  winner={winner}"
            )

    print("\n[7] Verify written-back columns...")
    written = conn.execute(
        """
        SELECT manager, week, is_playoffs, is_championship, champion, playoff_round
        FROM matchup
        WHERE year=2013 AND week >= 13
        ORDER BY week, manager
        """
    ).df()
    print(written.to_string(index=False))

    n_playoffs = int(written["is_playoffs"].fillna(0).astype(int).sum())
    n_champ_flag = int(written["is_championship"].fillna(False).astype(bool).sum())
    gavi_champ = int(
        written.loc[(written["manager"] == "Gavi") & (written["week"] == 14), "champion"].iloc[0]
        if len(written.loc[(written["manager"] == "Gavi") & (written["week"] == 14)]) > 0
        else 0
    )

    print(f"\n  is_playoffs=true rows: {n_playoffs}        (expected 6)")
    print(f"  is_championship=true rows: {n_champ_flag}    (expected 2)")
    print(f"  champion=1 for Gavi week 14: {gavi_champ}    (expected 1)")
    print(f"  tracer.champion: {result.get('champion')}    (expected '{GAVI}')")

    print("\n[8] Run downstream aggregation — homepage_manager_rankings logic...")
    # Pull all years of canonical KMFFL into local DuckDB so the rankings query
    # has the full career picture, then patch in our 2013 fix.
    all_canon = reader.query_df(
        "SELECT * FROM ___leagues.public.matchup WHERE db_name='kmffl'",
        database="___leagues",
    )
    # Apply our 2013 fix: replace 2013 rows with the bracket-traced merged frame.
    all_canon = pd.concat(
        [all_canon[all_canon["year"] != 2013], merged.drop(columns=["db_name"], errors="ignore")],
        ignore_index=True,
    )
    # The merged frame doesn't have db_name; re-add it.
    if "db_name" not in all_canon.columns:
        all_canon["db_name"] = "kmffl"
    all_canon["db_name"] = all_canon["db_name"].fillna("kmffl")
    # is_consolation may be missing for 2013 rows; default to false
    if "is_consolation" not in all_canon.columns:
        all_canon["is_consolation"] = False
    all_canon["is_consolation"] = all_canon["is_consolation"].fillna(False)

    # Concat created mixed Int64 / int year+week dtypes — pre-cast to plain int.
    all_canon = all_canon.dropna(subset=["year", "week"])
    all_canon["year"] = all_canon["year"].astype(int)
    all_canon["week"] = all_canon["week"].astype(int)

    conn2 = duckdb.connect(":memory:")
    conn2.register("v", all_canon)
    # Mirror the production qualified path so the aggregation SQL works as-is.
    conn2.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn2.execute("CREATE TABLE public.matchup AS SELECT * FROM v")

    # Carry over the 2013 bracket_tracer write-back from `matchup` (local)
    traced_2013 = conn.execute(
        "SELECT manager, week, franchise_id, champion, is_playoffs, is_championship, playoff_round "
        "FROM matchup WHERE year=2013"
    ).df()
    for _, r in traced_2013.iterrows():
        champ_val = r["champion"]
        champ_int = int(champ_val) if pd.notna(champ_val) else 0
        conn2.execute(
            "UPDATE public.matchup SET champion=?, is_playoffs=?, is_championship=?, playoff_round=? "
            "WHERE db_name='kmffl' AND year=2013 AND week=? AND franchise_id=?",
            [
                champ_int,
                bool(r["is_playoffs"]),
                bool(r["is_championship"]),
                r["playoff_round"],
                int(r["week"]),
                r["franchise_id"],
            ],
        )

    # Run the same aggregation logic as compute_manager_rankings (simplified).
    rankings = conn2.execute("""
        WITH typed_matchup AS (
            SELECT * EXCLUDE (year, week),
                   TRY_CAST(year AS INT) AS year,
                   TRY_CAST(week AS INT) AS week
            FROM public.matchup
            WHERE franchise_id IS NOT NULL
              AND COALESCE(CAST(is_bye_week AS INT), 0) = 0
              AND db_name='kmffl'
        ),
        deduped AS (
            SELECT typed_matchup.*,
                   ROW_NUMBER() OVER (PARTITION BY franchise_id, year, week ORDER BY manager) AS rn
            FROM typed_matchup
        ),
        base_matchup AS (SELECT * FROM deduped WHERE rn = 1),
        year_stats AS (
            SELECT franchise_id, year, MAX(manager) AS manager,
                   MAX(COALESCE(CAST(champion AS INT), 0)) AS is_champion
            FROM base_matchup
            GROUP BY franchise_id, year
        )
        SELECT ARG_MAX(manager, year) AS manager,
               franchise_id,
               SUM(is_champion) AS championships,
               COUNT(*) AS seasons
        FROM year_stats
        GROUP BY franchise_id
        ORDER BY championships DESC, manager
    """).df()
    print(rankings.to_string(index=False))

    gavi_titles_row = rankings[rankings["franchise_id"] == GAVI]
    yaacov_titles_row = rankings[rankings["franchise_id"] == YAACOV]
    gavi_titles = int(gavi_titles_row["championships"].iloc[0]) if len(gavi_titles_row) else 0
    yaacov_titles = int(yaacov_titles_row["championships"].iloc[0]) if len(yaacov_titles_row) else 0
    print(f"\n  Gavi titles: {gavi_titles}    (expected >=1, was 0 pre-fix)")
    print(f"  Yaacov titles: {yaacov_titles}  (expected 0, was 1 pre-fix in UI)")

    success = (
        n_playoffs == 6
        and n_champ_flag == 2
        and gavi_champ == 1
        and result.get("champion") == GAVI
        and result.get("runner_up") == YAACOV
        and gavi_titles >= 1
    )
    print(f"\n=== {'OK' if success else 'FAIL'} ===")
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
