"""
build_season_career_ranks_v26.py  --  add PPG-twin + all-positions overall rank columns to the v26
season/career artifacts (player_nfl_season[_all], player_nfl_career[_all]).

The existing rank_season_* / rank_alltime_* columns rank by TOTAL points. This adds, for every table:

  1. PPG TWINS  ({rank}_ppg): for each of the 65 canonical RankSpecs (imported from the pipeline so the
     population + scoring stay identical), rank the SAME position population by points-PER-GAME. Small-sample
     gate = played >= SEASON_GAMES_PCT of that season's games (era-adaptive; also works for the in-progress
     current year); career gate = >= CAREER_MIN_GAMES.

  2. OVERALL "PLAYER RANK" (rank_{scope}_overall_{ruleset} + _ppg): rank all individual PLAYERS together by
     one comparable value -- offense/K use fpts_{ruleset}, IDP uses pts_idp_std -- for each scoring ruleset.
     Total (points>0) and PPG (gated) variants. Team DST is EXCLUDED (it is a team unit, not a player, and its
     "career" spans the whole franchise 1920-2025 so it isn't comparable to a player's career); DST keeps its
     own dedicated rank_{scope}_def.

So e.g. LaDainian Tomlinson 2006 gets rank_season_rb_ppr_ppg, rank_season_flex_ppr_ppg,
rank_season_overall_4pt_ppr (+_ppg) etc., and the all-time twins on the career table.

Ranks are ROW_NUMBER() (unique 1..N) matching the pipeline's build_rank_stage_sql. Season tables partition by
year; career tables are global. GATED WRITE per table: backup + verify (rows/ids unchanged, new cols present,
LT anchors), then os.replace.

    python -m scripts.sota_recon.build_season_career_ranks_v26            # dry-run (plan + counts)
    python -m scripts.sota_recon.build_season_career_ranks_v26 --apply
"""

from __future__ import annotations
import argparse
import os
import shutil
import sys
from datetime import datetime, timezone, UTC
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))
sys.path.insert(0, str(ROOT / "scripts"))
from multi_league.data_fetchers.aggregate_nfl_stats_fly import rank_specs_for_scope  # noqa: E402
from sota_recon.sources import latest_v26  # noqa: E402

ART = Path(latest_v26()).parent / "season_career_v26"
TABLES = [  # (file stem, scope)
    ("player_nfl_season", "season"),
    ("player_nfl_season_all", "season"),
    ("player_nfl_career", "alltime"),
    ("player_nfl_career_all", "alltime"),
]
OVERALL_RULESETS = [
    "4pt_0ppr",
    "4pt_half",
    "4pt_ppr",
    "5pt_0ppr",
    "5pt_half",
    "5pt_ppr",
    "6pt_0ppr",
    "6pt_half",
    "6pt_ppr",
    "4pt_tep",
    "5pt_tep",
    "6pt_tep",
    "4pt_ppfd",
    "5pt_ppfd",
    "6pt_ppfd",
]
IDP_POS = ("LB", "ILB", "OLB", "MLB", "DL", "DE", "DT", "NT", "ED", "EDGE", "DB", "CB", "S", "SS", "FS", "SAF")
DEF_POS = ("DEF", "DST")
SEASON_GAMES_PCT = 0.5  # season PPG rank: must have played >= 50% of that season's games
CAREER_MIN_GAMES = 16  # career PPG rank: >= one modern season


def _inlist(vals):
    return ", ".join("'" + v.replace("'", "''") + "'" for v in vals)


def _list(vals):
    return "[" + ", ".join("'" + v.replace("'", "''") + "'" for v in vals) + "]"


def _position_has_any(vals, alias: str = ""):
    prefix = f"{alias}." if alias else ""
    return f"list_has_any(string_split(COALESCE({prefix}position, ''), ','), {_list(vals)})"


def _overall_metric(ruleset: str) -> str:
    # ALL-POSITIONS raw-points leaderboard -- MUST mirror build_full_ops._overall_metric_expr: each
    # position by its OWN points so nobody is excluded (positional scarcity lives in LAMAR):
    #   DEF/DST -> pts_def_std, K (not skill) -> pts_k_std, IDP-only -> pts_idp_std, else -> fpts.
    off_pos = ("QB", "RB", "WR", "TE", "OL", "K", "P")
    return (
        f"CASE WHEN {_position_has_any(DEF_POS)} THEN COALESCE(pts_def_std,0) "
        f"WHEN {_position_has_any(('K',))} AND NOT {_position_has_any(('QB', 'RB', 'WR', 'TE'))} THEN COALESCE(pts_k_std,0) "
        f"WHEN {_position_has_any(IDP_POS)} AND NOT {_position_has_any(off_pos)} THEN COALESCE(pts_idp_std,0) "
        f"ELSE COALESCE(fpts_{ruleset},0) END"
    )


def build_for(con: duckdb.DuckDBPyConnection, path: str, scope: str) -> dict:
    season = scope == "season"
    prefix = "rank_season" if season else "rank_alltime"
    con.execute("DROP TABLE IF EXISTS t")
    con.execute(f"CREATE TABLE t AS SELECT * FROM '{path}'")
    existing = {r[0] for r in con.execute("DESCRIBE t").fetchall()}
    key = "t.NFL_player_id, t.year" if season else "t.NFL_player_id"
    keyjoin = "t.NFL_player_id=r.NFL_player_id AND t.year=r.year" if season else "t.NFL_player_id=r.NFL_player_id"
    part = "PARTITION BY t.year" if season else ""

    # per-year season length (max games any player played that year) for the % gate
    if season:
        con.execute("DROP TABLE IF EXISTS yg")
        con.execute("CREATE TABLE yg AS SELECT year, MAX(games_played) mg FROM t GROUP BY year")

    def gate_sql(alias="t"):
        if season:
            return (
                f"{alias}.games_played IS NOT NULL AND {alias}.games_played > 0 "
                f"AND {alias}.games_played >= {SEASON_GAMES_PCT} * yg.mg"
            )
        return f"{alias}.games_played IS NOT NULL AND {alias}.games_played >= {CAREER_MIN_GAMES}"

    gate_from = "t JOIN yg ON t.year=yg.year" if season else "t"

    jobs: list[tuple[str, str, str, str, bool]] = []  # (colname, popfilter, order_expr, metric_notnull, is_ppg)
    # 1. PPG twins for every canonical spec
    for spec in rank_specs_for_scope(scope):
        col = f"{spec.col}_ppg"
        pop = _position_has_any(spec.positions)
        order = f"({spec.points_col} * 1.0 / NULLIF(games_played,0))"
        jobs.append((col, pop, order, f"{spec.points_col} IS NOT NULL", True))
    # 2. overall ranks (total + ppg) per ruleset -- ALL players incl DEF/DST + K + IDP + offense,
    #    each by its own points (metric>0 is the only gate).
    for rs in OVERALL_RULESETS:
        m = _overall_metric(rs)
        jobs.append((f"{prefix}_overall_{rs}", "1=1", m, f"({m}) > 0", False))
        jobs.append(
            (f"{prefix}_overall_{rs}_ppg", "1=1", f"(({m}) * 1.0 / NULLIF(games_played,0))", f"({m}) > 0", True)
        )

    added = 0
    for col, pop, order, metric_nn, is_ppg in jobs:
        if col in existing:
            continue
        con.execute(f'ALTER TABLE t ADD COLUMN "{col}" INTEGER')
        where = [pop, metric_nn]
        frm = "t"
        if is_ppg:
            where.append(gate_sql("t"))
            frm = gate_from
        con.execute(f"""
            UPDATE t SET "{col}" = r.rn FROM (
              SELECT {key}, CAST(ROW_NUMBER() OVER ({part} ORDER BY {order} DESC, NFL_player_id ASC) AS INTEGER) rn
              FROM {frm} WHERE {" AND ".join(where)}
            ) r WHERE {keyjoin}
        """)
        added += 1
    return {"added": added, "total_jobs": len(jobs)}


def run(apply: bool) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='10GB'")
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for stem, scope in TABLES:
        path = (ART / f"{stem}.parquet").as_posix()
        res = build_for(con, path, scope)
        n0 = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
        nt = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        ncol = len(con.execute("DESCRIBE t").fetchall())
        print(f"[{stem}] +{res['added']} rank cols (of {res['total_jobs']} jobs)  rows {nt:,}  cols now {ncol}")
        if stem == "player_nfl_season":
            lt = con.execute("""SELECT rank_season_rb_ppr, rank_season_rb_ppr_ppg,
                rank_season_flex_ppr, rank_season_flex_ppr_ppg, rank_season_overall_4pt_ppr,
                rank_season_overall_4pt_ppr_ppg FROM t WHERE player='LaDainian Tomlinson' AND year=2006""").fetchone()
            print(f"   LT2006 [rb_ppr, rb_ppr_ppg, flex_ppr, flex_ppr_ppg, overall_ppr, overall_ppr_ppg]: {lt}")
        if not apply:
            continue
        assert nt == n0, f"row count changed for {stem}"
        assert con.execute(
            "SELECT COUNT(*)=COUNT(DISTINCT NFL_player_id) FROM t"
            if scope == "alltime"
            else "SELECT COUNT(*)=COUNT(DISTINCT (NFL_player_id||'|'||year)) FROM t"
        ).fetchone()[0], f"key not unique for {stem}"
        dest = ART / f"{stem}.parquet"
        backup = ART / f"{stem}.parquet.bak_ranks_{ts}"
        shutil.copy2(dest, backup)
        tmp = ART / f"{stem}.parquet.tmp"
        rb = con.execute("SELECT * FROM t").fetch_record_batch(50000)
        import pyarrow.parquet as pq

        w = pq.ParquetWriter(str(tmp), rb.schema)
        for b in rb:
            w.write_batch(b)
        w.close()
        os.replace(tmp, dest)
        print(f"   WROTE {dest.name}  backup {backup.name}")
    con.close()
    print("\nDONE." + ("" if apply else "  (dry-run -- no write)"))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    run(ap.parse_args().apply)
