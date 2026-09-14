"""
build_career_finishes_v26.py  --  add a compact fantasy-finish resume to the v26 career tables.

Precomputes the classic career resume ("3x top-5 RB, 8x RB1") from the season ranks, so a player card can
read it off player_nfl_career without scanning the season table. Compact + canonical (one scoring per axis,
not a per-ruleset explosion):

  positional finish (per-position canonical scoring: skill=ppr, QB=4pt, K, DEF, IDP=std):
    best_pos_finish            -- career-best single-season positional rank
    seasons_pos_top5 / _top12 / _top24
  overall "player rank" (4pt PPR, individual players; team DST excluded upstream):
    best_overall_finish_ppr
    seasons_overall_top12_ppr / _top24_ppr

Counts default to 0 (never finished top-N); best_* is NULL if the player never had a positional finish (OL/P).
career <- season, career_all <- season_all. GATED WRITE per table.

    python -m scripts.sota_recon.build_career_finishes_v26            # dry-run
    python -m scripts.sota_recon.build_career_finishes_v26 --apply
"""

from __future__ import annotations
import argparse
import os
import shutil
import sys
from datetime import datetime, timezone, UTC
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from sota_recon.sources import latest_v26  # noqa: E402

ART = Path(latest_v26()).parent / "season_career_v26"
PAIRS = [("player_nfl_career", "player_nfl_season"), ("player_nfl_career_all", "player_nfl_season_all")]

LB = ("LB", "ILB", "OLB", "MLB")
DL = ("DL", "DE", "DT", "NT", "ED", "EDGE")
DB = ("DB", "CB", "S", "SS", "FS", "SAF")


def _inlist(vals):
    return ", ".join("'" + v + "'" for v in vals)


def _has(vals):
    return "list_has_any(string_split(COALESCE(position, ''), ','), [" + ", ".join("'" + v + "'" for v in vals) + "])"


# per-season positional finish = rank in the player's position's canonical column
POS_FINISH = f"""
  NULLIF(LEAST(
    CASE WHEN {_has(("QB",))} THEN COALESCE(rank_season_qb_4pt, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(("RB",))} THEN COALESCE(rank_season_rb_ppr, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(("WR",))} THEN COALESCE(rank_season_wr_ppr, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(("TE",))} THEN COALESCE(rank_season_te_ppr, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(("K",))} THEN COALESCE(rank_season_k, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(("DEF", "DST"))} THEN COALESCE(rank_season_def, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(LB)} THEN COALESCE(rank_season_lb_std, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(DL)} THEN COALESCE(rank_season_dl_std, 2147483647) ELSE 2147483647 END,
    CASE WHEN {_has(DB)} THEN COALESCE(rank_season_db_std, 2147483647) ELSE 2147483647 END
  ), 2147483647)
"""

NEW_COLS = [
    "best_pos_finish",
    "seasons_pos_top5",
    "seasons_pos_top12",
    "seasons_pos_top24",
    "best_overall_finish_ppr",
    "seasons_overall_top12_ppr",
    "seasons_overall_top24_ppr",
]


def run(apply: bool) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='8GB'")
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for career_stem, season_stem in PAIRS:
        cpath = (ART / f"{career_stem}.parquet").as_posix()
        spath = (ART / f"{season_stem}.parquet").as_posix()
        con.execute("DROP TABLE IF EXISTS c; DROP TABLE IF EXISTS agg")
        con.execute(f"CREATE TABLE c AS SELECT * FROM '{cpath}'")
        con.execute(f"""
            CREATE TABLE agg AS
            WITH pf AS (
              SELECT NFL_player_id, ({POS_FINISH}) AS pos_finish, rank_season_overall_4pt_ppr AS ov
              FROM '{spath}' WHERE NFL_player_id IS NOT NULL
            )
            SELECT NFL_player_id,
              MIN(pos_finish) AS best_pos_finish,
              CAST(COUNT(*) FILTER (WHERE pos_finish<=5)  AS INTEGER) AS seasons_pos_top5,
              CAST(COUNT(*) FILTER (WHERE pos_finish<=12) AS INTEGER) AS seasons_pos_top12,
              CAST(COUNT(*) FILTER (WHERE pos_finish<=24) AS INTEGER) AS seasons_pos_top24,
              MIN(ov) AS best_overall_finish_ppr,
              CAST(COUNT(*) FILTER (WHERE ov<=12) AS INTEGER) AS seasons_overall_top12_ppr,
              CAST(COUNT(*) FILTER (WHERE ov<=24) AS INTEGER) AS seasons_overall_top24_ppr
            FROM pf GROUP BY NFL_player_id
        """)
        existing = {r[0] for r in con.execute("DESCRIBE c").fetchall()}
        types = {
            "best_pos_finish": "INTEGER",
            "seasons_pos_top5": "INTEGER",
            "seasons_pos_top12": "INTEGER",
            "seasons_pos_top24": "INTEGER",
            "best_overall_finish_ppr": "INTEGER",
            "seasons_overall_top12_ppr": "INTEGER",
            "seasons_overall_top24_ppr": "INTEGER",
        }
        for col in NEW_COLS:
            if col not in existing:
                con.execute(f'ALTER TABLE c ADD COLUMN "{col}" {types[col]}')
        # counts default 0; best_* NULL when no positional finish ever
        set_expr = ", ".join(
            f'"{col}" = COALESCE(a.{col}, {"NULL" if col.startswith("best_") else "0"})' for col in NEW_COLS
        )
        con.execute(f"UPDATE c SET {set_expr} FROM agg a WHERE c.NFL_player_id = a.NFL_player_id")
        # players with no season agg row -> counts 0
        con.execute(
            "UPDATE c SET "
            + ", ".join(f'"{col}" = 0' for col in NEW_COLS if not col.startswith("best_"))
            + " WHERE NFL_player_id NOT IN (SELECT NFL_player_id FROM agg)"
        )

        n0 = con.execute(f"SELECT COUNT(*) FROM '{cpath}'").fetchone()[0]
        nc = con.execute("SELECT COUNT(*) FROM c").fetchone()[0]
        lt = con.execute("""SELECT best_pos_finish, seasons_pos_top5, seasons_pos_top12,
            best_overall_finish_ppr, seasons_overall_top12_ppr FROM c WHERE player='LaDainian Tomlinson'""").fetchall()
        print(
            f"[{career_stem}] rows {nc:,} cols {len(con.execute('DESCRIBE c').fetchall())} | LT [best_pos, top5, top12, best_ov, ov_top12]: {lt}"
        )
        if not apply:
            continue
        assert nc == n0, "row count changed"
        assert con.execute("SELECT COUNT(*)=COUNT(DISTINCT NFL_player_id) FROM c").fetchone()[0], "key not unique"
        dest = ART / f"{career_stem}.parquet"
        backup = ART / f"{career_stem}.parquet.bak_finish_{ts}"
        shutil.copy2(dest, backup)
        tmp = ART / f"{career_stem}.parquet.tmp"
        rb = con.execute("SELECT * FROM c").fetch_record_batch(50000)
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
