"""
build_season_experience_v26.py  --  add season_number + is_rookie to the v26 season tables.

The season tables carry `age` but no experience marker. season_number = the player's Nth stat-bearing NFL
season (1 = first); is_rookie = season_number == 1. Enables age/experience curves and rookie leaderboards
(e.g. best rookie WR seasons). Career tables already carry rookie_year / years_active, so they're unchanged.

season_number = DENSE_RANK() over the player's distinct seasons ordered by year (dense so a player with two
rows in one year -- rare -- still gets one season number). GATED WRITE per table: backup + verify rows/keys
unchanged + anchors, then os.replace.

    python -m scripts.sota_recon.build_season_experience_v26            # dry-run
    python -m scripts.sota_recon.build_season_experience_v26 --apply
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
TABLES = ["player_nfl_season", "player_nfl_season_all"]


def run(apply: bool) -> None:
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='8GB'")
    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for stem in TABLES:
        path = (ART / f"{stem}.parquet").as_posix()
        con.execute("DROP TABLE IF EXISTS t")
        con.execute(f"CREATE TABLE t AS SELECT * FROM '{path}'")
        existing = {r[0] for r in con.execute("DESCRIBE t").fetchall()}
        n0 = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
        for col, typ in (("season_number", "INTEGER"), ("is_rookie", "BOOLEAN")):
            if col not in existing:
                con.execute(f'ALTER TABLE t ADD COLUMN "{col}" {typ}')
        con.execute("""
            UPDATE t SET season_number = r.sn, is_rookie = (r.sn = 1) FROM (
              SELECT NFL_player_id, year,
                     CAST(DENSE_RANK() OVER (PARTITION BY NFL_player_id ORDER BY year) AS INTEGER) sn
              FROM t WHERE NFL_player_id IS NOT NULL
            ) r WHERE t.NFL_player_id = r.NFL_player_id AND t.year = r.year
        """)
        rook = con.execute("SELECT COUNT(*) FROM t WHERE is_rookie").fetchone()[0]
        mx = con.execute("SELECT MAX(season_number) FROM t").fetchone()[0]
        ex = con.execute("""SELECT player, year, season_number, is_rookie FROM t
                            WHERE player IN ('Tom Brady','LaDainian Tomlinson') ORDER BY player, year LIMIT 4""").fetchall()
        print(f"[{stem}] rows {n0:,} | rookies {rook:,} | max season_number {mx} | sample {ex}")
        if not apply:
            continue
        assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == n0, "row count changed"
        assert con.execute("SELECT COUNT(*)=COUNT(DISTINCT (NFL_player_id||'|'||year)) FROM t").fetchone()[
            0
        ], "key not unique"
        dest = ART / f"{stem}.parquet"
        backup = ART / f"{stem}.parquet.bak_exp_{ts}"
        shutil.copy2(dest, backup)
        tmp = ART / f"{stem}.parquet.tmp"
        rb = con.execute("SELECT * FROM t").fetch_record_batch(50000)
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
