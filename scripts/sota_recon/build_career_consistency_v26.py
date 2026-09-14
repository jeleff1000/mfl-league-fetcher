"""
build_career_consistency_v26.py  --  add career week-to-week consistency to the v26 career tables.

The season tables carry consistency_{td}_{ppr} = the per-(player,year) coefficient of variation of weekly
fantasy points (STDDEV/AVG). The career tables had none. This adds the SAME metric at career scope: the CV
of a player's weekly fantasy points across his ENTIRE career (grouped by player, not year) -- computed fresh
from the weekly super table, NOT a naive average of season CVs (averaging ratios is statistically wrong).

Formula matches build_weekly_ppg_all_v26 exactly: CASE WHEN AVG(fpts)>0 THEN ROUND(STDDEV(fpts)/AVG(fpts),3)
ELSE 0 END, over weekly rows where fpts IS NOT NULL (AVG/STDDEV skip NULLs). Lower = more consistent. Both
player_nfl_career and _all get the same per-player value (it's a player-level stat). 15 scoring variants.

GATED WRITE per table: backup + verify rows/keys unchanged, then os.replace.

    python -m scripts.sota_recon.build_career_consistency_v26            # dry-run
    python -m scripts.sota_recon.build_career_consistency_v26 --apply
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
CAREER_TABLES = ["player_nfl_career", "player_nfl_career_all"]
TDS = ("4pt", "5pt", "6pt")
PPRS = ("0ppr", "half", "ppr", "tep", "ppfd")
VARIANTS = [f"{td}_{ppr}" for td in TDS for ppr in PPRS]  # 15


def run(apply: bool) -> None:
    v = Path(latest_v26()).as_posix()
    con = duckdb.connect()
    con.execute("PRAGMA threads=4")
    con.execute("SET memory_limit='10GB'")

    # per-player career CV for each variant (AVG/STDDEV auto-skip NULL weekly fpts == "WHERE fpts NOT NULL")
    cv_exprs = ",\n          ".join(
        f"CASE WHEN AVG(fpts_{var})>0 THEN ROUND(STDDEV(fpts_{var})/AVG(fpts_{var}),3) ELSE 0 END "
        f"AS consistency_{var}"
        for var in VARIANTS
    )
    con.execute(f"""
        CREATE TABLE cvagg AS
        SELECT NFL_player_id,
          {cv_exprs}
        FROM '{v}' WHERE NFL_player_id IS NOT NULL GROUP BY NFL_player_id
    """)
    newcols = [f"consistency_{var}" for var in VARIANTS]

    ts = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    for stem in CAREER_TABLES:
        path = (ART / f"{stem}.parquet").as_posix()
        con.execute("DROP TABLE IF EXISTS c")
        con.execute(f"CREATE TABLE c AS SELECT * FROM '{path}'")
        existing = {r[0] for r in con.execute("DESCRIBE c").fetchall()}
        n0 = con.execute("SELECT COUNT(*) FROM c").fetchone()[0]
        for col in newcols:
            if col not in existing:
                con.execute(f'ALTER TABLE c ADD COLUMN "{col}" DOUBLE')
        set_expr = ", ".join(f'"{col}" = a.{col}' for col in newcols)
        con.execute(f"UPDATE c SET {set_expr} FROM cvagg a WHERE c.NFL_player_id = a.NFL_player_id")

        nc = con.execute("SELECT COUNT(*) FROM c").fetchone()[0]
        pop = con.execute("SELECT COUNT(consistency_4pt_ppr) FROM c").fetchone()[0]
        lt = con.execute(
            "SELECT consistency_4pt_ppr, consistency_6pt_ppr FROM c WHERE player='LaDainian Tomlinson'"
        ).fetchall()
        print(
            f"[{stem}] rows {nc:,} cols {len(con.execute('DESCRIBE c').fetchall())} | "
            f"consistency_4pt_ppr populated {pop:,} | LT [4pt_ppr,6pt_ppr]: {lt}"
        )
        if not apply:
            continue
        assert nc == n0, "row count changed"
        assert con.execute("SELECT COUNT(*)=COUNT(DISTINCT NFL_player_id) FROM c").fetchone()[0], "key not unique"
        dest = ART / f"{stem}.parquet"
        backup = ART / f"{stem}.parquet.bak_cons_{ts}"
        shutil.copy2(dest, backup)
        tmp = ART / f"{stem}.parquet.tmp"
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
