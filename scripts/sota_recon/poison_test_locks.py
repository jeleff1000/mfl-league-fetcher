"""THE POISON TEST v2: a gate does not exist until watched refusing.

v1 rebuilt the whole plane to flip one atom and OOM'd twice on a shared box.
v2 never builds a monolith: the poisoned plane is TWO files read as a glob --
  part A: plain filtered copy of year != 2024 (streaming, no expressions);
  part B: the small 2024 slice with ONE atom flipped (+7), target chosen
          beforehand and injected as constants (no scalar subqueries).
The gate run is pointed at the glob via a monkeypatched latest_v26 -- DuckDB
reads 'dir/*.parquet' exactly like a file.

Exit 0 = gate refused the poison AND passed clean: the locks are real.
Exit 1 = the gate swallowed the poison: fiction; fix the gate.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LOCKFILE = Path(__file__).parent / "witness_gate" / "contracts" / "witness_locks.v1.json"


def main() -> int:
    locks = json.loads(LOCKFILE.read_text(encoding="utf-8"))["locks"]
    week_locks = [l for l in locks
                  if l.get("grain") == "week" and l.get("agree") == 1.0
                  and l.get("window") == "2024"]
    target = (week_locks or locks)[0]
    col = target["column"]
    print(f"poisoning one 2024 atom of {col} "
          f"(locked {target['agree']} via {target['source']})", flush=True)

    src = Path(S.latest_v26())
    pdir = src.parent / "POISON_PARTS"
    if pdir.exists():
        shutil.rmtree(pdir)
    pdir.mkdir()
    con = duckdb.connect()
    con.execute("SET memory_limit='700MB'")
    con.execute("SET threads=1")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET temp_directory='D:/league-history-data/nfl/derived/"
                "validation/sota_recon_master/duck_spill'")

    # the atom MUST be one the locked lane actually witnesses -- v2 flipped
    # a plane cell outside the targeted crawl's coverage and the compare
    # never saw it (fiction #2, caught by the poison itself). Pick the
    # target from the witness join.
    import scripts.sota_recon.witness_map as W
    from scripts.sota_recon.vouch_2024 import lane_sql, fingerprint
    sp = next(s for s in W.WITNESS_MAP
              if fingerprint(s) == target["fingerprint"])
    wsql, keys = lane_sql(sp)
    assert wsql and keys == ("pid", "yr", "wk"), "need a player-week lane"
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{src.as_posix()}')
    WHERE season_type = 'REG'""")
    pid, wkn = con.execute(f"""
    WITH w AS ({wsql})
    SELECT w.pid, w.wk FROM w
    JOIN plane t ON t.NFL_player_id = w.pid AND t.year = w.yr
      AND t.week = w.wk
    WHERE w.yr = 2024 AND t.{col} IS NOT NULL
    ORDER BY w.pid, w.wk LIMIT 1""").fetchone()
    print(f"target atom (WITNESSED by the lane): {pid} wk{wkn}", flush=True)

    print("part A: plain copy of every other year", flush=True)
    con.execute(f"""
    COPY (SELECT * FROM read_parquet('{src.as_posix()}')
          WHERE year IS DISTINCT FROM 2024)
    TO '{(pdir / "rest.parquet").as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE 20000)""")
    print("part B: poisoned 2024 slice", flush=True)
    con.execute(f"""
    COPY (SELECT * REPLACE (
        CASE WHEN season_type='REG' AND NFL_player_id='{pid}'
              AND week={wkn} AND {col} IS NOT NULL
             THEN TRY_CAST({col} AS DOUBLE) + 7
             ELSE TRY_CAST({col} AS DOUBLE) END AS {col})
      FROM read_parquet('{src.as_posix()}') WHERE year = 2024)
    TO '{(pdir / "y2024.parquet").as_posix()}' (FORMAT parquet, ROW_GROUP_SIZE 20000)""")
    con.close()
    glob = (pdir.as_posix() + "/*.parquet").replace("'", "")

    def gate(plane_path: str) -> int:
        code = (
            "import sys; sys.path.insert(0, '.');"
            "import scripts.sota_recon.sources as S;"
            f"S.latest_v26 = lambda: r'{plane_path}';"
            "import pytest;"
            "sys.exit(pytest.main(['-q',"
            "'scripts/sota_recon/test_witness_locks.py']))")
        return subprocess.run(
            [sys.executable, "-c", code],
            cwd=str(Path(__file__).resolve().parents[2])).returncode

    poisoned_failed = gate(glob) != 0
    shutil.rmtree(pdir, ignore_errors=True)
    clean_passed = gate(str(src)) == 0
    print(f"POISON refused: {poisoned_failed} | CLEAN passes: {clean_passed}",
          flush=True)
    if poisoned_failed and clean_passed:
        print("THE GATE IS REAL: it refused the flipped atom and passed clean")
        return 0
    print("THE GATE IS FICTION -- fix the gate, not the story")
    return 1


if __name__ == "__main__":
    sys.exit(main())
