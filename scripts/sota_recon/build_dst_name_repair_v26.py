"""
sota_recon/build_dst_name_repair_v26.py  --  wave53: defunct DST display names repaired.

Joe's find (2026-07-11): the 70-points-allowed board showed the 1950 Baltimore Colts
DST as "Brooklyn Lions". The franchise ids are CORRECT everywhere (DEF-132 rows carry
fid 132 / code BCL); the player_bio NAMES for defunct DST pseudo-players are wrong in
two classes: (a) shifted assignments (BCL Colts named Brooklyn Lions, BRL Lions named
Brooklyn Tigers, BDA Dodgers named Boston/NY Bulldogs, CHR Rockets named Cincinnati
Reds) and (b) modern-franchise nicknames pasted onto same-code defunct teams (1920s
Buffalo All-Americans as "Bills", Miami Seahawks 1946 as "Dolphins", Boston Yanks as
"Redskins", ...). No active generator produces these rows -- this curated, PFR-sourced
anchor map IS the pipeline fix, recorded as facts.

Every correction asserts the CURRENT (wrong) value -- a mismatch means the row moved
under us and the fix refuses (stale-fact discipline). Gates: only listed rows change,
row count unchanged, every listed row updated.

    python -m scripts.sota_recon.build_dst_name_repair_v26            # DRY RUN
    python -m scripts.sota_recon.build_dst_name_repair_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb

from .recon_common import utc_stamp
from .sources import PLAYER_BIO, latest_v26

WAVE = "wave53.dst_name_repair"

# NFL_player_id -> (asserted current name, corrected name). Franchise identity from
# nfl_team_games_all fid/code spans; names per PFR franchise pages.
REPAIRS: dict[str, tuple[str, str]] = {
    # class (a): shifted assignments
    "DEF-116": ("Brooklyn Tigers DST", "Brooklyn Lions DST"),          # BRL 1926
    "DEF-132": ("Brooklyn Lions DST", "Baltimore Colts DST"),          # BCL 1947-50
    "DEF-133": ("Boston/NY Bulldogs DST", "Brooklyn Dodgers (AAFC) DST"),  # BDA 1946-48
    "DEF-134": ("Cincinnati Reds DST", "Chicago Rockets DST"),         # CHR 1946-48
    # class (b): modern nicknames on defunct same-code teams
    "DEF-137": ("Texans DST", "Dallas Texans DST"),                    # DTX 1952
    "DEF-139": ("Bills DST", "Buffalo All-Americans DST"),             # BUF 1920-29
    "DEF-140": ("Bills DST", "Buffalo Bills (AAFC) DST"),              # BUF 1946-49
    "DEF-142": ("Bengals DST", "Cincinnati Celts/Reds DST"),           # CIN 1921-34
    "DEF-143": ("Browns DST", "Cleveland Bulldogs DST"),               # CLE 1920-31
    "DEF-144": ("Spartans DST", "Detroit Heralds/Wolverines DST"),     # DET 1920-28
    "DEF-145": ("Texans DST", "Kansas City Cowboys DST"),              # KAN 1924-26
    "DEF-146": ("Dolphins DST", "Miami Seahawks DST"),                 # MIA 1946 (AAFC)
    "DEF-147": ("Vikings DST", "Minneapolis Marines DST"),             # MIN 1921-30
    "DEF-148": ("Redskins DST", "Boston Yanks DST"),                   # BOS 1944-48
    "DEF-150": ("Browns DST", "St. Louis All-Stars/Gunners DST"),      # STL 1923-34
    "DEF-151": ("Braves DST", "Washington Senators DST"),              # WAS 1921
}


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    bio_p = Path(PLAYER_BIO.path)
    bq = _q(PLAYER_BIO)
    current = dict(con.execute(f"""
        SELECT NFL_player_id, player FROM '{bq}'
        WHERE NFL_player_id IN ({", ".join(f"'{k}'" for k in REPAIRS)})""").fetchall())
    stale, missing, ok = [], [], []
    for pid, (want_old, new) in REPAIRS.items():
        cur = current.get(pid)
        if cur is None:
            missing.append(pid)
        elif cur != want_old:
            stale.append((pid, cur, want_old))
        else:
            ok.append((pid, want_old, new))
    diag = {"repairs_listed": len(REPAIRS), "appliable": len(ok),
            "stale_assertions": stale, "missing_rows": missing}
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}
    if stale or missing:
        con.close()
        return {"mode": "APPLY-REFUSED", **diag,
                "reason": "stale assertion or missing row -- adjudicate, never force"}

    con.execute("CREATE TEMP TABLE rep (pid VARCHAR, new_name VARCHAR)")
    con.executemany("INSERT INTO rep VALUES (?, ?)", [(p, n) for p, _, n in ok])
    tmp = bio_p.with_name(bio_p.stem + "_w53.parquet")
    con.execute(f"""
        COPY (
          SELECT b.* REPLACE (COALESCE(rep.new_name, b.player) AS player)
          FROM '{bq}' b LEFT JOIN rep ON rep.pid = b.NFL_player_id
        ) TO '{Path(tmp).as_posix()}' (FORMAT PARQUET)""")
    tq = Path(tmp).as_posix()
    before = con.execute(f"SELECT COUNT(*) FROM '{bq}'").fetchone()[0]
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    changed = con.execute(f"""
        SELECT COUNT(*) FROM '{tq}' t JOIN '{bq}' b USING (NFL_player_id)
        WHERE COALESCE(t.player, '') != COALESCE(b.player, '')""").fetchone()[0]
    applied = con.execute(f"""
        SELECT COUNT(*) FROM '{tq}' t JOIN rep ON rep.pid = t.NFL_player_id
        WHERE t.player = rep.new_name""").fetchone()[0]
    gate = after == before and changed == len(ok) and applied == len(ok)
    res = {"mode": "APPLY", **diag, "bio_before": before, "bio_after": after,
           "rows_changed": changed, "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(_q(latest_v26())))
        for pid, old, new in ok:
            facts.emit_fact("cell_override", con=fc, table_name="player_bio",
                            target_key=pid, column_name="player",
                            old_value=old, new_value=new,
                            wave_id=WAVE,
                            reason="defunct DST display name wrong (fid/code identity "
                                   "verified against nfl_team_games_all; name per PFR "
                                   "franchise history)",
                            witness="nfl_team_games_all fid+code spans; PFR franchise pages",
                            source_snapshot_id=snap)
        fc.close()
        bk = bio_p.with_name(bio_p.stem + f"_prew53_{utc_stamp()}.parquet")
        shutil.copy2(bio_p, bk)
        os.replace(tmp, bio_p)
        res.update(backup=str(bk), swapped=True)
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
