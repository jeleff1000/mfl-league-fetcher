"""
sota_recon/build_team_dst_advanced_v26.py  --  fold the TEAM-DEFENSE advanced "allowed" mirror
onto the v26 super table's DEF rows, in place (backup + atomic swap).

At the team level there is no per-player attribution problem, so these are clean: for each
team-week we group the merged PBP by defteam and compute what the defense ALLOWED --
def_epa_allowed (total/pass/rush), def_wpa_allowed, def_success_allowed, def_explosive_*_
allowed, def_yards_allowed, third/fourth-down faced/allowed, red-zone faced/TD-allowed.

Joined onto DEF rows on the CANONICAL franchise/team code (recon_common.canon_team_sql), NOT
the raw abbreviation -- so OAK/LV, SD/LAC, STL/LA, etc. all land correctly (verified 100% for
2010/2016/2020/2023, 96.6% for 1985). Non-DEF rows get NULL. EPA/WP/success-allowed are NULL
before 1999 (win-prob era); the model-free allowed stats (yards, explosive, downs, red-zone)
extend to 1978.

Source: pbp_team_defense_week.parquet (scripts/build_team_defense_rollup.py --output).
Gate: row count unchanged, all 16 cols present, def_epa_allowed populated only on DEF rows &
only 1999+, golden_samples 56/56 -> backup + swap. Idempotent.

    python -m scripts.sota_recon.build_team_dst_advanced_v26            # dry-run
    python -m scripts.sota_recon.build_team_dst_advanced_v26 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from .recon_common import canon_team_sql, utc_stamp
from .sources import latest_v26

TD_SOURCE = Path("D:/league-history-data/nfl/raw/stathead/generated/pbp_team_defense_1978_2025/pbp_team_defense_week.parquet")
EP_ERA_START = 1999

# EPA/WP-model "allowed" atoms -> NULL before the win-prob era (1999).
DEF_EP_GATED = ["def_epa_allowed", "def_pass_epa_allowed", "def_rush_epa_allowed",
                "def_wpa_allowed", "def_success_allowed", "def_success_plays"]
# Model-free "allowed" atoms -> real for every year the PBP covers (1978+).
DEF_MODEL_FREE = ["def_plays", "def_explosive_pass_allowed", "def_explosive_rush_allowed",
                  "def_yards_allowed", "def_third_down_faced", "def_third_down_allowed",
                  "def_fourth_down_faced", "def_fourth_down_allowed",
                  "def_rz_plays_faced", "def_rz_td_allowed"]
DEF_TIER1 = ["def_attempts_allowed", "def_carries_allowed",
             "passing_first_downs_allowed", "rushing_first_downs_allowed",
             "receiving_first_downs_allowed"]
DEF_COLS = DEF_TIER1 + DEF_EP_GATED + DEF_MODEL_FREE


def _tda_sql() -> str:
    ct = canon_team_sql("nfl_team")
    parts = []
    for c in DEF_COLS:
        if c in DEF_EP_GATED:
            parts.append(f"CASE WHEN CAST(year AS INTEGER) >= {EP_ERA_START} THEN SUM({c}) ELSE NULL END AS {c}")
        else:
            parts.append(f"SUM({c}) AS {c}")
    return (f"SELECT {ct} AS ct, CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week, "
            f"{', '.join(parts)} FROM read_parquet('{TD_SOURCE.as_posix()}') GROUP BY 1, 2, 3")


def _def_lookup_sql(v26: str) -> str:
    # Resolve the canonical-code match ONCE per distinct DEF (team, year, week) (~24k rows) so the
    # streaming rewrite joins on plain raw keys -- no 30-branch canon CASE evaluated per row.
    ct_s = canon_team_sql("sd.nfl_team")
    dcols = ", ".join(f"t.{c}" for c in DEF_COLS)
    return f"""
        SELECT sd.nfl_team, sd.year, sd.week, {dcols}
        FROM (
            SELECT DISTINCT nfl_team, CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
            FROM read_parquet('{v26}') WHERE position = 'DEF' AND nfl_team IS NOT NULL
        ) sd
        LEFT JOIN tda t
          ON {ct_s} = t.ct AND sd.year = t.year AND sd.week = t.week
    """


def _select_sql(v26: str, exclude: list[str]) -> str:
    star = "s.*" if not exclude else f"s.* EXCLUDE ({', '.join(exclude)})"
    dcols = ",\n            ".join(f"d.{c}" for c in DEF_COLS)
    # join fires ONLY for DEF rows (via def_lookup keys), so non-DEF players never inherit their
    # team's allowed stats; plain equi-join on raw (nfl_team, year, week).
    return f"""
        SELECT
            {star},
            {dcols}
        FROM read_parquet('{v26}') s
        LEFT JOIN def_lookup d
          ON s.position = 'DEF'
         AND s.nfl_team = d.nfl_team
         AND CAST(s.year AS INTEGER) = d.year
         AND CAST(s.week AS INTEGER) = d.week
    """


def run(apply: bool) -> dict:
    if not TD_SOURCE.exists():
        raise FileNotFoundError(f"team-defense rollup not found: {TD_SOURCE}")
    v26 = latest_v26()
    stamp = utc_stamp()
    con = duckdb.connect()
    con.execute("PRAGMA threads=1")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='900MB'")  # coexist with other jobs on this low-RAM box
    _tmp = Path("D:/league-history-data/nfl/tmp/duckdb")
    _tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{_tmp.as_posix()}'")

    vqp = Path(v26).as_posix()
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{vqp}')").fetchall()]
    existing = [c for c in DEF_COLS if c in cols]
    con.execute(f"CREATE TEMP TABLE tda AS {_tda_sql()}")
    con.execute(f"CREATE TEMP TABLE def_lookup AS {_def_lookup_sql(vqp)}")
    before = con.execute(f"SELECT COUNT(*) FROM read_parquet('{Path(v26).as_posix()}')").fetchone()[0]
    info = {"existing_def_cols": len(existing), "adds": len(DEF_COLS)}
    if not apply:
        con.close()
        return {"before": int(before), "info": info, "swapped": False}

    sql = _select_sql(Path(v26).as_posix(), existing)
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_teamdst.parquet")
    # Native DuckDB COPY with a small row group streams the wide (~1050-col) join without the
    # PyArrow round-trip that OOM-killed the record-batch writer on this low-RAM box.
    con.execute(f"COPY ({sql}) TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 10000)")

    tq = f"read_parquet('{tmp.as_posix()}')"
    after = con.execute(f"SELECT COUNT(*) FROM {tq}").fetchone()[0]
    out_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {tq}").fetchall()}
    missing = [c for c in DEF_COLS if c not in out_cols]
    # correctness gates: allowed stats ONLY on DEF rows; EPA-allowed ONLY 1999+.
    nondef_leak = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE position IS DISTINCT FROM 'DEF' AND def_epa_allowed IS NOT NULL").fetchone()[0]
    pre99_epa = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE CAST(year AS INTEGER) < {EP_ERA_START} AND def_epa_allowed IS NOT NULL").fetchone()[0]
    def_pop = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE position='DEF' AND CAST(year AS INTEGER)=2023 AND def_epa_allowed IS NOT NULL").fetchone()[0]
    con.close()

    import scripts.sota_recon.sources as S
    o = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        from . import golden_samples
        g = golden_samples.run()
    finally:
        S.latest_v26 = o

    gate = (after == before and not missing and nondef_leak == 0 and pre99_epa == 0
            and def_pop > 0 and g["failed"] == 0)
    res = {"before": int(before), "after": int(after), "info": info, "missing": missing,
           "nondef_leak": nondef_leak, "pre1999_epa_allowed": pre99_epa, "def_2023_populated": def_pop,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preteamdst_backup_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(a.apply)
    if not a.apply:
        print("DRY-RUN:", r["info"], "| rows", f"{r['before']:,}")
    else:
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']} | missing {r['missing']} | "
              f"nondef_leak {r['nondef_leak']} | pre99_epa {r['pre1999_epa_allowed']} | def2023_pop {r['def_2023_populated']:,}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
