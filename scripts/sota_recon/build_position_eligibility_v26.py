"""
sota_recon/build_position_eligibility_v26.py  --  season/career position eligibility.

The weekly super table carries PFR-led display/fantasy positions per player-season. Season and career
artifacts need the same compact eligibility set, plus tightly bounded stat-derived add-ons for kicking,
punting, and modern coverage-volume two-way seasons:

  SEASON.season_positions = primary position
        + 'K' if that season's FG attempts >= 5 OR XP made >= 5   (a genuine kicking role, not emergency)
        + 'P' if that season's punts >= 5
        + 'DB' for offensive-position seasons only when defensive coverage volume is high
    (canonical fantasy order, comma-joined, distinct). Gilchrist 1962 -> "RB,K".
  CAREER.career_positions = union of the player's season_positions (so career picks up K too).

Bounded: ~247 dual-K (fga>=5) + ~585 dual-P seasons. Reads the per-season kick/punt aggregates from the
v26 super table; enriches the 4 season/career artifacts (season adds season_positions; career REPLACES
career_positions). Gate: row counts unchanged; Gilchrist season_positions contains 'K'; career too.

    python -m scripts.sota_recon.build_position_eligibility_v26 [--apply]
"""

from __future__ import annotations
import argparse
import os
import shutil
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
from .sources import latest_v26
from .position_tokens import canonical_position_list_sql


# This module remains only for reproducibility of the retired v26 artifact. It is
# not an authority for position or fantasy eligibility; current declarations come
# from build_position_declaration.py and PFR's per-season Pos field.
DEPRECATED_NON_AUTHORITATIVE = True


def D(c: str) -> str:
    return f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _elig(con, v26):
    """per (NFL_player_id, year): kicked? punted? -> eligibility add-ons."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE elig AS
        SELECT NFL_player_id, year,
          list_sort(list_distinct(flatten(array_agg(string_split(position, ',')) FILTER (WHERE position IS NOT NULL)))) AS pos_list,
          SUM({D("fg_att")}) fga, SUM({D("pat_made")}) xpm, SUM({D("punts")}) punts,
          SUM({D("def_pass_defended")}) pd, SUM({D("def_interceptions")}) di,
          SUM({D("def_sacks")}) sk, SUM({D("def_tackles_solo")}) solo,
          SUM({D("def_tackle_assists")}) ast,
          SUM({D("def_tackles_with_assist")}) comb_tackles,
          SUM({D("receptions")}) rec, SUM({D("receiving_yards")}) rec_yds, SUM({D("receiving_tds")}) rec_td,
          SUM({D("carries")}) car, SUM({D("rushing_yards")}) rush_yds, SUM({D("rushing_tds")}) rush_td
        FROM '{v26}' GROUP BY 1,2""")


def run(apply=False):
    v26 = Path(latest_v26()).as_posix()
    art = Path(latest_v26()).parent / "season_career_v26"
    con = duckdb.connect()
    con.execute("PRAGMA threads=3")
    con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    sp = art / ".posspill"
    sp.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory='{sp.as_posix()}'")
    _elig(con, v26)
    if not apply:
        n = con.execute("""SELECT
              SUM(CASE WHEN fga>=5 OR xpm>=5 THEN 1 ELSE 0 END) k,
              SUM(CASE WHEN punts>=5 THEN 1 ELSE 0 END) p,
              SUM(CASE WHEN list_has_any(pos_list,['QB','RB','WR','TE']) AND NOT list_has_any(pos_list,['DB','LB','DL'])
                       AND ((pd>=3 AND GREATEST(COALESCE(solo,0)+COALESCE(ast,0),COALESCE(comb_tackles,0))>=10) OR di>=3)
                  THEN 1 ELSE 0 END) off_to_def,
              0 AS def_to_off
            FROM elig""").fetchone()
        con.close()
        return {"dual_k": n[0], "dual_p": n[1], "off_to_def": n[2], "def_to_off": n[3]}

    # season eligibility = weekly position UNION + stat-derived add-ons (K/P + offense<->defense
    # crossover with stat-profile-inferred position). sorted distinct, comma-joined.
    _E = "CAST([] AS VARCHAR[])"  # typed empty list
    OFF = "['QB','RB','WR','TE']"
    DEFN = "['DB','LB','DL']"
    _pl = "COALESCE(e.pos_list, " + _E + ")"
    has_off = f"list_has_any({_pl},{OFF})"
    has_def = f"list_has_any({_pl},{DEFN})"
    pd = "COALESCE(e.pd,0)"
    di = "COALESCE(e.di,0)"
    tkl = "GREATEST(COALESCE(e.solo,0) + COALESCE(e.ast,0), COALESCE(e.comb_tackles,0))"
    # Offense->defense is deliberately narrow. PFR composite positions already carry true
    # historical dual-role seasons; stat inference is only for modern coverage-volume cases like
    # Travis Hunter, not one-off special-teams tackles or a single offensive-player sack.
    off_def_trig = f"(({pd}>=3 AND {tkl}>=10) OR {di}>=3)"
    ELIG_LIST = canonical_position_list_sql(
        "list_concat("
        f"{_pl}, "
        f"CASE WHEN COALESCE(e.fga,0)>=5 OR COALESCE(e.xpm,0)>=5 THEN ['K'] ELSE {_E} END, "
        f"CASE WHEN COALESCE(e.punts,0)>=5 THEN ['P'] ELSE {_E} END, "
        f"CASE WHEN {has_off} AND NOT {has_def} AND {off_def_trig} THEN ['DB'] ELSE {_E} END "
        ")"
    )
    ELIG_STRING = f"NULLIF(array_to_string({ELIG_LIST}, ','), '')"
    results = {}
    tmps = {}
    for t in ["player_nfl_season", "player_nfl_season_all"]:
        src = (art / f"{t}.parquet").as_posix()
        tmp = (art / f"{t}_pos.parquet").as_posix()
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()}
        exclude = [c for c in ("position", "season_positions") if c in cols]
        star = f"s.* EXCLUDE ({', '.join(exclude)})" if exclude else "s.*"
        sql = (
            f"SELECT {star}, {ELIG_STRING} AS position, "
            f"{ELIG_STRING} AS season_positions "
            f"FROM '{src}' s LEFT JOIN elig e ON s.NFL_player_id=e.NFL_player_id AND s.year=e.year"
        )
        before = con.execute(f"SELECT COUNT(*) FROM '{src}'").fetchone()[0]
        r = con.execute(sql).fetch_record_batch(50000)
        w = pq.ParquetWriter(tmp, r.schema)
        for b in r:
            w.write_batch(b)
        w.close()
        after = con.execute(f"SELECT COUNT(*) FROM '{tmp}'").fetchone()[0]
        results[t] = (before, after)
        tmps[t] = (src, tmp)
    # career_positions = union of season_positions (from the just-built season eligibility)
    for t, ssrc in zip(["player_nfl_career", "player_nfl_career_all"], ["player_nfl_season", "player_nfl_season_all"]):
        src = (art / f"{t}.parquet").as_posix()
        tmp = (art / f"{t}_pos.parquet").as_posix()
        se = tmps[ssrc][1]
        career_list = canonical_position_list_sql("flatten(array_agg(string_split(season_positions, ',')))")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE cpos AS
            SELECT NFL_player_id, NULLIF(array_to_string({career_list}, ','), '') career_positions
            FROM '{se}' WHERE season_positions IS NOT NULL AND season_positions<>'' GROUP BY NFL_player_id""")
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()}
        exclude = [c for c in ("position", "career_positions") if c in cols]
        star = f"c.* EXCLUDE ({', '.join(exclude)})" if exclude else "c.*"
        sql = (
            f"SELECT {star}, COALESCE(cp.career_positions, c.position) AS position, cp.career_positions "
            f"FROM '{src}' c LEFT JOIN cpos cp ON c.NFL_player_id=cp.NFL_player_id"
        )
        before = con.execute(f"SELECT COUNT(*) FROM '{src}'").fetchone()[0]
        r = con.execute(sql).fetch_record_batch(50000)
        w = pq.ParquetWriter(tmp, r.schema)
        for b in r:
            w.write_batch(b)
        w.close()
        after = con.execute(f"SELECT COUNT(*) FROM '{tmp}'").fetchone()[0]
        results[t] = (before, after)
        tmps[t] = (src, tmp)
    # anchors: the named dual-eligibility cases must resolve correctly
    se = tmps["player_nfl_season"][1]
    ce = tmps["player_nfl_career"][1]

    def _sp(player, year):
        r = con.execute(f"SELECT season_positions FROM '{se}' WHERE player='{player}' AND year={year}").fetchone()
        return r[0] if r else None

    anchors = {
        "Gilchrist_1962": _sp("Cookie Gilchrist", 1962),  # expect K,RB
        "Hunter_2025": _sp("Travis Hunter", 2025),  # expect DB,WR
        "Walker_2025": _sp("Devontez Walker", 2025),  # expect WR only
        "Deion_1996": _sp("Deion Sanders", 1996),  # expect DB,WR
        "Deion_1993": _sp("Deion Sanders", 1993),  # expect DB only (6 rec < 10)
        "Blanda_1962": _sp("George Blanda", 1962),  # expect K,QB
    }
    con.close()
    rows_ok = all(b == a for b, a in results.values())
    anchors_ok = (
        anchors["Gilchrist_1962"] == "RB,K"
        and anchors["Hunter_2025"] == "WR,DB"
        and (anchors["Walker_2025"] or "") == "WR"
        and anchors["Deion_1996"] == "WR,DB"
        and (anchors["Deion_1993"] or "") == "DB"
        and anchors["Blanda_1962"] == "QB,K"
    )
    gate = rows_ok and anchors_ok
    res = {
        "rows": {t: f"{b}->{a}" for t, (b, a) in results.items()},
        "rows_ok": rows_ok,
        "anchors": anchors,
        "gate_pass": bool(gate),
    }
    if gate:
        from .recon_common import utc_stamp

        stamp = utc_stamp()
        for t, (src, tmp) in tmps.items():
            bk = Path(src).with_name(Path(src).stem + f"_prepos_{stamp}.parquet")
            shutil.copy2(src, bk)
            os.replace(tmp, src)
        res["swapped"] = True
    else:
        res["swapped"] = False
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if not a.apply:
        print("DRY:", run())
    else:
        r = run(apply=True)
        print(f"rows: {r['rows']}")
        print(f"anchors: {r['anchors']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else "NOT swapped"))
