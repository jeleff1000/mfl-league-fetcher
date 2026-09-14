"""
sota_recon/golden_grid.py  --  the GOLDEN-MASTER GRID: ~1,000+ frozen leader anchors.

golden_samples (56) and golden_matrix (record book) pin hand-picked truths. This grid scales to
EVERY countable stat x EVERY decade-era x EVERY grain (single-game / season / career) PLUS team
defenses -- by freezing the v26 leader (holder + value + year) for each cell. The external
authority lanes (season_authority, player_season, oracle) prove the CURRENT values are correct;
this grid LOCKS them so any future rebuild that moves a single leader fails loudly.

  * first run with no frozen file -> writes golden_grid_frozen.json (the master) and reports N.
  * subsequent runs -> recompute and diff vs frozen; any changed/missing cell = FAIL.
  * CEILINGS: a handful of hard all-time single-game/season records that NO cell may exceed
    (external sanity so a wrong frozen value can't silently become "golden").

Grains: week (single game), season (player-year), career (player), team_season (NFL team-year DST).
Eras: decades 1920-2025 + alltime. Determinism: leader = value DESC, NFL_player_id ASC.

    python -m scripts.sota_recon.golden_grid              # compare vs frozen (or freeze if none)
    python -m scripts.sota_recon.golden_grid --refreeze   # overwrite the master (use deliberately)
    python -m scripts.sota_recon.golden_grid --fails      # only show drifted cells
"""
from __future__ import annotations
import argparse, json, os
from pathlib import Path
import duckdb
from .sources import latest_v26

FROZEN = Path("D:/league-history-data/nfl/derived/validation/golden_grid_frozen.json")
ERAS = [(1920, 1939), (1940, 1949), (1950, 1959), (1960, 1969), (1970, 1979),
        (1980, 1989), (1990, 1999), (2000, 2009), (2010, 2019), (2020, 2025)]

# countable stats that aggregate by SUM (max single-game / season-sum / career-sum leaders)
STATS = [
    "passing_yards", "passing_tds", "passing_interceptions", "completions", "attempts",
    "passing_first_downs", "passing_2pt_conversions",
    "rushing_yards", "rushing_tds", "carries", "rushing_first_downs", "rushing_2pt_conversions",
    "receiving_yards", "receiving_tds", "receptions", "targets", "receiving_first_downs",
    "receiving_2pt_conversions",
    "fg_made", "fg_att", "pat_made",
    "kickoff_return_yards", "kickoff_return_tds", "kickoff_returns",
    "punt_return_yards", "punt_return_tds", "punt_returns",
    "def_sacks", "def_interceptions", "def_tackles_solo", "def_tackle_assists",
    "def_tackles_for_loss", "def_fumbles_forced", "def_pass_defended", "def_tds",
    "def_safeties", "fum_rec", "sack_fumbles", "fumbles",
]
TEAM_STATS = ["def_sacks", "def_interceptions", "def_tds", "def_safeties", "def_fumbles_forced"]

# hard external ceilings: NO cell value may exceed these (all-season-types tolerant)
CEIL_GAME = {"passing_yards": 554, "rushing_yards": 296, "receiving_yards": 336, "receptions": 21,
             "passing_tds": 7, "rushing_tds": 6, "receiving_tds": 5, "fg_made": 8, "def_sacks": 7.0}
CEIL_SEASON = {"passing_yards": 5477, "passing_tds": 55, "rushing_yards": 2105, "rushing_tds": 28,
               "receiving_yards": 1964, "receiving_tds": 23, "receptions": 149,
               "def_sacks": 23.0,  # Myles Garrett, CLE, 2025 (broke Strahan/Watt 22.5); PBP-corroborated
               "def_interceptions": 14, "fg_made": 44}


def _con():
    c = duckdb.connect(); c.execute("PRAGMA threads=3"); c.execute("SET memory_limit='5GB'")
    c.execute("PRAGMA disable_progress_bar"); c.execute("SET preserve_insertion_order=false")
    sp = Path(latest_v26()).parent / ".gridspill"; sp.mkdir(exist_ok=True)
    c.execute(f"SET temp_directory='{sp.as_posix()}'")
    return c


# individual-player leaders exclude team-DST rows (position='DEF' holds team totals, not a player)
IND = "position <> 'DEF'"


# A leader whose value is 0 is NOT a leader: it means nobody recorded the stat in that cell
# (e.g. individual REG sacks in the 1960s -- sacks were not official until 1982), and the
# "holder" is just whichever id sorts first among thousands of zeros. Such an anchor validates
# nothing and flips on any id/position change, so it is excluded (v > 0, never v = 0). This also
# purged frozen anchors that had leaked a POST value into a REG-filtered cell -- once the REG
# individual pool is genuinely all-zero, there is no leader to freeze.
def _leader(con, V, stat, where, key="NFL_player_id"):
    row = con.execute(f"""SELECT who, ROUND(v,3) AS val, yr FROM (
        SELECT {key} AS who, SUM(CAST({stat} AS DOUBLE)) v, year AS yr FROM {V}
        WHERE {where} AND {IND} AND {stat} IS NOT NULL AND {key} IS NOT NULL GROUP BY {key}, year)
        WHERE v IS NOT NULL AND v > 0 ORDER BY v DESC, who ASC LIMIT 1""").fetchone()
    return None if not row or row[1] is None else [str(row[0]), float(row[1]), int(row[2]) if row[2] else None]


def _career_leader(con, V, stat, key="NFL_player_id"):
    row = con.execute(f"""SELECT {key} AS who, ROUND(SUM(CAST({stat} AS DOUBLE)),3) v FROM {V}
        WHERE season_type='REG' AND {IND} AND {stat} IS NOT NULL AND {key} IS NOT NULL
        GROUP BY {key} HAVING SUM(CAST({stat} AS DOUBLE)) > 0
        ORDER BY v DESC, who ASC LIMIT 1""").fetchone()
    return None if not row or row[1] is None else [str(row[0]), float(row[1])]


def compute() -> dict:
    con = _con(); V = f"read_parquet('{latest_v26()}')"
    grid = {}
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {V}").fetchall()}
    for stat in STATS:
        if stat not in cols:
            continue
        # career (REG)
        cl = _career_leader(con, V, stat)
        if cl:
            grid[f"career|alltime|{stat}"] = cl
        for lo, hi in ERAS:
            base = f"year>={lo} AND year<={hi}"
            reg = f"{base} AND season_type='REG'"
            # single-game REG leader (one row = one game) -- ceiling-checked vs single-game records
            wk = con.execute(f"""SELECT NFL_player_id who, ROUND(CAST({stat} AS DOUBLE),3) v, year FROM {V}
                WHERE {reg} AND {IND} AND {stat} IS NOT NULL AND CAST({stat} AS DOUBLE) > 0
                  AND NFL_player_id IS NOT NULL
                ORDER BY v DESC, who ASC LIMIT 1""").fetchone()
            if wk and wk[1] is not None:
                grid[f"week|{lo}_{hi}|{stat}"] = [str(wk[0]), float(wk[1]), int(wk[2]) if wk[2] else None]
            sl = _leader(con, V, stat, reg)          # REG season record (ceiling-checked)
            if sl:
                grid[f"season|{lo}_{hi}|{stat}"] = sl
            sla = _leader(con, V, stat, base)         # REG+POST combined (combo coverage, not ceilinged)
            if sla:
                grid[f"seasonall|{lo}_{hi}|{stat}"] = sla
    # team-defense (team-season DST leaders): sum DEF rows per (nfl_team, year)
    for stat in TEAM_STATS:
        if stat not in cols:
            continue
        for lo, hi in ERAS:
            row = con.execute(f"""SELECT nfl_team who, ROUND(SUM(CAST({stat} AS DOUBLE)),3) v, year FROM {V}
                WHERE position='DEF' AND year>={lo} AND year<={hi} AND {stat} IS NOT NULL AND nfl_team IS NOT NULL
                GROUP BY nfl_team, year HAVING SUM(CAST({stat} AS DOUBLE)) > 0
                ORDER BY v DESC, who ASC LIMIT 1""").fetchone()
            if row and row[1] is not None:
                grid[f"team_season|{lo}_{hi}|{stat}"] = [str(row[0]), float(row[1]), int(row[2]) if row[2] else None]
    con.close()
    return grid


def _ceiling_violations(grid) -> list:
    bad = []
    for k, v in grid.items():
        grain, era, stat = k.split("|")
        val = v[1]
        if grain == "week" and stat in CEIL_GAME and val > CEIL_GAME[stat] + 0.01:
            bad.append((k, val, CEIL_GAME[stat]))
        if grain == "season" and stat in CEIL_SEASON and val > CEIL_SEASON[stat] + 0.01:
            bad.append((k, val, CEIL_SEASON[stat]))
    return bad


def run(refreeze=False) -> dict:
    grid = compute()
    ceil_bad = _ceiling_violations(grid)
    if refreeze or not FROZEN.exists():
        FROZEN.parent.mkdir(parents=True, exist_ok=True)
        FROZEN.write_text(json.dumps(grid, indent=0, sort_keys=True))
        return {"action": "frozen" if not FROZEN.exists() else "refrozen", "anchors": len(grid),
                "ceiling_violations": ceil_bad, "frozen_path": str(FROZEN), "drift": [], "passed": len(grid),
                "total": len(grid), "failed": len(ceil_bad)}
    frozen = json.loads(FROZEN.read_text())
    drift = []
    for k, fv in frozen.items():
        cur = grid.get(k)
        if cur is None:
            drift.append((k, fv, None)); continue
        # holder must match; value within tiny tolerance (rounding)
        if cur[0] != fv[0] or abs(cur[1] - fv[1]) > 0.011:
            drift.append((k, fv, cur))
    new_keys = [k for k in grid if k not in frozen]
    failed = len(drift) + len(ceil_bad)
    return {"action": "compared", "anchors": len(frozen), "current_cells": len(grid),
            "new_cells": len(new_keys), "drift": drift, "ceiling_violations": ceil_bad,
            "passed": len(frozen) - len(drift), "total": len(frozen), "failed": failed}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--refreeze", action="store_true"); ap.add_argument("--fails", action="store_true")
    a = ap.parse_args()
    r = run(refreeze=a.refreeze)
    if r["action"] in ("frozen", "refrozen"):
        print(f"{r['action'].upper()} {r['anchors']} golden-grid anchors -> {r['frozen_path']}")
        if r["ceiling_violations"]:
            print(f"  CEILING VIOLATIONS ({len(r['ceiling_violations'])}):")
            for k, val, ceil in r["ceiling_violations"]:
                print(f"    {k}: {val} > {ceil}")
    else:
        print(f"golden grid: {r['passed']}/{r['total']} anchors stable | "
              f"{len(r['drift'])} drifted, {r['new_cells']} new cells, {len(r['ceiling_violations'])} ceiling viol")
        for k, fv, cur in r["drift"]:
            print(f"  DRIFT {k}: frozen={fv} now={cur}")
        for k, val, ceil in r["ceiling_violations"]:
            print(f"  CEILING {k}: {val} > {ceil}")
        print(f"\nVERDICT: {'PASS' if r['failed']==0 else 'FAIL'}")
