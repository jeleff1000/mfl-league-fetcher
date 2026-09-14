# -*- coding: utf-8 -*-
"""era_rescue.py -- which columns can be extended back to 1978 (or deeper) with what we hold?

Phase 1 (scan): per-column per-year NONZERO counts over the weekly super table -> the column's
ACTUAL populated era + interior holes.

!! HEURISTIC LIMITATION (learned 2026-07-16 the hard way -- read before trusting `interior_zero_years`):
   an interior zero-year is only EVIDENCE OF A GAP for columns whose event happens most weeks. For RARE
   events the zero years are TRUE. This lane originally flagged `bonus_rush_200yd` as "zero 1964-96 =
   systematic compute bug"; the truth-test (rows WHERE rushing_yards>=200 AND flag=0) found 177 such games
   in all of NFL history and exactly 1 missing flag. Nobody rushed for 200 in those years. ALWAYS truth-test
   an interior hole against the column's own precondition before calling it a defect.
Phase 2 (analyze): achievable floor per column via three mechanisms:
    witness  -- earliest game-grain witness era (from witness_contracts_v2 + reviewed aliases)
    pbp      -- hand-curated PBP-derivation floors (model-free 1978 / official-flag 1999 / charted 2006)
    formula  -- known formula over OUR OWN component columns -> max(actual floor of components)
Ledger = columns where achievable < actual (and interior holes worth filling).
"""
from __future__ import annotations

import glob
import json
import sys
from pathlib import Path

import duckdb

WD = Path("D:/league-history-data/nfl/derived/validation/witness_audit_2026_07_16")
SCAN_OUT = WD / "super_column_era_coverage.json"
LEDGER_OUT = WD / "era_rescue_ledger.json"
CHUNK = 180


def latest_v26() -> str:
    files = sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                   key=lambda p: Path(p).stat().st_mtime, reverse=True)
    return Path(files[0]).as_posix()


def scan():
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA threads=6")
    src = f"read_parquet('{latest_v26()}')"
    desc = con.execute(f"DESCRIBE SELECT * FROM {src}").fetchall()
    numcols = [r[0] for r in desc if any(k in r[1].upper() for k in
               ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "HUGE", "SMALL", "TINY"))
               and r[0] not in ("year", "week")]
    cov: dict[str, dict[int, int]] = {c: {} for c in numcols}
    for i in range(0, len(numcols), CHUNK):
        chunk = numcols[i:i + CHUNK]
        aggs = ", ".join(f'COUNT(*) FILTER (WHERE "{c}" IS NOT NULL AND "{c}" <> 0) AS "{c}"' for c in chunk)
        rows = con.execute(f"SELECT CAST(year AS INT) y, {aggs} FROM {src} GROUP BY 1").fetchall()
        for row in rows:
            y = row[0]
            for c, v in zip(chunk, row[1:]):
                if v:
                    cov[c][y] = int(v)
        print(f"scanned {min(i + CHUNK, len(numcols))}/{len(numcols)} cols", flush=True)
    SCAN_OUT.write_text(json.dumps({c: {str(k): v for k, v in sorted(ys.items())} for c, ys in cov.items()}))
    print(f"wrote {SCAN_OUT}")


# ---------------- phase 2 ----------------
# PBP derivation floors: column -> (floor_year, mechanism note). Curated from pbp_merged's 371-col
# vocabulary + the rollup products. model-free counting = 1978; nflverse official flags = 1999;
# charted air = 2006. Columns already filled 1978+ are still listed -- the ledger simply shows 0 gap.
PBP = {
    "targets": (1978, "receiver_player_id on pass plays (rollup carries targets 1978+); verify incompletion attribution pre-99"),
    "receiving_target_interceptions": (1978, "interception + intended receiver"),
    "def_pass_defended": (1999, "pass_defense_*_player_id (nflverse flag)"),
    "def_qb_hits": (1999, "qb_hit_*_player_id"),
    "def_tackles_for_loss": (1978, "tackled_for_loss flag (backfill twin carries 1978+)"),
    "def_tackles_for_loss_yards": (1978, "TFL + yards_gained"),
    "def_tackles_solo": (1978, "solo_tackle_*_player_id (applied 1978-98 already)"),
    "def_tackle_assists": (1978, "assist_tackle_*_player_id"),
    "def_fumbles_forced": (1978, "forced_fumble_player ids"),
    "fumbles": (1978, "fumble + fumbled_1_player_id (100% attribution)"),
    "fumbles_lost": (1999, "fumble_lost flag; 1978-98 recovery-team lane only ~29% -> NFL.com logs close it"),
    "rushing_fumbles": (1978, "fumble on rush plays"), "receiving_fumbles": (1978, "fumble on catch plays"),
    "sack_fumbles": (1978, "fumble on sacks"),
    "rushing_fumbles_lost": (1999, "split + lost flag"), "receiving_fumbles_lost": (1999, "split + lost flag"),
    "sack_fumbles_lost": (1999, "split + lost flag"),
    "penalties": (1978, "penalty flag + penalty_player_id (PFR pbp text already used 1966+)"),
    "penalty_yards": (1978, "penalty_yards attributed"),
    "rushing_scrambles": (1978, "qb_scramble flag"),
    "timeouts": (1978, "timeout + timeout_team"),
    "passing_2pt_conversions": (1994, "VERIFIED: pbp two_point flags are 1999+ ONLY (zero 1994-98) -> 1994-98 + AFL 1960s via PFR scoring-log text parse"),
    "rushing_2pt_conversions": (1994, "scoring-log text parse for 1994-98/AFL; pbp flags 1999+"),
    "receiving_2pt_conversions": (1994, "scoring-log text parse for 1994-98/AFL; pbp flags 1999+"),
    "completions_40plus": (1978, "complete_pass + yards_gained"), "completions_50plus": (1978, "same"),
    "passing_tds_40plus": (1978, "pass TD + yards"), "passing_tds_50plus": (1978, "same"),
    "receiving_tds_40plus": (1978, "same"), "receiving_tds_50plus": (1978, "same"),
    "rushing_tds_40plus": (1978, "rush TD + yards"), "rushing_tds_50plus": (1978, "same"),
    "rushing_40plus": (1978, "rush >=40yd count"),
    "receptions_0_4": (1978, "completion yardage buckets"), "receptions_5_9": (1978, "same"),
    "receptions_10_19": (1978, "same"), "receptions_20_29": (1978, "same"),
    "receptions_30_39": (1978, "same"), "receptions_40plus": (1978, "same"),
    "passing_long": (1978, "max completion"), "rushing_long": (1978, "max rush"),
    "receiving_long": (1978, "max reception"), "punt_long": (1978, "punt distance"),
    "kickoff_return_long": (1978, "return yards max"), "punt_return_long": (1978, "same"),
    "sack_yards_lost": (1978, "sack yards"), "dropbacks": (1978, "qb_dropback flag"),
    "passing_epa": (1999, "qb_epa (ancient-EPA 1978+ exists but 1993 hole + model quality -> gate)"),
    "rushing_epa": (1999, "epa"), "receiving_epa": (1999, "epa"),
    "passing_wpa": (1999, "wpa"), "rushing_wpa": (1999, "wpa"), "receiving_wpa": (1999, "wpa"),
    "passing_cpoe": (2006, "cp/cpoe model"),
    "passing_air_yards": (2006, "air_yards charted"), "receiving_air_yards": (2006, "same"),
    "passing_completed_air_yards": (2006, "air_yards on completions"),
    "receiving_completed_air_yards": (2006, "same"),
    "receiving_adot": (2006, "air_yards/targets"), "adot": (2006, "same"),
    "pass_explosive_20": (1978, "yardage"), "rush_explosive_10": (1978, "yardage"),
    "rec_explosive_20": (1978, "yardage"),
    "rz_pass_att": (1978, "yardline_100<=20"), "rz_pass_td": (1978, "same"), "rz_carries": (1978, "same"),
    "rz_rush_td": (1978, "same"), "rz_targets": (1978, "same"), "rz_rec_td": (1978, "same"),
    "pass_success": (1978, "down/distance success — VERIFIED basis (down+ydstogo+yards) ~89% of 1978 plays; rollup's 1999 gate was over-conservative"),
    "rush_success": (1978, "same"), "rec_success": (1978, "same"),
    "pass_success_plays": (1978, "same"), "rush_success_plays": (1978, "same"), "rec_success_plays": (1978, "same"),
    "three_out": (1978, "drive reconstruction (applied 1978-97)"),
    "fourth_down_stop": (1978, "same"),
    "special_teams_tackles_solo": (1978, "ST tackle ids"),
    "misc_yards": (1978, "misc return yardage plays"),
    "pick6": (1978, "interception + return TD by passer"),
    "def_sacks": (1978, "sack_player_id + half sacks (applied 1978-81)"),
    "def_safeties": (1978, "safety_player_id"),
    "def_interceptions": (1978, "interception_player_id"),
    "def_interception_yards": (1978, "int return yards"),
    "def_int_ret_td": (1978, "int return TD"),
    "fum_rec": (1978, "fumble_recovery_1_player_id"), "fum_rec_yds": (1978, "recovery yards"),
    "fum_ret_td": (1978, "recovery TD"),
    "fumble_recovery_own": (1978, "recovery same-team"),
    "fumble_recovery_yards_own": (1978, "same"), "fumble_recovery_yards_opp": (1978, "same"),
    "def_blk_kick": (1978, "blocked_player_id"), "punts_blocked": (1978, "punt blocked flag"),
    "fg_blocked": (1978, "fg blocked flag"), "pat_blocked": (1978, "xp blocked flag"),
    "gwfg_att": (1978, "scoring-log/pbp game-winning FG context (PFR scoring log reaches 1920s)"),
    "kickoff_returns": (1978, "kickoff_returner ids"), "kickoff_return_yards": (1978, "same"),
    "punt_returns": (1978, "punt_returner ids"), "punt_return_yards": (1978, "same"),
    "total_return_yards": (1978, "returns"), "dst_return_yards": (1978, "returns"),
}

# Known formulas over OUR OWN columns: col -> (components, note). Achievable floor =
# max(actual floor of components) -- and components themselves may be rescued first.
FORMULA = {
    "passer_rating": (["completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions"],
                      "NFL rating formula, retro-computable (PFR shows it from 1932)"),
    "rate": (["completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions"], "dup of passer_rating"),
    "completion_pct": (["completions", "attempts"], ""),
    "passing_yards_per_attempt": (["passing_yards", "attempts"], ""),
    "passing_adjusted_yards_per_attempt": (["passing_yards", "passing_tds", "passing_interceptions", "attempts"], "AY/A"),
    "passing_net_yards_per_attempt": (["passing_yards", "sack_yards_lost", "attempts", "sacks_suffered"], "NY/A"),
    "passing_adjusted_net_yards_per_attempt": (["passing_yards", "passing_tds", "passing_interceptions", "sack_yards_lost", "attempts", "sacks_suffered"], "ANY/A"),
    "passing_td_pct": (["passing_tds", "attempts"], ""),
    "passing_int_pct": (["passing_interceptions", "attempts"], ""),
    "receiving_yards_per_reception": (["receiving_yards", "receptions"], ""),
    "catch_rate": (["receptions", "targets"], ""),
    "yards_per_touch": (["rushing_yards", "receiving_yards", "carries", "receptions"], ""),
    "punt_yards_per_punt": (["punt_yards", "punts"], ""),
    "touches": (["carries", "receptions"], ""),
    "total_touches": (["carries", "receptions"], ""),
    "opportunities": (["carries", "targets"], ""),
    "turnovers": (["passing_interceptions", "fumbles_lost"], ""),
    "scrimmage_yards": (["rushing_yards", "receiving_yards"], ""),
    "yds_from_scrimmage": (["rushing_yards", "receiving_yards"], ""),
    "scrimmage_tds": (["rushing_tds", "receiving_tds"], ""),
    "rush_receive_td": (["rushing_tds", "receiving_tds"], ""),
    "total_tds_scored": (["rushing_tds", "receiving_tds", "kickoff_return_tds", "punt_return_tds", "fum_ret_td", "def_int_ret_td"], ""),
    "total_tds_accounted_for": (["rushing_tds", "receiving_tds", "kickoff_return_tds", "punt_return_tds", "fum_ret_td", "def_int_ret_td"], ""),
    "all_purpose_yards": (["rushing_yards", "receiving_yards", "kickoff_return_yards", "punt_return_yards", "def_interception_yards", "fum_rec_yds"], ""),
    "total_points_scored": (["rushing_tds", "receiving_tds", "fg_made", "pat_made", "def_safeties"], ""),
    "total_return_yards": (["kickoff_return_yards", "punt_return_yards"], ""),
    "def_tackles_combined": (["def_tackles_solo", "def_tackle_assists"], ""),
    "fg_missed": (["fg_att", "fg_made"], ""), "pat_missed": (["pat_att", "pat_made"], ""),
    "fg%": (["fg_made", "fg_att"], ""), "xp%": (["pat_made", "pat_att"], ""), "pat_pct": (["pat_made", "pat_att"], ""),
    "game_margin": (["team_points", "opponent_points"], ""), "is_win": (["team_points", "opponent_points"], ""),
    "pacr": (["passing_yards", "passing_air_yards"], "air-limited"),
    "racr": (["receiving_yards", "receiving_air_yards"], "air-limited"),
    "wopr": (["targets", "receiving_air_yards"], "air-limited; target-share part reaches targets floor"),
    "target_share": (["targets"], "team-week normalization"),
    "bonus_pass_300yd": (["passing_yards"], ""), "bonus_pass_400yd": (["passing_yards"], ""),
    "bonus_pass_25cmp": (["completions"], ""), "bonus_rush_100yd": (["rushing_yards"], ""),
    "bonus_rush_200yd": (["rushing_yards"], ""), "bonus_rush_20att": (["carries"], ""),
    "bonus_rec_100yd": (["receiving_yards"], ""), "bonus_rec_200yd": (["receiving_yards"], ""),
    "bonus_rec_10rec": (["receptions"], ""),
    "bonus_rush_rec_100yd": (["rushing_yards", "receiving_yards"], ""),
    "bonus_rush_rec_200yd": (["rushing_yards", "receiving_yards"], ""),
    "weighted_opportunities": (["carries", "targets"], ""),
}

SKIP_FAMILIES_PREFIX = ("rank_", "lamar_", "ppg_", "rolling_", "weighted_ppg_", "consistency_",
                        "avg_pts_next_year", "fpts_", "pts_")


def analyze(min_rows=3, solid_rows=50):
    cov = {c: {int(y): n for y, n in ys.items()} for c, ys in json.loads(SCAN_OUT.read_text()).items()}
    v2 = json.loads((WD / "witness_contracts_v2.json").read_text()) if (WD / "witness_contracts_v2.json").exists() \
        else json.loads(Path(r"C:\Users\joeye\AppData\Local\Temp\claude\d--yahoo-oauth\a97dc4c4-29ee-4deb-b5fb-17b48c1c30f2\scratchpad\witness_contracts_v2.json").read_text())
    mm = json.loads((WD / "master_witness_matrix.json").read_text())

    # witness game-grain floors per column via the matrix's direct hits
    wk = mm["tables"]["weekly"]

    def floor_of(col):
        ys = sorted(y for y, n in cov.get(col, {}).items() if n >= min_rows)
        return ys[0] if ys else None

    def solid_floor_of(col):
        ys = sorted(y for y, n in cov.get(col, {}).items() if n >= solid_rows)
        return ys[0] if ys else None

    ledger = []
    for col, ys in cov.items():
        if col.startswith(SKIP_FAMILIES_PREFIX):
            continue
        actual = floor_of(col)
        if actual is None:
            continue
        mechs = []
        r = wk.get(col, {})
        ge = r.get("game_era")
        if ge:
            mechs.append(("witness", ge, ",".join(r.get("witnesses", [])[:3])))
        if col in PBP:
            mechs.append(("pbp", PBP[col][0], PBP[col][1]))
        if col in FORMULA:
            comps, note = FORMULA[col]
            comp_floors = [floor_of(c) for c in comps]
            if all(f is not None for f in comp_floors):
                mechs.append(("formula", max(comp_floors), f"= f({', '.join(comps)}) {note}".strip()))
        if not mechs:
            continue
        best = min(mechs, key=lambda m: m[1])
        gap = actual - best[1]
        # interior holes: zero years strictly inside the populated span
        span = sorted(y for y, n in ys.items() if n >= min_rows)
        holes = []
        if span:
            inside = set(range(span[0], span[-1] + 1))
            holes = sorted(y for y in inside if cov[col].get(y, 0) < min_rows)
        # sparse early era: years where count < 10% of the column's median populated count
        med = sorted(n for n in ys.values())[len(ys) // 2] if ys else 0
        sparse = sorted(y for y, n in ys.items() if span and y <= span[0] + 15 and n < med * 0.1)
        if gap > 0 or len(holes) >= 2:
            ledger.append({"col": col, "actual_floor": actual, "solid_floor": solid_floor_of(col),
                           "achievable_floor": best[1], "gap_years": max(gap, 0),
                           "mechanism": best[0], "how": best[2],
                           "all_mechanisms": [{"m": m[0], "floor": m[1], "how": m[2]} for m in mechs],
                           "interior_zero_years": holes[:25], "n_holes": len(holes),
                           "sparse_early_years": sparse[:12]})
    ledger.sort(key=lambda r: (-r["gap_years"], -r["n_holes"]))
    LEDGER_OUT.write_text(json.dumps(ledger, indent=1))
    print(f"{len(ledger)} rescue/hole rows -> {LEDGER_OUT}\n")
    print(f"{'column':34}{'actual':>7}{'solid':>7}{'achv':>6}{'gap':>5}  mech    how")
    print("-" * 130)
    for r in ledger[:60]:
        print(f"{r['col']:34}{r['actual_floor']:>7}{str(r['solid_floor'] or '-'):>7}{r['achievable_floor']:>6}"
              f"{r['gap_years']:>5}  {r['mechanism']:6}  {r['how'][:70]}")
    nh = [r for r in ledger if r["n_holes"] >= 2]
    print(f"\ncolumns with interior zero-year holes: {len(nh)}")
    for r in nh[:25]:
        print(f"   {r['col']:34} holes({r['n_holes']}): {r['interior_zero_years'][:12]}")


if __name__ == "__main__":
    if sys.argv[1:] and sys.argv[1] == "analyze":
        analyze()
    else:
        scan()
        analyze()
