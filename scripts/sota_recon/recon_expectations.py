"""
sota_recon/recon_expectations.py  --  the expectations engine ("no exceptions")

The finish-line artifact. For every NON-DERIVED stat we know (from the source registry /
data catalog) which source SHOULD fill it, at which grain, in which eras. This lane turns
that knowledge into expectations and logs EVERY violation, so "where are our errors, for
every column and every row" has one complete, machine-checkable answer.

Families of expectation (each emits offending rows/keys to one discrepancy log):

  IDENTITY   one player == one identity. No (franchise, year, week, name) under >1 id
             (the dup-person bug); no (NFL_player_id, year, week, season_type) twice
             EXCEPT genuine 1920s-40s doubleheaders (two games, different opponents).
  BOUNDS     no per-game value beyond the physical ceiling (typos, unit errors).
  COVERAGE   within the relevant position group (only QBs throw, only Ks kick), a column an
             authoritative source covers must be populated in that source's eras.

Value-level corroboration vs independent sources (PBP per-stat, record-book season totals,
scoreboard) is owned by the oracle / season_authority / scoring / team_total lanes (run_all);
this lane adds identity, bounds and position-aware coverage so the union covers every
non-derived column at every level. Runs in DuckDB (streams from parquet) to stay memory-safe.

    python -m scripts.sota_recon.recon_expectations
      -> derived/validation/expectations/{EXPECTATIONS.md, discrepancies.parquet}
"""

from __future__ import annotations

import os

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .sources import latest_v26

OUT_DIR = "D:/league-history-data/nfl/derived/validation/expectations"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"

BOUNDS = {
    "passing_yards": 600, "rushing_yards": 320, "receiving_yards": 360,
    "passing_tds": 8, "rushing_tds": 6, "receiving_tds": 5, "passing_interceptions": 8,
    "completions": 55, "attempts": 75, "carries": 50, "receptions": 25, "targets": 30,
    "fg_made": 9, "fg_att": 12, "pat_made": 12, "fumbles": 6,
}
_PASS, _RUSH = ("QB",), ("QB", "RB", "FB", "HB")
_RECV, _K = ("WR", "TE", "RB", "FB", "HB"), ("K",)
COVERAGE_FROM = {
    "passing_yards": (1932, _PASS), "completions": (1932, _PASS), "attempts": (1932, _PASS),
    "passing_tds": (1932, _PASS),
    # Interceptions THROWN start 1933, not 1932. The other passing columns carry 1932 data
    # (sparse: 14 QBs with attempts, 15 passing TDs) but INTs-thrown were not compiled until
    # 1933 -- 1930/31/32 each show 0 INTs while 14-17 QBs are actively throwing, and 1933
    # opens clean at 194 INTs across 78 QBs (the year forward passing from anywhere behind the
    # line was legalised and passing stats became systematic). The inherited 1932 floor was
    # copied from the sibling passing columns without checking interceptions specifically;
    # extending coverage and letting the gate assert is what surfaced it.
    "passing_interceptions": (1933, _PASS),
    "rushing_yards": (1932, _RUSH), "carries": (1932, _RUSH),
    "receiving_yards": (1932, _RECV), "receptions": (1932, _RECV), "targets": (1978, _RECV),
    "fg_made": (1932, _K), "pat_made": (1932, _K),
    # Success-rate counts. The merged PBP carries a play-level `success` for 1978-2025
    # (2,092,435 of 2,107,143 plays; 40,006/40,006 in 1978), so 1978 is the true floor for
    # every one of these. They shipped populated only from 1999 -- 0 non-null across all
    # 601,842 pre-1999 rows -- because the ROLLUP stopped at the cutover, not because the
    # source stops. Registered here so the EMPTY_ERA gate below owns that truncation.
    "pass_success": (1978, _PASS), "pass_success_plays": (1978, _PASS),
    "rush_success": (1978, _RUSH), "rush_success_plays": (1978, _RUSH),
    "rec_success": (1978, _RECV), "rec_success_plays": (1978, _RECV),

    # --- Bridged from docs/advanced-stats-registry.json ---------------------------------
    # Floors are SOURCED, not guessed: each is the registry's own adjudicated era for that
    # stat, restricted to entries whose status carries evidence (by_construction/validated).
    # A guessed floor that fails for the wrong reason is worse than no floor, so every one
    # below also had to clear two data checks before being written here:
    #   1. the position group holds >=80% of the column's populated mass (else the grouping
    #      is wrong and the gate would fire on the group, not on the coverage)
    #   2. observed data starts at or before the floor (a floor already violated on arrival
    #      means the floor, the source, or the rollup is wrong -- a finding, not a registration)
    # 6 further registry columns (pacr, scrimmage_yards, total_epa/wpa/tds/touches) are
    # deliberately NOT here: they span positions and no group can be inferred safely.
    "ngs_avg_separation": (2016, _RECV), "ngs_rush_yards_over_expected": (2018, _RUSH),
    "pass_explosive_20": (1978, _PASS), "rush_explosive_10": (1978, _RUSH),
    "rec_explosive_20": (1978, _RECV),
    "passing_2pt_conversions": (1999, _PASS), "rushing_2pt_conversions": (1999, _RUSH),
    "receiving_2pt_conversions": (1999, _RECV),
    "passing_air_yards": (2006, _PASS), "receiving_air_yards": (2006, _RECV),
    "passing_cpoe": (2006, _PASS), "racr": (2006, _RECV),
    # EPA back to 1978 (wave67): the merged PBP carries play-level `epa` for 1978-2025 and these
    # are SUM(epa) rollups. rushing/receiving reproduce the stored 1999+ value at 100%; passing is
    # raw-epa pre-1999 (qb_epa, which the stored 1999+ used, is absent before 1999) -- see wave67.
    "passing_epa": (1978, _PASS), "rushing_epa": (1978, _RUSH), "receiving_epa": (1978, _RECV),
    "passing_wpa": (1999, _PASS), "rushing_wpa": (1999, _RUSH), "receiving_wpa": (1999, _RECV),
    "rz_pass_att": (1978, _PASS), "rz_pass_td": (1978, _PASS),
    "rz_carries": (1978, _RUSH), "rz_rush_td": (1978, _RUSH),
    "rz_targets": (1978, _RECV), "rz_rec_td": (1978, _RECV),
}

# Years where a registered column is empty because the SOURCE has no evidence, not because a
# rollup stalled. EMPTY_YEAR cannot tell those apart -- both look like an empty covered year --
# so the distinction has to be declared, with a measurement behind it.
#
# This is NOT a way to quiet the gate. An exempted year is still emitted, as EVIDENCE_GAP at
# low severity, so it stays on the report and stays countable. What the exemption buys is that
# a KNOWN, sourced, tracked gap cannot drown out an UNKNOWN one -- which is the failure mode a
# permanently-red gate actually has.
#
# Every entry must name the measurement and the condition for its own deletion.
EVIDENCE_GAPS = {
    1993: (
        ("pass_success", "pass_success_plays", "rush_success", "rush_success_plays",
         "rec_success", "rec_success_plays",
         "rushing_epa", "receiving_epa", "passing_epa"),
        "Stathead returned no exp_pts_diff for 1993: 39,029 plays, 0 with epa -- the only such "
        "season in 1978-2025 (epa is otherwise unbroken from 1978). success is DEFINED as "
        "epa > 0, and the EPA rollups are SUM(epa), so both are unknowable, not zero, for 1993. "
        "The twin parser's zero-default had been shipping success as 0.0 on every play; wave64 "
        "retracted that to NULL, and the wave67 EPA backfill leaves 1993 NULL (no epa source). "
        "DELETE THIS ENTRY once Stathead is re-scraped for 1993 exp_pts_diff.",
    ),
}


def _evidence_gap(col: str, year: int) -> str | None:
    """Sourced reason this column is legitimately empty this year, or None."""
    entry = EVIDENCE_GAPS.get(year)
    return entry[1] if entry and col in entry[0] else None


_NN = "lower(regexp_replace(player, '[^A-Za-z]', '', 'g'))"


def _con(v26):
    c = duckdb.connect()
    c.execute("PRAGMA threads=2"); c.execute("PRAGMA disable_progress_bar")
    c.execute("SET memory_limit='3GB'")
    return c


def run() -> dict:
    v26 = latest_v26()
    have = set(pq.read_schema(v26).names)
    con = _con(v26)
    V = f"read_parquet('{v26}')"
    disc = []

    # --- IDENTITY: one person per (franchise, year, week, name, POSITION) ---
    # NOTE: the NFL genuinely has different players with the SAME name on the SAME roster,
    # distinguished by position (e.g. an OL "Chris Smith" and a DL "Chris Smith" in 2024).
    # So a real split/phantom requires SAME position too; differing positions = real people.
    rows = con.execute(f"""
        SELECT nfl_franchise_number fr, year, week, {_NN} nn, position pos
        FROM {V} WHERE player IS NOT NULL AND player <> '' AND position IS NOT NULL
        GROUP BY 1,2,3,4,5 HAVING COUNT(DISTINCT NFL_player_id) > 1
    """).fetchall()
    for fr, yr, wk, nn, pos in rows:
        disc.append(("identity.dup_person", "player-week",
                     f"fr{fr}|{int(yr)}|wk{int(wk)}|{nn}|{pos}", "NFL_player_id",
                     "1 id", ">1 id", "high"))

    # --- IDENTITY: one row per GAME. Legitimate exception = 1920s-40s DOUBLEHEADERS (one row
    # per distinct opponent; a stray NULL-opponent week-aggregate row counts as its own "game"
    # so a real doubleheader is not flagged). A true bug = MORE rows than distinct games (same
    # opponent twice), or ANY multi-row player-week in the modern era (year>=1950, no doubleheaders). ---
    opp = "opponent_nfl_franchise_number" if "opponent_nfl_franchise_number" in have else "NULL"
    rows = con.execute(f"""
        SELECT NFL_player_id, year, week, season_type, COUNT(*) n
        FROM {V} GROUP BY 1,2,3,4
        HAVING COUNT(*) > 1
           AND (year >= 1950
                OR COUNT(*) > COUNT(DISTINCT COALESCE(CAST({opp} AS VARCHAR), '~null')))
    """).fetchall()
    for pid, yr, wk, st, n in rows:
        disc.append(("identity.dup_player_week", "player-week",
                     f"{pid}|{int(yr)}|wk{int(wk)}|{st}", "row_count", "1", str(int(n)), "high"))

    # --- IDENTITY: a synthetic id must not co-occur with a REAL id for the same player
    # (franchise, year, week, name) -- that's a same-person split (the root cause of dup rows) ---
    SYN = ("(NFL_player_id LIKE 'HIST-%' OR NFL_player_id LIKE 'SYN-%' "
           "OR regexp_matches(NFL_player_id,'^[A-Z]{2,4}[0-9]{5,7}$'))")
    REALID = ("(regexp_matches(NFL_player_id,'^00-[0-9]+$') "
              "OR regexp_matches(NFL_player_id,'^[A-Za-z][A-Za-z.''-]*[0-9]{2}$'))")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE _r2 AS
        SELECT NFL_player_id id, nfl_franchise_number fr, year, week, {_NN} nn,
               {SYN} s, {REALID} r FROM {V} WHERE player IS NOT NULL""")
    syn_dups = con.execute("""SELECT DISTINCT a.id, a.fr, a.year, a.week, a.nn FROM _r2 a
        WHERE a.s AND EXISTS (SELECT 1 FROM _r2 b WHERE b.r AND b.fr=a.fr AND b.year=a.year
              AND b.week=a.week AND b.nn=a.nn)""").fetchall()
    for sid, fr, yr, wk, nn in syn_dups:
        disc.append(("identity.synthetic_id_split", "player-week",
                     f"{sid}|fr{fr}|{int(yr)}|wk{int(wk)}|{nn}", "NFL_player_id",
                     "real id only", "synthetic+real split", "high"))

    # --- IDENTITY: every v26 PLAYER id must exist in player_bio (the authority). Team-defense
    # rows (DEF-*/DST*) are intentional team identifiers, not players, so they are exempt. ---
    if os.path.isfile(BIO):
        rows = con.execute(f"""
            SELECT DISTINCT NFL_player_id FROM {V} v
            WHERE NFL_player_id IS NOT NULL
              AND NFL_player_id NOT LIKE 'DEF-%' AND NFL_player_id NOT LIKE 'DST%'
              AND NFL_player_id NOT IN (SELECT NFL_player_id FROM read_parquet('{BIO}'))
        """).fetchall()
        for (pid,) in rows:
            disc.append(("identity.orphan_id_not_in_bio", "player", str(pid),
                         "NFL_player_id", "in player_bio", "absent", "medium"))

    # --- IDENTITY: franchise/team lineage consistency. The franchise NUMBER is the stable
    # lineage; within a season each franchise must have ONE abbrev, the team-view and
    # opponent-view abbrev must agree, and the DEF team id must be the uniform DEF-<number>
    # scheme. (Cross-season abbrev change is allowed.) ---
    if "nfl_franchise_number" in have and "opponent_nfl_franchise_number" in have:
        for side, frcol, abcol, tag in [
            ("team", "nfl_franchise_number", "nfl_team", "within_team"),
            ("opp", "opponent_nfl_franchise_number", "opponent_nfl_team", "within_opp")]:
            for r in con.execute(f"""SELECT year, {frcol} fr FROM {V}
                WHERE {frcol} IS NOT NULL AND {abcol} IS NOT NULL
                GROUP BY 1,2 HAVING COUNT(DISTINCT {abcol})>1""").fetchall():
                disc.append(("identity.franchise_abbrev_inconsistent", "franchise-season",
                             f"{int(r[0])}|fr{r[1]}|{tag}", abcol, "1 abbrev", ">1", "high"))
        for r in con.execute(f"""WITH t AS (SELECT DISTINCT year,nfl_franchise_number fr,nfl_team ab FROM {V}
                WHERE nfl_franchise_number IS NOT NULL AND nfl_team IS NOT NULL),
              o AS (SELECT DISTINCT year,opponent_nfl_franchise_number fr,opponent_nfl_team ab FROM {V}
                WHERE opponent_nfl_franchise_number IS NOT NULL AND opponent_nfl_team IS NOT NULL)
            SELECT t.year,t.fr FROM t JOIN o ON t.year=o.year AND t.fr=o.fr WHERE t.ab<>o.ab""").fetchall():
            disc.append(("identity.franchise_team_vs_opp_abbrev", "franchise-season",
                         f"{int(r[0])}|fr{r[1]}", "nfl_team", "team==opp abbrev", "differ", "high"))
        for (pid,) in con.execute(f"""SELECT DISTINCT NFL_player_id FROM {V}
            WHERE NFL_player_id LIKE 'DEF-%' AND NOT regexp_matches(NFL_player_id,'^DEF-[0-9]+$')""").fetchall():
            disc.append(("identity.def_id_nonuniform", "team", str(pid), "NFL_player_id",
                         "DEF-<number>", "DEF-<abbrev>", "medium"))

    # --- LINEAGE: every franchise has a flowing inception->dissolution/modern history, or its
    # internal gaps are EXPLICITLY KNOWN. The franchise NUMBER is the lineage anchor (relocations
    # keep the same number). 32 franchises are active modern-day; the rest are real dissolutions
    # (no unrecorded rebrand continued under a different number). Known gaps (year set per
    # franchise) -- anything outside these is flagged. ---
    KNOWN_FRANCHISE_GAPS = {
        23: set(range(1996, 1999)),              # Cleveland Browns suspension 1996-98
        24: {1943, 1944},                        # Steelers WWII: Steagles '43, Card-Pitt '44
        14: {1943},                              # Rams suspended for WWII 1943
        4:  {1930, 1931},                        # Boston predecessor (Bulldogs '29) before Braves '32
        2:  {1922, 1923, 1924},                  # Brickley "Giants" 1921 before Mara Giants 1925
        103: {1924}, 139: {1928}, 123: {1925}, 119: {1924, 1925},
        147: set(range(1925, 1929)), 144: {1922, 1923, 1924, 1927},
        143: {1922, 1923, 1926, 1928, 1929, 1930},
        142: set(range(1922, 1933)), 150: set(range(1924, 1934)),
        148: set(range(1930, 1944)), 114: set(range(1929, 1946)),
    }
    if "nfl_franchise_number" in have:
        fy = con.execute(f"""SELECT nfl_franchise_number fr, list(DISTINCT CAST(year AS INT)) yrs
            FROM {V} WHERE nfl_franchise_number IS NOT NULL AND year IS NOT NULL GROUP BY 1""").fetchall()
        for fr, yrs in fy:
            ys = set(int(y) for y in yrs)
            gaps = set(range(min(ys), max(ys) + 1)) - ys
            unknown = gaps - KNOWN_FRANCHISE_GAPS.get(int(fr), set())
            if unknown:
                disc.append(("lineage.unexpected_franchise_gap", "franchise", f"fr{int(fr)}",
                             "continuous_or_known_gap", str(sorted(unknown)), "high"))

    # --- BOUNDS: per-game physical ceilings (offensive/kicking positions) ---
    for col, mx in BOUNDS.items():
        if col not in have:
            continue
        rows = con.execute(f"""
            SELECT NFL_player_id, year, week, {col} FROM {V}
            WHERE position IN ('QB','RB','WR','TE','K','FB','HB') AND {col} > {mx}
        """).fetchall()
        for pid, yr, wk, val in rows:
            disc.append(("bounds.exceeds_ceiling", "player-game",
                         f"{pid}|{int(yr)}|wk{int(wk)}", col, f"<= {mx}", str(val), "high"))

    # --- COVERAGE within relevant position group ---
    coverage = []
    for col, (since, posset) in COVERAGE_FROM.items():
        if col not in have:
            continue
        pin = ",".join(f"'{p}'" for p in posset)
        for lo, hi in [(since, 1949), (1950, 1977), (1978, 2009), (2010, 2025)]:
            if hi < since:
                continue
            r = con.execute(f"""
                SELECT COUNT(*) n, AVG(CASE WHEN COALESCE({col},0) <> 0 THEN 1.0 ELSE 0 END) rate
                FROM {V} WHERE position IN ({pin}) AND year BETWEEN {lo} AND {hi}
            """).fetchone()
            if r and r[0]:
                rate = round(100 * r[1], 1)
                coverage.append((col, f"{lo}-{hi}", rate, int(r[0])))
                # EMPTY_ERA: a column whose source demonstrably covers this era is TOTALLY
                # empty in it. Until now COVERAGE only *reported* the rate into the markdown
                # and never appended to `disc`, so it observed without asserting -- which is
                # why the success-rate rollup could stop at 1999 and stay silent for a year.
                #
                # The predicate is ZERO, deliberately, not "low". Plenty of registered
                # columns are legitimately sparse for a position group in an era (RB targets,
                # backup QB attempts), and a percentage floor would drown this in false
                # positives. Nothing populated AT ALL across a whole era, for a source that
                # covers it, is unambiguous: the rollup never ran.
                if r[1] == 0 and lo >= since:
                    disc.append(("EMPTY_ERA", "column", f"{col}|{lo}-{hi}", col,
                                 f"source covers from {since}; era must not be empty",
                                 f"0 of {int(r[0])} rows populated", "high"))

        # Per-YEAR sweep. The bucket check above cannot see a truncation that starts mid-bucket:
        # the success columns are empty 1978-1998 and populated 1999+, and the (1978, 2009)
        # bucket straddles that cliff, so its rate is non-zero and the bucket gate stays silent.
        # A whole EMPTY YEAR, in a span the source covers, is the honest unit of detection --
        # it is what a stalled rollup or a truncated backfill actually looks like.
        empty_years = con.execute(f"""
            SELECT CAST(year AS INT) y, COUNT(*) n
            FROM {V} WHERE position IN ({pin}) AND year >= {since}
            GROUP BY 1 HAVING SUM(CASE WHEN COALESCE({col},0) <> 0 THEN 1 ELSE 0 END) = 0
            ORDER BY 1
        """).fetchall()
        # Split the empty years into "the source had nothing to give" (declared, sourced, and
        # reported at low severity) and everything else, which is the real signal.
        ys, exempt = [], []
        for y, _n in empty_years:
            y = int(y)
            reason = _evidence_gap(col, y)
            (exempt if reason else ys).append((y, reason))
        for y, reason in exempt:
            disc.append(("EVIDENCE_GAP", "column", f"{col}|{y}", col,
                         f"source has no evidence for {y}", reason, "low"))
        if ys:
            yy = [y for y, _ in ys]
            disc.append(("EMPTY_YEAR", "column", f"{col}|{yy[0]}-{yy[-1]}", col,
                         f"source covers from {since}; no covered year may be empty",
                         f"{len(yy)} empty years: {yy[0]}-{yy[-1]}", "high"))
    con.close()

    dd = pd.DataFrame(disc, columns=["check", "level", "key", "column",
                                     "expected", "actual", "severity"])
    os.makedirs(OUT_DIR, exist_ok=True)
    pq.write_table(pa.Table.from_pandas(dd), os.path.join(OUT_DIR, "discrepancies.parquet"))
    by_check = dd.check.value_counts().to_dict()
    _write_md(by_check, coverage)
    return {"total_discrepancies": int(len(dd)), "by_check": by_check,
            "out": os.path.join(OUT_DIR, "EXPECTATIONS.md")}


def _write_md(by_check, coverage):
    L = ["# v26 Expectations Report", "",
         "One discrepancy log: `discrepancies.parquet`. Value-level corroboration is in the "
         "oracle / season_authority / scoring / team_total lanes (run_all).", "",
         "## Violations by check", ""]
    if not by_check:
        L.append("_No identity/bounds violations._")
    for c, n in sorted(by_check.items(), key=lambda x: -x[1]):
        L.append(f"- **{c}**: {n:,}")
    L += ["", "## Coverage within relevant position group (non-zero %)", "",
          "| column | era | non-zero % | group rows |", "|---|---|---|---|"]
    for col, era, pct, n in coverage:
        flag = "  ⚠️ LOW" if pct < 50 and n > 200 else ""
        L.append(f"| {col} | {era} | {pct}% | {n:,}{flag} |")
    with open(os.path.join(OUT_DIR, "EXPECTATIONS.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(L))


if __name__ == "__main__":
    r = run()
    print(f"total discrepancies: {r['total_discrepancies']:,}")
    for c, n in sorted(r["by_check"].items(), key=lambda x: -x[1]):
        print(f"  {c}: {n:,}")
    print(f"\nwrote {r['out']}")
