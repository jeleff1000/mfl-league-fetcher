"""
sota_recon/analyze_adjudication.py  --  root-cause grouping of the adjudicated disputes.

Consumes dispute_adjudication CSV (from adjudicate_disputes) and answers Joe's questions:

  1. WHY do MISSING_WEEKs occur?  grouped by era / week-number / doubleheader overlap /
     whole-game-absent vs player-absent.
  2. Do MISSING and EXTRA weeks MATCH each other?  same player-season, |extra value| ==
     |missing witness value| => WEEK_SHIFT (one game under the wrong week label; the fix
     is a re-label, NOT a fill + delete -- promoting "the correct value" means promoting
     the correct WEEK too).
  3. WEEK_VALUE_WRONG patterns: delta distribution (rounding/lateral-sized vs large),
     and swaps (our value equals the witness value of a DIFFERENT week).
  4. WITNESS_CONFLICT: which lineage do the season PAGES side with (tie-breaker signal)?
  5. PAGES_QUIRK: how big are the season-page deltas, and do they cluster by era?

READ-ONLY analysis; output feeds correction-wave design, nothing is applied.

    python -m scripts.sota_recon.analyze_adjudication [--csv <adjudication csv>]
"""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict

import duckdb

DEFAULT_CSV = (r"D:/league-history-data/nfl/derived/validation/sota_recon_master"
               r"/phase1_sweeps/dispute_adjudication_20260710.csv")


def _era(y: int) -> str:
    for lo, hi in ((1920, 1949), (1950, 1956), (1957, 1977), (1978, 1998), (1999, 2015),
                   (2016, 2025)):
        if lo <= y <= hi:
            return f"{lo}-{hi}"
    return "?"


def run(csv: str = DEFAULT_CSV) -> None:
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT stat, pfr_id, CAST(year AS INT), season_verdict, TRY_CAST(week AS INT),
               kind, TRY_CAST(ours AS DOUBLE), TRY_CAST(box AS DOUBLE), TRY_CAST(pbp AS DOUBLE)
        FROM '{csv}'""").fetchall()
    con.close()

    by_atom: dict[tuple, list] = defaultdict(list)
    for stat, pid, yr, sv, wk, kind, ours, box, pbp in rows:
        by_atom[(stat, pid, yr, sv)].append((wk, kind, ours, box, pbp))

    # ---- 2. missing<->extra matching (week shifts) --------------------------------------
    shifts, pure_missing, pure_extra = [], Counter(), Counter()
    missing_by_week, missing_examples = Counter(), []
    for (stat, pid, yr, sv), items in by_atom.items():
        miss = [(wk, box if box is not None else pbp) for wk, k, o, box, pbp in items
                if k == "MISSING_WEEK"]
        extra = [(wk, o) for wk, k, o, box, pbp in items if k == "EXTRA_WEEK"]
        used = set()
        for mwk, mval in miss:
            match = next((i for i, (ewk, ev) in enumerate(extra)
                          if i not in used and mval is not None and ev is not None
                          and abs(ev - mval) <= 1.5), None)
            if match is not None:
                used.add(match)
                shifts.append((stat, pid, yr, extra[match][0], mwk, mval))
            else:
                pure_missing[(stat, _era(yr))] += 1
                missing_by_week[mwk] += 1
                if len(missing_examples) < 8:
                    missing_examples.append((stat, pid, yr, mwk, mval))
        for i, (ewk, ev) in enumerate(extra):
            if i not in used:
                pure_extra[(stat, _era(yr))] += 1

    print(f"== 2. WEEK SHIFTS (extra week == missing week's value): {len(shifts)} pairs ==")
    for s in shifts[:10]:
        print(f"   {s[0]} {s[1]} {s[2]}: our wk{s[3]} should be wk{s[4]} (val {s[5]})")
    print(f"   pure MISSING (no matching extra): {sum(pure_missing.values())}")
    for k, n in pure_missing.most_common(8):
        print(f"     {k[0]:20s} {k[1]:10s} {n}")
    print(f"   pure EXTRA (no matching missing): {sum(pure_extra.values())}")
    for k, n in pure_extra.most_common(5):
        print(f"     {k[0]:20s} {k[1]:10s} {n}")
    print(f"   missing-week week-number histogram (top): {missing_by_week.most_common(8)}")
    print(f"   sample pure-missing: {missing_examples}")

    # ---- 3. WEEK_VALUE_WRONG patterns ---------------------------------------------------
    deltas, swaps, wvw = [], 0, 0
    for (stat, pid, yr, sv), items in by_atom.items():
        week_wit = {wk: (box if box is not None else pbp) for wk, k, o, box, pbp in items}
        for wk, k, o, box, pbp in items:
            if k != "WEEK_VALUE_WRONG":
                continue
            wvw += 1
            wit = box if box is not None else pbp
            deltas.append(abs(o - wit))
            if any(w2 != wk and v2 is not None and abs(o - v2) <= 0.5
                   for w2, v2 in week_wit.items()):
                swaps += 1
    small = sum(1 for d in deltas if d <= 5)
    print(f"\n== 3. WEEK_VALUE_WRONG: {wvw} weeks ==")
    if deltas:
        import statistics
        print(f"   delta: median {statistics.median(deltas):.0f}, <=5 yds {small} "
              f"({small/len(deltas):.0%}), swap-with-other-week candidates {swaps}")

    # ---- 4/5. conflicts + pages quirks --------------------------------------------------
    sv_counts = Counter((s, sv) for (s, p, y, sv) in by_atom)
    print("\n== season-verdict counts by stat ==")
    for (s, sv), n in sorted(sv_counts.items()):
        print(f"   {s:20s} {sv:22s} {n}")
    era_pages = Counter(_era(y) for (s, p, y, sv) in by_atom if sv == "PAGES_QUIRK")
    print(f"\n== 5. PAGES_QUIRK by era == {dict(era_pages)}")
    era_confl = Counter(_era(y) for (s, p, y, sv) in by_atom if sv == "WITNESS_CONFLICT")
    print(f"== 4. WITNESS_CONFLICT by era == {dict(era_confl)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=DEFAULT_CSV)
    a = ap.parse_args()
    run(a.csv)
