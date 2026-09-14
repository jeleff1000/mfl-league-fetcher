"""THE SUBJECT MUST RECONCILE WITH ITSELF, LEVEL BY LEVEL, COLUMN BY COLUMN.

Joe, 2026-07-31: "our nfl table should recon at each level too. for each column" /
"we want the recon to hit weekly season and career and player bio wherever needed."

Every witness lane in this programme compares an EXTERNAL source against ONE plane of the
subject -- the weekly release, because that is the only thing `latest_v26()` returns. But
the subject has four planes, built from each other:

    weekly   nfl_player_stats_all        (player, year, week)
    season   player_nfl_season           (player, year)
    career   player_nfl_career           (player)
    s_team   player_nfl_season_team      (player, year, team)
    bio      player_bio                  (player)            <- identity, not stats

Nothing checked that they agree. A column can be right weekly and wrong at season, and no
external witness would ever see it, because no external witness is pointed at season.

WHAT THIS IS NOT. It is not a witness. Agreement here proves only INTERNAL consistency --
all four planes can be identically wrong, since three are derived from the first. That makes
it a CHECK, not a vote (witness_class semantics: derived may check, never votes). Its value
is the converse: DISAGREEMENT here is unambiguously our own defect, with no source to blame
and no lineage to argue about.

THE AGGREGATION CLASS DECIDES THE TEST, NEVER THE NAME. Summing a MAX column is how you
"discover" that fg_long is broken. Classes come from stat_contracts.v1.json:

    SUM                 season == SUM(weekly)      career == SUM(season)
    MAX                 season == MAX(weekly)      career == MAX(season)
    FIRST / ANY         compared for equality, not aggregated
    RECOMPUTE_RATE      SKIPPED -- a rate must be recomputed from its operands, and
    WEIGHTED_RECOMPUTE  re-deriving it here would just restate the equation lane
    NON_AGGREGATABLE    SKIPPED by definition
    EVENT_CARDINALITY   SKIPPED (games_played: its own lane, see below)

games_played is the one EVENT_CARDINALITY column and it is exactly where this matters. v26
holds a row only for weeks it captured, so weekly COUNT(*) counts weeks-we-have while season
games_played claims games-played. NFL.com is higher on 30,296 rows and lower on 167 -- a
one-sidedness that names the mechanism. Left out of the SUM sweep on purpose; it needs a
coverage lane, not an arithmetic one.

    python -m scripts.sota_recon.subject_level_recon [--limit N]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from . import sources as S
from .witness_votes import tolerance_of

LEDGER = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
          r"\subject_level_recon.json")
CONTRACTS = Path(__file__).parent / "witness_gate" / "contracts" / "stat_contracts.v1.json"

#: classes this lane can test, and the aggregate that is CORRECT for each
AGG_SQL = {"SUM": "SUM", "MAX": "MAX"}

#: tested for equality without aggregation -- a season's value should equal any week's
EQUALITY_CLASSES = ("FIRST", "ANY")

#: never tested here, with the reason
#: never tested here. THE REASON MUST NAME WHAT THE COLUMN ACTUALLY DOES AT SEASON GRAIN,
#: not merely that this lane declines it. The first version said NON_AGGREGATABLE meant
#: "declared not to aggregate" -- true, useless, and it buried 287 columns. Reading
#: `season_derivation` shows 276 of them declare RECOMPUTE (rank 160, ppg 60, lamar 56): they
#: DO have a season value, produced by a recompute rule, and nothing verifies it. Only 11 are
#: genuinely value-free at season grain, and all 11 are identity or context (NFL_player_id,
#: player, position, year, team_points).
SKIP = {
    "RECOMPUTE_RATE": "declared RECOMPUTE_RATE: the season value is recomputed from operands, "
                      "not aggregated. UNCHECKED by this lane -- needs a recompute lane.",
    "WEIGHTED_RECOMPUTE": "declared WEIGHTED_RECOMPUTE: recomputed with weights. UNCHECKED.",
    "NON_AGGREGATABLE": "declared NON_AGGREGATABLE; see season_derivation -- 'recompute' means "
                        "a season value EXISTS and is UNCHECKED, 'none' means identity/context",
    "EVENT_CARDINALITY": "games_played -- needs a coverage lane, not arithmetic",
}


def _classes() -> dict[str, str]:
    doc = json.loads(CONTRACTS.read_text(encoding="utf-8"))
    rows = doc.get("stats") or doc.get("contracts") or doc.get("rows") or []
    return {r["canonical_name"]: r.get("aggregation_class") for r in rows
            if r.get("canonical_name")}


def _season_derivation() -> dict[str, str]:
    """canonical_name -> declared season_derivation. Reading this is what turned 287
    "skipped, not aggregatable" columns into 276 "recomputed and unverified" plus 11
    identity/context."""
    doc = json.loads(CONTRACTS.read_text(encoding="utf-8"))
    rows = doc.get("stats") or doc.get("contracts") or doc.get("rows") or []
    return {r["canonical_name"]: r.get("season_derivation") for r in rows
            if r.get("canonical_name")}


def build(limit: int | None = None) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    wk = Path(S.v26_plane("weekly")).as_posix()
    se = Path(S.v26_plane("season")).as_posix()
    ca = Path(S.v26_plane("career")).as_posix()
    cols_wk = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{wk}')").fetchall()}
    cols_se = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{se}')").fetchall()}
    cols_ca = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{ca}')").fetchall()}
    klass = _classes()

    # THE DENOMINATOR IS DECLARED, not discovered from whatever happens to join. A column is
    # in scope iff it is PUBLISHED at both levels AND its class says how to combine it.
    scope = sorted(c for c in (cols_wk & cols_se)
                   if klass.get(c) in AGG_SQL or klass.get(c) in EQUALITY_CLASSES)
    skipped = {c: SKIP.get(klass.get(c), f"class={klass.get(c)!r} not in contracts")
               for c in sorted(cols_wk & cols_se) if c not in scope}
    if limit:
        scope = scope[:limit]

    rows = []
    for c in scope:
        cls = klass[c]
        # THE TOLERANCE IS DECLARED, NEVER CHOSEN (§17.1). EPA and other floats fail exact
        # equality for representation reasons alone: def_epa_allowed scored 0.0000 on n=861
        # under `=` and that was the comparison, not the data.
        tol = tolerance_of(c)
        eq = (f'ABS(w.v - s.v) <= {tol}' if tol else 'w.v = s.v')
        eq_ca = (f'ABS(s.v - k.v) <= {tol}' if tol else 's.v = k.v')
        if cls in EQUALITY_CLASSES:
            agg = "ANY_VALUE"
        else:
            agg = AGG_SQL[cls]
        try:
            n, ok = con.execute(f"""
                WITH w AS (SELECT NFL_player_id nid, year yr, {agg}("{c}") v
                           FROM read_parquet('{wk}') WHERE season_type='REG'
                             AND "{c}" IS NOT NULL GROUP BY 1, 2),
                     s AS (SELECT NFL_player_id nid, year yr, "{c}" v
                           FROM read_parquet('{se}') WHERE "{c}" IS NOT NULL)
                SELECT COUNT(*), COUNT(*) FILTER (WHERE {eq})
                FROM w JOIN s USING (nid, yr)""").fetchone()
        except Exception as e:                                    # noqa: BLE001
            rows.append({"column": c, "level": "weekly->season", "class": cls,
                         "error": str(e).splitlines()[0][:120]})
            continue
        rows.append({"column": c, "level": "weekly->season", "class": cls, "n": n,
                     "agree_pct": round(ok / n, 4) if n else None})

        # FIRST/ANY ARE NOT CHECKED AT season->career, AND NOT BECAUSE IT IS HARD. The
        # career semantic is UNDECLARED: nothing in stat_contracts says whether a career
        # FIRST is the first season's value, the last, or the rookie-year value. Comparing
        # against ANY_VALUE scored `age` at 0.0004 and that reads exactly like a defect --
        # it was my test picking an arbitrary season. Guessing the semantic to make a number
        # appear is how this lane would start lying.
        if c in cols_ca and cls not in EQUALITY_CLASSES:
            try:
                n2, ok2 = con.execute(f"""
                    WITH s AS (SELECT NFL_player_id nid, {agg}("{c}") v
                               FROM read_parquet('{se}') WHERE "{c}" IS NOT NULL GROUP BY 1),
                         k AS (SELECT NFL_player_id nid, "{c}" v
                               FROM read_parquet('{ca}') WHERE "{c}" IS NOT NULL)
                    SELECT COUNT(*), COUNT(*) FILTER (WHERE {eq_ca})
                    FROM s JOIN k USING (nid)""").fetchone()
            except Exception as e:                                # noqa: BLE001
                rows.append({"column": c, "level": "season->career", "class": cls,
                             "error": str(e).splitlines()[0][:120]})
                continue
            rows.append({"column": c, "level": "season->career", "class": cls, "n": n2,
                         "agree_pct": round(ok2 / n2, 4) if n2 else None})
    con.close()
    scored = [r for r in rows if r.get("agree_pct") is not None]
    return {
        # PIN THE RELEASE. This ledger is what the gate compares against, so a measurement
        # taken on an older release must be detectable as stale rather than trusted.
        "measured_against_release": Path(S.latest_v26()).parent.parent.name,
        "planes": {g: S.v26_plane(g) for g in ("weekly", "season", "career")},
        "rows": rows,
        "skipped": skipped,
        "counters": {
            "columns_in_scope": len(scope),
            "columns_published_both_levels": len(cols_wk & cols_se),
            "columns_skipped_by_class": len(skipped),
            "checks_run": len(scored),
            "checks_errored": sum(1 for r in rows if "error" in r),
            "checks_exact": sum(1 for r in scored if r["agree_pct"] == 1.0),
            "checks_below_995": sum(1 for r in scored if r["agree_pct"] < 0.995),
            "career_columns_absent_from_season": len(cols_ca - cols_se),
            # THE LAYER BENEATH `checks_exact`. 807 floored checks look like coverage; they
            # cover 430 of 1,077 weekly columns. These counters publish the rest so the
            # ladder cannot be mistaken for the whole table.
            "weekly_columns_total": len(cols_wk),
            "weekly_columns_with_no_season_counterpart": len(cols_wk - cols_se),
            "season_columns_absent_from_weekly": len(cols_se - cols_wk),
            "recompute_class_columns_unchecked": sum(
                1 for c in (cols_wk & cols_se)
                if klass.get(c) in ("RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE")
                or (klass.get(c) == "NON_AGGREGATABLE"
                    and _season_derivation().get(c) == "recompute")),
            "identity_or_context_legitimately_unladderable": sum(
                1 for c in (cols_wk & cols_se)
                if klass.get(c) == "NON_AGGREGATABLE"
                and _season_derivation().get(c) in (None, "none", "")),
        },
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    a = ap.parse_args()
    rep = build(a.limit)
    for k, v in rep["counters"].items():
        print(f"{k:44s} {v:>6}")
    bad = sorted((r for r in rep["rows"] if r.get("agree_pct") is not None
                  and r["agree_pct"] < 0.995), key=lambda r: r["agree_pct"])
    print(f"\n=== below 99.5% ({len(bad)}) ===")
    for r in bad[:40]:
        print(f"  {r['column']:34s} {r['level']:16s} {r['class']:6s} "
              f"{r['agree_pct']:7.4f}  n={r['n']:,}")
    errs = [r for r in rep["rows"] if "error" in r]
    if errs:
        print(f"\n=== errored ({len(errs)}) ===")
        for r in errs[:8]:
            print(f"  {r['column']:34s} {r['level']:16s} {r['error']}")
    # --limit MUST NOT CLOBBER THE LEDGER. The gate reads this file and compares it against
    # 807 floors, so a 3-column sample would report every other floored check as "no longer
    # measured" and fail the gate for a reason unrelated to the data. That happened within a
    # minute of the flag existing.
    if a.limit:
        print(f"\n--limit {a.limit}: SAMPLE RUN, ledger NOT written "
              f"({len(rep['rows'])} of ~841 checks -- the gate compares against all of them)")
    else:
        Path(LEDGER).write_text(json.dumps(rep, indent=1, default=str), encoding="utf-8")
        print(f"\nreceipts -> {LEDGER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
