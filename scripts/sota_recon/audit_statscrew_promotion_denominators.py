"""Audit StatsCrew promotion candidates against the lower-layer counters.

This is a receipt lane, not a promotion lane. A candidate only clears when its
counter/rate has an explicit lower-layer operand or identity; a clean numerator
alone is never sufficient.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .statscrew_column_adjudication import build_decisions
from .sources import DATA_LAKE

STATS = Path(DATA_LAKE) / "ff_assets" / "statscrew" / "team_season_stats" / "*" / "shards" / "shard-*" / "records.parquet"
RESULTS = Path(DATA_LAKE) / "ff_assets" / "statscrew" / "team_season_results" / "*" / "shards" / "shard-*" / "records.parquet"
OUT = Path("docs/audits/statscrew-promotion-denominator-2025.json")


def _row(status: str, key: str, *, denominator: str | None = None,
         comparable: int | None = None, matches: int | None = None,
         reason: str) -> dict:
    return {
        "key": key,
        "status": status,
        "denominator": denominator,
        "comparable": comparable,
        "matches": matches,
        "reason": reason,
    }


def build() -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    stats = STATS.as_posix()
    rows: list[dict] = []

    # Long is a max, but it still needs the event count and total-yard bound from
    # the lower layer. This is deliberately measured on player rows, not Totals rows.
    n, ok = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE TRY_CAST(long AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(no AS DOUBLE) > 0),
          COUNT(*) FILTER (WHERE TRY_CAST(long AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(no AS DOUBLE) > 0
                           AND TRY_CAST(long AS DOUBLE) <= TRY_CAST(yds AS DOUBLE))
        FROM read_parquet('{stats}', union_by_name=true)
        WHERE table_tag='interceptions' AND NOT is_total_row
    """).fetchone()
    rows.append(_row(
        "HOLD" if n != ok or (ok / n if n else 0) < 0.995 else "PASS",
        "statscrew_team_season_stats|interceptions|long",
        denominator="no; bound long <= yds", comparable=int(n), matches=int(ok),
        reason="interception-return count and total-yard bound; below the 0.995 bar"
        if n != ok else "interception-return count and total-yard bound passed"))

    # Kickoff yards and average share the same source denominator. Because KO is
    # itself a promotion candidate with no lower-layer receipt, yards cannot clear
    # by citing the candidate as its own denominator.
    n, ok = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE TRY_CAST(avg AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(ko AS DOUBLE) > 0),
          COUNT(*) FILTER (WHERE TRY_CAST(avg AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(ko AS DOUBLE) > 0
                           AND ABS(TRY_CAST(avg AS DOUBLE)
                                   - TRY_CAST(yds AS DOUBLE)/TRY_CAST(ko AS DOUBLE)) <= 0.15)
        FROM read_parquet('{stats}', union_by_name=true)
        WHERE table_tag='kicking'
    """).fetchone()
    for column in ("yds", "ko"):
        rows.append(_row(
            "HOLD", f"statscrew_team_season_stats|kicking|{column}",
            denominator="ko", comparable=int(n), matches=int(ok),
            reason="kickoff volume is part of the same unresolved candidate family; "
                   "no independent lower-layer denominator receipt"))

    # Kickoff Long has no rows where both its lower-layer total and event count are
    # present, so a clean/empty counter must not be mistaken for a pass.
    n = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{stats}', union_by_name=true)
        WHERE table_tag='kicking' AND TRY_CAST(long AS DOUBLE) IS NOT NULL
          AND TRY_CAST(yds AS DOUBLE) IS NOT NULL AND TRY_CAST(ko AS DOUBLE) > 0
    """).fetchone()[0]
    rows.append(_row(
        "HOLD", "statscrew_team_season_stats|kicking|long",
        denominator="ko; bound long <= yds", comparable=int(n), matches=None,
        reason="no comparable lower-layer rows"))

    # StatsCrew's Total Points is the requested scorer-points measure: passing
    # touchdowns are not in this scoring table, while the scorer-side touchdown,
    # safety, two-point, field-goal, extra-point, and single components are.
    n, ok = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE TRY_CAST(points AS DOUBLE) IS NOT NULL),
          COUNT(*) FILTER (WHERE TRY_CAST(points AS DOUBLE) IS NOT NULL
            AND ABS(TRY_CAST(points AS DOUBLE) - (
              6*(COALESCE(TRY_CAST(rush AS DOUBLE),0)
                + COALESCE(TRY_CAST(rec AS DOUBLE),0)
                + COALESCE(TRY_CAST(punt AS DOUBLE),0)
                + COALESCE(TRY_CAST(kick AS DOUBLE),0)
                + COALESCE(TRY_CAST(int AS DOUBLE),0)
                + COALESCE(TRY_CAST(fum AS DOUBLE),0)
                + COALESCE(TRY_CAST(mfg AS DOUBLE),0)
                + COALESCE(TRY_CAST(other AS DOUBLE),0))
              + 3*COALESCE(TRY_CAST(fg AS DOUBLE),0)
              + COALESCE(TRY_CAST(x_c AS DOUBLE),0)
              + COALESCE(TRY_CAST(single AS DOUBLE),0)
              + 2*(COALESCE(TRY_CAST(saf AS DOUBLE),0)
                   + COALESCE(TRY_CAST("2pt" AS DOUBLE),0))
            )) <= 0.01)
        FROM read_parquet('{stats}', union_by_name=true)
        WHERE table_tag='total_scoring' AND NOT is_total_row
    """).fetchone()
    rows.append(_row(
        "PASS" if n == ok and n > 0 else "HOLD",
        "statscrew_team_season_stats|total_scoring|points",
        denominator="scorer TD/FG/PAT/single/safety/2pt components",
        comparable=int(n), matches=int(ok),
        reason="existing total_points_scored canonical validated with scorer-only formula"))

    # Kicking points are independently reconstructible from the published made-FG
    # and made-PAT counters; this is the one candidate that clears this pass.
    n, ok = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE TRY_CAST(pts AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(fgm AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(x_cm AS DOUBLE) IS NOT NULL),
          COUNT(*) FILTER (WHERE TRY_CAST(pts AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(fgm AS DOUBLE) IS NOT NULL
                           AND TRY_CAST(x_cm AS DOUBLE) IS NOT NULL
                           AND ABS(TRY_CAST(pts AS DOUBLE)
                                   - (TRY_CAST(fgm AS DOUBLE)*3 + TRY_CAST(x_cm AS DOUBLE))) <= 0.01)
        FROM read_parquet('{stats}', union_by_name=true)
        WHERE table_tag='kicking'
    """).fetchone()
    rows.append(_row(
        "PASS" if n == ok and n > 0 else "HOLD",
        "statscrew_team_season_stats|kicking|pts",
        denominator="fgm*3 + x_cm", comparable=int(n), matches=int(ok),
        reason="published kicking-points identity against made-FG and made-PAT counters"))

    # These are real material, but no lower-layer counter/closure receipt exists yet.
    for column, reason in {
        "mfg": "no independent lower-layer closure for missed-FG return touchdowns",
        "other": "residual touchdown bucket has no lower-layer closure identity",
        "2pt": "total two-point conversions lack a receipted component closure",
    }.items():
        rows.append(_row(
            "HOLD", f"statscrew_team_season_stats|total_scoring|{column}",
            reason=reason))

    # Context is not a counter; it is a split-at-admission candidate and therefore
    # has no denominator to clear.
    rows.append(_row(
        "NOT_A_COUNTER", "statscrew_team_season_results|*|col_7",
        reason="mixed playoff-round/neutral-site context cell; route to a split schema decision"))
    con.close()

    decisions = build_decisions()[0]
    candidates = [row["key"] for row in decisions
                  if row["disposition"] == "NEW_SUPERTABLE_COLUMN_CANDIDATE"]
    existing_canonical_candidates = [
        {"key": row["key"], "canonical": row["canonical"],
         "formula": row["candidate_formula"]}
        for row in decisions if row.get("candidate_role") == "EXISTING_CANONICAL_VALIDATION"
    ]
    derived_witnesses = [
        {
            "key": row["key"],
            "disposition": row["disposition"],
            "canonical": row.get("canonical"),
            "canonical_numerator": row.get("canonical_numerator"),
            "canonical_denominator": row.get("canonical_denominator"),
            "source_numerator": row.get("source_numerator"),
            "source_denominator": row.get("source_denominator"),
            "status": "OPERAND_MAPPED" if row.get("canonical_numerator")
            and row.get("canonical_denominator") else "SOURCE_ONLY_HOLD",
        }
        for row in decisions if row.get("witness_kind") == "DERIVED_WITNESS"
    ]
    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "candidate_count": len(candidates),
        "candidates": candidates,
        "existing_canonical_candidates": existing_canonical_candidates,
        "derived_witnesses": derived_witnesses,
        "rows": rows,
        "counts": {
            "pass": sum(r["status"] == "PASS" for r in rows),
            "held": sum(r["status"] == "HOLD" for r in rows),
            "not_a_counter": sum(r["status"] == "NOT_A_COUNTER" for r in rows),
        },
        "policy": "A clean counter without a lower-layer denominator/constraint receipt remains held.",
    }
    return report


def main() -> None:
    report = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report["counts"], sort_keys=True))


if __name__ == "__main__":
    main()
