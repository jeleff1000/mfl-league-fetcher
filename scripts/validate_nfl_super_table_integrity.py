"""NFL super-table integrity gate.

A battery of invariants that MUST hold for any super-table build. Each check maps to
a problem class that reached production silently before there was a gate for it; if a
future build reintroduces the problem, the corresponding check fails the build instead
of shipping it. Runs against a local parquet (default: the latest v26 release) or,
read-only, against Fly. Exit non-zero on any violation so it can gate promotion.

Problem classes encoded (2026-07-07):
  FRANCHISE  the Boston Yanks/Bulldogs leaking into the Redskins (players AND DST),
             mis-keyed DST ids (Jets/Titans DEF-138), franchise fielding two team
             codes in one year (the PFR authority's own merge bug).
  POSITION   career ranks depending on a per-rebuild MODE instead of a stored
             attribute; a player with no resolvable primary position.
  SCORING    the mislabeled/stale kicker column (pts_k_yahoo, 60+*6, no missed-XP).
  RANK       ranks going stale after a content change (rank not matching the points
             it ranks) -- the exact inconsistency a partial rebuild creates.

    python -m scripts.validate_nfl_super_table_integrity                 # local latest v26
    python -m scripts.validate_nfl_super_table_integrity --source <path>
    python -m scripts.validate_nfl_super_table_integrity --fly           # read-only Fly
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

# WEEKLY ranks monotonicity-checked against the per-row points they rank (higher points must
# never get a worse rank in the same year/week). Season/career ranks rank by AGGREGATED points,
# so they get a constancy check here (rank constant within its scope) + full reproduction is the
# WS-B golden diff's job -- the two gates are complementary.
_RANK_MONOTONIC = [
    ("rank_qb_4pt", "fpts_4pt_half", ("QB",)),
    ("rank_flex_ppr", "fpts_4pt_ppr", ("RB", "WR", "TE")),
    ("rank_k", "pts_k_std", ("K",)),
    ("rank_def", "pts_def_std", ("DEF",)),
    ("rank_te_tep", "fpts_4pt_tep", ("TE",)),
    ("rank_idp_flex_std", "pts_idp_std", ("LB", "DL", "DB", "ILB", "OLB", "MLB", "DE", "DT", "NT", "ED", "EDGE", "CB", "S", "SS", "FS", "SAF")),
]
# Season/career ranks must be constant within their scope group (denormalization integrity).
_RANK_CONSTANT = [
    ("rank_season_qb_4pt", "season"),
    ("rank_season_flex_ppr", "season"),
    ("rank_season_k", "season"),
    ("rank_alltime_qb_4pt", "career"),
    ("rank_alltime_k", "career"),
]


def _poslist(positions: tuple[str, ...]) -> str:
    return "[" + ", ".join("'" + p + "'" for p in positions) + "]"


def _checks(t: str, cols: set[str]) -> list[tuple[str, str, str]]:
    """(name, problem_class, sql-returning-violation-count). t = table/parquet ref."""
    checks: list[tuple[str, str, str]] = []

    # ---- FRANCHISE ----
    checks.append((
        "dst_id_equals_def_fn", "FRANCHISE",
        f"""SELECT COUNT(*) FROM {t} WHERE position='DEF' AND nfl_franchise_number IS NOT NULL
            AND NFL_player_id <> 'DEF-'||CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR)""",
    ))
    checks.append((
        "one_dst_id_per_franchise", "FRANCHISE",
        f"""SELECT COUNT(*) FROM (SELECT nfl_franchise_number FROM {t}
            WHERE position='DEF' AND nfl_franchise_number IS NOT NULL
            GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id) > 1)""",
    ))
    checks.append((
        "no_team_year_multi_franchise", "FRANCHISE",
        f"""SELECT COUNT(*) FROM (SELECT nfl_team, year FROM {t}
            WHERE nfl_team IS NOT NULL AND year IS NOT NULL AND nfl_franchise_number IS NOT NULL
            GROUP BY 1,2 HAVING COUNT(DISTINCT nfl_franchise_number) > 1)""",
    ))
    checks.append((
        "no_franchise_year_multi_code", "FRANCHISE",  # catches a franchise fielding two team codes in one year
        f"""SELECT COUNT(*) FROM (SELECT nfl_franchise_number, year FROM {t}
            WHERE nfl_team IS NOT NULL AND year IS NOT NULL AND nfl_franchise_number IS NOT NULL
            GROUP BY 1,2 HAVING COUNT(DISTINCT nfl_team) > 1)""",
    ))

    # ---- POSITION ----
    if "primary_position" in cols:
        checks.append((
            "primary_position_complete", "POSITION",
            f"""SELECT COUNT(*) FROM {t}
                WHERE position IS NOT NULL AND position <> '' AND (primary_position IS NULL OR primary_position='')""",
        ))
        checks.append((
            "primary_position_constant_per_player", "POSITION",
            f"""SELECT COUNT(*) FROM (SELECT NFL_player_id FROM {t}
                WHERE primary_position IS NOT NULL GROUP BY 1 HAVING COUNT(DISTINCT primary_position) > 1)""",
        ))
    else:
        checks.append(("primary_position_column_present", "POSITION", "SELECT 1"))  # missing -> violation

    # ---- SCORING ----
    checks.append((
        "legacy_pts_k_yahoo_absent", "SCORING",
        "SELECT 1" if "pts_k_yahoo" in cols else "SELECT 0",
    ))
    checks.append((
        "pts_k_std_present", "SCORING",
        "SELECT 0" if "pts_k_std" in cols else "SELECT 1",
    ))

    # ---- RANK weekly monotonicity (rank must agree with the per-week points it ranks) ----
    week_grp = "CAST(year AS VARCHAR)||'|'||CAST(week AS VARCHAR)||'|'||COALESCE(season_type,'')"
    for rank_col, pts_col, positions in _RANK_MONOTONIC:
        if rank_col not in cols or pts_col not in cols:
            continue
        checks.append((
            f"rank_monotonic_{rank_col}", "RANK",
            f"""
            WITH pool AS (
              SELECT {week_grp} AS grp, {rank_col} AS rk, TRY_CAST({pts_col} AS DOUBLE) AS pts
              FROM {t}
              WHERE {rank_col} IS NOT NULL AND {pts_col} IS NOT NULL
                AND list_has_any(string_split(COALESCE(position,''), ','), {_poslist(positions)})
            )
            SELECT COUNT(*) FROM pool a JOIN pool b
              ON a.grp = b.grp AND a.rk < b.rk AND a.pts < b.pts
            """,
        ))
    # ---- RANK denormalization constancy (season/career rank constant within its scope group) ----
    for rank_col, scope in _RANK_CONSTANT:
        if rank_col not in cols:
            continue
        key = 'NFL_player_id, year' if scope == "season" else 'NFL_player_id'
        checks.append((
            f"rank_constant_{rank_col}", "RANK",
            f"""SELECT COUNT(*) FROM (SELECT {key} FROM {t} WHERE {rank_col} IS NOT NULL
                GROUP BY {key} HAVING COUNT(DISTINCT {rank_col}) > 1)""",
        ))
    return checks


def run(source_ref: str, database: str | None) -> dict:
    if database:  # Fly, read-only
        from multi_league.core.readers.fly_reader import FlyReader  # noqa
        raise SystemExit("Fly mode: wire FlyReader.query per check (read-only). Not enabled in this offline run.")
    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    con.execute("SET memory_limit='8GB'")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {source_ref}").fetchall()}
    results = []
    for name, klass, sql in _checks(source_ref, cols):
        try:
            v = int(con.execute(sql).fetchone()[0] or 0)
        except Exception as e:  # a check that can't run is itself a violation
            v = -1
            name = f"{name} (ERROR: {str(e)[:60]})"
        results.append({"check": name, "class": klass, "violations": v})
    con.close()
    failed = [r for r in results if r["violations"] != 0]
    return {"source": source_ref, "checks": results, "passed": not failed, "failed": failed}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--source", default=None, help="local parquet path (default: latest v26)")
    ap.add_argument("--fly", action="store_true", help="run read-only against Fly ___ops")
    args = ap.parse_args()
    if args.fly:
        return run("", "___ops")["passed"] and 0 or 1
    if args.source:
        src = f"read_parquet('{Path(args.source).as_posix()}')"
    else:
        from sota_recon.sources import latest_v26
        src = f"read_parquet('{Path(latest_v26()).as_posix()}')"
    res = run(src, None)
    print(f"[integrity] source: {res['source']}")
    by_class: dict[str, list] = {}
    for r in res["checks"]:
        by_class.setdefault(r["class"], []).append(r)
    for klass, rows in by_class.items():
        bad = [r for r in rows if r["violations"] != 0]
        tag = "OK" if not bad else f"{len(bad)} FAIL"
        print(f"  [{klass}] {tag}")
        for r in bad:
            print(f"      x {r['check']}: {r['violations']} violations")
    print(f"[integrity] {'PASS' if res['passed'] else 'FAIL'} ({len(res['failed'])} checks failing)")
    return 0 if res["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
