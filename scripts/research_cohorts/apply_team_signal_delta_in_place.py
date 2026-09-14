"""Apply an already-audited team fan-out delta to the frozen player table.

This is intentionally narrow: it updates existing player rows only, fills NULLs
only, and refuses schema/duplicate/key violations. It never touches ops tables.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


BASE_PLAYER_COLUMNS = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar",
    "manager", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "player", "position", "fantasy_position", "platform",
    "team_key", "team_name", "nfl_team_api", "yahoo_player_id",
    "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--delta", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.base))
    pcols = [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()]
    missing = set(BASE_PLAYER_COLUMNS) - set(pcols)
    optional = {'loss', 'tie'}
    cohort_columns = [c for c in pcols if c not in BASE_PLAYER_COLUMNS and c not in optional]
    forbidden = {'opponent_points','is_championship','is_active','is_playoffs_bf','made_po_bf','made_po'} & set(pcols)
    if missing or len(cohort_columns) != 7 or forbidden:
        raise SystemExit(f"canonical player schema mismatch: missing={sorted(missing)} cohort_columns={cohort_columns} optional={[c for c in pcols if c in optional]} forbidden={sorted(forbidden)}")
    loss_expr = "p.loss" if "loss" in pcols else "CAST(NULL AS INTEGER)"
    tie_expr = "p.tie" if "tie" in pcols else "CAST(NULL AS INTEGER)"
    canonical_columns_before = list(pcols)
    dpath = str(args.delta.resolve()).replace("'", "''")
    con.execute(f"CREATE OR REPLACE TEMP VIEW delta_raw AS SELECT * FROM read_parquet('{dpath}')")
    dcols = {r[0] for r in con.execute("DESCRIBE delta_raw").fetchall()}
    required = {"db_name", "year", "week", "NFL_player_id", "manager", "team_key", "team_name", "platform"}
    required |= {"source_win", "source_team_points", "source_playoffs", "source_champion", "source_final_playoff_seed"}
    if not required <= dcols:
        raise SystemExit(f"delta schema missing: {sorted(required-dcols)}")
    key = "db_name,year,week,NFL_player_id,manager,team_key,team_name,platform"
    dup = con.execute(f"SELECT COUNT(*) FROM (SELECT {key} FROM delta_raw GROUP BY ALL HAVING COUNT(*)>1)").fetchone()[0]
    if dup:
        raise SystemExit(f"duplicate delta keys: {dup}")
    before_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE matched AS
      SELECT p.rowid player_rowid,
             p.win canonical_win,
             {loss_expr} canonical_loss,
             {tie_expr} canonical_tie,
             p.team_points canonical_team_points,
             p.is_playoffs canonical_is_playoffs,
             p.champion canonical_champion,
             p.final_playoff_seed canonical_final_playoff_seed,
             p.made_playoffs canonical_made_playoffs,
             d.*
      FROM public.player_fantasy p JOIN delta_raw d
        ON d.db_name=p.db_name AND CAST(d.year AS INTEGER)=CAST(p.year AS INTEGER)
       AND CAST(d.week AS INTEGER)=CAST(p.week AS INTEGER)
       AND d.NFL_player_id=p.NFL_player_id
       AND d.platform=p.platform
       AND d.manager IS NOT DISTINCT FROM p.manager
       AND d.team_key IS NOT DISTINCT FROM p.team_key
       AND d.team_name IS NOT DISTINCT FROM p.team_name
    """)
    matched = con.execute("SELECT COUNT(*) FROM matched").fetchone()[0]
    if matched != con.execute("SELECT COUNT(*) FROM delta_raw").fetchone()[0]:
        raise SystemExit("delta contains unmatched canonical player rows")
    fields = {
        "win": "source_win",
        "loss": "source_loss",
        "tie": "source_tie",
        "team_points": "source_team_points",
        "is_playoffs": "source_playoffs",
        "champion": "source_champion",
        "final_playoff_seed": "source_final_playoff_seed",
        "made_playoffs": "source_made_playoffs",
    }
    improvements = {}
    for field, src in fields.items():
        if src not in dcols:
            continue
        canonical = f"canonical_{field}"
        improvements[field] = con.execute(f"SELECT COUNT(*) FROM matched WHERE {canonical} IS NULL AND {src} IS NOT NULL").fetchone()[0]
    update_assignments = [
        "win=CASE WHEN p.win IS NULL THEN m.source_win ELSE p.win END",
        "team_points=CASE WHEN p.team_points IS NULL THEN m.source_team_points ELSE p.team_points END",
        "is_playoffs=CASE WHEN p.is_playoffs IS NULL AND m.source_playoffs=1 THEN 1 ELSE p.is_playoffs END",
        "champion=CASE WHEN p.champion IS NULL AND m.source_champion=1 THEN 1 ELSE p.champion END",
        "final_playoff_seed=CASE WHEN p.final_playoff_seed IS NULL THEN m.source_final_playoff_seed ELSE p.final_playoff_seed END",
        "made_playoffs=CASE WHEN p.made_playoffs IS NULL THEN m.source_made_playoffs ELSE p.made_playoffs END",
    ]
    if "loss" in pcols and "source_loss" in dcols:
        update_assignments.insert(1, "loss=CASE WHEN p.loss IS NULL THEN m.source_loss ELSE p.loss END")
    if "tie" in pcols and "source_tie" in dcols:
        update_assignments.insert(2 if "loss" in pcols and "source_loss" in dcols else 1, "tie=CASE WHEN p.tie IS NULL THEN m.source_tie ELSE p.tie END")
    update_sql = ",\n            ".join(update_assignments)
    con.execute("BEGIN")
    try:
        con.execute(f"""
          UPDATE public.player_fantasy p SET
            {update_sql}
          FROM matched m WHERE p.rowid=m.player_rowid
        """)
        if con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] != before_rows:
            raise SystemExit("player row count changed")
        if [r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()] != canonical_columns_before:
            raise SystemExit("player schema changed")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    report = {
        "in_place": True, "cache_mutated": True, "new_lineage": False,
        "ops_untouched": True, "schema_unchanged": True,
        "delta_rows": int(con.execute("SELECT COUNT(*) FROM delta_raw").fetchone()[0]),
        "matched_rows": int(matched),
        "improvements_by_field": {k:int(v) for k,v in improvements.items()},
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))


if __name__ == "__main__":
    main()
