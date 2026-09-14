"""Audit player team-game schedule uniqueness in the public research lake."""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    # LocalReader attaches the immutable corpus/ops sources and exposes the same
    # derived public views used by the matchup builder.
    root = Path(__file__).resolve().parents[2]
    import sys
    sys.path.insert(0, str(root / "scripts" / "sleeper_corpus"))
    from local_reader import LocalReader
    con = LocalReader().con
    q = """
    WITH x AS (
      SELECT year, NFL_player_id, week,
             COUNT(*) AS physical_rows,
             COUNT(DISTINCT team) AS distinct_teams
      FROM public.player_team_game_week
      GROUP BY ALL
    )
    SELECT year,
           SUM(physical_rows) AS physical_rows,
           COUNT(*) AS player_week_keys,
           SUM(physical_rows - 1) AS extra_rows,
           COUNT(*) FILTER (physical_rows > 1) AS duplicated_player_weeks,
           MAX(physical_rows) AS max_rows_per_player_week,
           MAX(distinct_teams) AS max_teams_per_player_week
    FROM x
    GROUP BY year
    ORDER BY year
    """
    # Some snapshots do not retain team on this view. The join contract only
    # needs player/week uniqueness, so retry with the portable projection.
    try:
        rows = con.execute(q).fetchall()
        cols = [d[0] for d in con.description]
    except duckdb.BinderException:
        q = q.replace("COUNT(DISTINCT team) AS distinct_teams", "1 AS distinct_teams")
        rows = con.execute(q).fetchall()
        cols = [d[0] for d in con.description]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        f.write(",".join(cols) + "\n")
        for row in rows:
            f.write(",".join("" if v is None else str(v) for v in row) + "\n")

    audits = {
        "position": """
          SELECT year, SUM(n) physical_rows, COUNT(*) keys,
                 SUM(n-1) extra_rows, COUNT(*) FILTER (n>1) duplicated_keys,
                 MAX(n) max_rows_per_key
          FROM (SELECT year,NFL_player_id,COUNT(*) n
                FROM public.player_position GROUP BY ALL)
          GROUP BY year ORDER BY year
        """,
        "active": """
          SELECT year, SUM(n) physical_rows, COUNT(*) keys,
                 SUM(n-1) extra_rows, COUNT(*) FILTER (n>1) duplicated_keys,
                 MAX(n) max_rows_per_key
          FROM (SELECT year,NFL_player_id,week,COUNT(*) n
                FROM public.player_active_week GROUP BY ALL)
          GROUP BY year ORDER BY year
        """,
    }
    for label, sql in audits.items():
        try:
            rows = con.execute(sql).fetchall()
        except duckdb.CatalogException:
            continue
        target = out.with_name(f"team_schedule_{label}.csv")
        with target.open("w", encoding="utf-8", newline="") as f:
            f.write(",".join(d[0] for d in con.description) + "\n")
            for row in rows:
                f.write(",".join("" if v is None else str(v) for v in row) + "\n")
        print(f"wrote {len(rows)} years to {target}")
    print(f"wrote {len(rows)} years to {out}")


if __name__ == "__main__":
    main()
