"""Build the exact unresolved playoff/championship league-year manifest.

This is an inventory-only step.  It reads the canonical GH lake and emits one
row per populated league-year whose playoff-team setting is null and whose
player rows do not already reveal an exact 4/6/8-team bracket.  It never writes
the lake and never treats the season-level ``champion`` marker as a title-game
signal.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def cols(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {relation}").fetchall()}


def build(root: Path, out: Path) -> dict:
    base = root / "corpus_snapshot.duckdb"
    if not base.is_file() or base.stat().st_size == 0:
        raise SystemExit(f"missing canonical snapshot: {base}")
    con = duckdb.connect(str(base), read_only=True)
    try:
        pcols = cols(con, "public.player_fantasy")
        scols = cols(con, "public.league_settings")
        required_p = {"db_name", "year", "team_key", "team_name", "manager", "is_playoffs"}
        required_s = {"db_name", "year", "playoff_teams", "league_key", "platform"}
        missing = (required_p - pcols) | (required_s - scols)
        if missing:
            raise SystemExit(f"canonical schema missing required columns: {sorted(missing)}")

        team_expr = (
            "COALESCE(NULLIF(TRIM(CAST(p.team_key AS VARCHAR)), ''), "
            "NULLIF(TRIM(CAST(p.team_name AS VARCHAR)), ''), "
            "NULLIF(TRIM(CAST(p.manager AS VARCHAR)), ''))"
        )
        # Do not count champion as a playoff signal.  It is a season winner
        # marker and is precisely the field that previously inflated title rates.
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE _bracket AS
          SELECT p.db_name, CAST(p.year AS INTEGER) AS year,
            COUNT(DISTINCT CASE WHEN p.made_playoffs=1 THEN {team_expr} END) AS made_teams,
            COUNT(DISTINCT CASE WHEN p.final_playoff_seed BETWEEN 1 AND 32 THEN {team_expr} END) AS seeded_teams,
            COUNT(DISTINCT CASE WHEN p.is_playoffs=1 THEN {team_expr} END) AS playoff_week_teams
          FROM public.player_fantasy p
          JOIN public.league_settings s
            ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
          WHERE s.playoff_teams IS NULL AND p.db_name IS NOT NULL
          GROUP BY 1,2
        """)
        max_week_expr = "MAX(CAST(p.week AS INTEGER))"
        con.execute(f"""
          CREATE OR REPLACE TEMP TABLE _targets AS
          SELECT
            s.db_name,
            CAST(s.year AS INTEGER) AS year,
            LOWER(TRIM(CAST(s.platform AS VARCHAR))) AS platform,
            CAST(s.league_key AS VARCHAR) AS source_id,
            CAST(COALESCE(s.playoff_start_week,
              CASE WHEN CAST(s.year AS INTEGER) < 2021 THEN 15 ELSE 18 END) AS INTEGER) AS playoff_start_week,
            CAST({max_week_expr} AS INTEGER) AS max_player_week,
            b.made_teams, b.seeded_teams, b.playoff_week_teams
          FROM public.league_settings s
          JOIN public.player_fantasy p
            ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
          JOIN _bracket b
            ON b.db_name=s.db_name AND b.year=CAST(s.year AS INTEGER)
          WHERE s.playoff_teams IS NULL
          GROUP BY ALL
        """)
        rows = con.execute("""
          SELECT db_name, year, platform, source_id, playoff_start_week,
                 max_player_week, made_teams, seeded_teams, playoff_week_teams
          FROM _targets
          WHERE COALESCE(NULLIF(made_teams, 0), NULLIF(seeded_teams, 0), NULLIF(playoff_week_teams, 0)) IS NULL
             OR COALESCE(NULLIF(made_teams, 0), NULLIF(seeded_teams, 0), NULLIF(playoff_week_teams, 0)) NOT IN (4, 6, 8)
          ORDER BY platform, year, db_name
        """).fetchall()
        targets = []
        for db_name, year, platform, source_id, start, max_week, made, seeded, playoff_week in rows:
            if not platform or platform not in {"mfl", "fleaflicker", "sleeper"}:
                raise SystemExit(f"unsupported/missing platform for {db_name}/{year}: {platform!r}")
            if not str(source_id or '').strip():
                raise SystemExit(f"missing source ID for {db_name}/{year}")
            start = int(start or (15 if int(year) < 2021 else 18))
            max_week = int(max_week or start)
            weeks = list(range(start, max(max_week, start) + 1))
            targets.append({
                "db_name": str(db_name), "year": int(year), "platform": str(platform),
                "source_id": str(source_id), "weeks": weeks,
                "reason_codes": ["unresolved_playoff_bracket", "missing_championship_signal"],
                "bracket_evidence": {
                    "made_teams": int(made or 0), "seeded_teams": int(seeded or 0),
                    "playoff_week_teams": int(playoff_week or 0),
                },
            })
        if len(targets) != 617:
            raise SystemExit(f"fail-closed target count: expected 617 unresolved brackets, got {len(targets)}")
        counts = {}
        for row in targets:
            counts[row["platform"]] = counts.get(row["platform"], 0) + 1
        result = {
            "schema_version": 1,
            "population": "populated league-years with null playoff_teams and unresolved 4/6/8 bracket evidence",
            "target_count": len(targets), "by_platform": counts, "targets": targets,
        }
        out.mkdir(parents=True, exist_ok=True)
        (out / "playoff_championship_targets.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        (out / "playoff_championship_targets_summary.json").write_text(json.dumps({k: result[k] for k in ("schema_version", "population", "target_count", "by_platform")}, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({k: result[k] for k in ("target_count", "by_platform")}, sort_keys=True))
        return result
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    build(args.root, args.out)


if __name__ == "__main__":
    main()
