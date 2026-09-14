"""Apply a validated source team-week sidecar directly to a lake copy.

The restored GitHub cache is the working copy.  This script updates only
matched player rows and matching/new matchup rows in that copy; it never
creates a replacement player table or changes the canonical schema.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


PLAYER_COLUMNS = [
    "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
    "fantasy_points", "win", "champion", "clutch_equity", "manager_lamar",
    "manager", "team_points", "final_playoff_seed", "is_playoffs",
    "has_po_signal", "player", "position", "fantasy_position", "platform",
    "team_key", "team_name", "nfl_team_api", "yahoo_player_id",
    "sleeper_player_id", "espn_player_id", "fleaflicker_player_id",
    "mfl_player_id", "made_playoffs",
]


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def columns(con: duckdb.DuckDBPyConnection, relation: str) -> list[str]:
    return [row[0] for row in con.execute(f"DESCRIBE {relation}").fetchall()]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=Path, required=True)
    ap.add_argument("--sidecar", type=Path, required=True)
    ap.add_argument("--report", type=Path, required=True)
    args = ap.parse_args()

    con = duckdb.connect(str(args.base))
    con.execute("PRAGMA preserve_insertion_order=false")
    sidecar_path = str(args.sidecar.resolve()).replace("'", "''")
    con.execute(
        f"CREATE OR REPLACE TEMP VIEW _source_raw AS "
        f"SELECT * FROM read_parquet('{sidecar_path}')"
    )
    source_all = columns(con, "_source_raw")
    source_only = {"target_year", "target_reason_codes"}
    source_matchup = [c for c in source_all if c not in source_only]
    base_player = columns(con, "public.player_fantasy")
    base_matchup = columns(con, "public.matchup")
    base_types = {row[0]: row[1] for row in con.execute("DESCRIBE public.matchup").fetchall()}
    if base_player != PLAYER_COLUMNS:
        raise SystemExit(f"canonical player schema mismatch: {base_player}")
    source_extras = set(source_matchup) - set(base_matchup)
    if source_extras:
        raise SystemExit(
            "source matchup contains non-canonical fields: "
            f"extra={sorted(source_extras)}"
        )
    if "target_year" not in source_all and "target_reason_codes" not in source_all:
        # This is allowed: the source may already have been projected.
        pass

    # Project the source into the base matchup order.  The source artifact is
    # allowed to serialize the exact same fields in another order.
    source_projection = ", ".join(
        f"s.{qi(c)} AS {qi(c)}" if c in source_matchup
        else f"CAST(NULL AS {base_types[c]}) AS {qi(c)}"
        for c in base_matchup
    )
    con.execute(
        f"CREATE OR REPLACE TEMP TABLE _source_matchup AS "
        f"SELECT {source_projection} FROM _source_raw s"
    )
    duplicate_source_keys = con.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT db_name, CAST(year AS INTEGER) season_year, CAST(week AS INTEGER) season_week,
                 NULLIF(TRIM(CAST(team_key AS VARCHAR)), '') team_key
          FROM _source_matchup
          GROUP BY 1,2,3,4 HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]
    if duplicate_source_keys:
        raise SystemExit(f"duplicate source team-week keys: {duplicate_source_keys}")

    p_manager = "LOWER(NULLIF(TRIM(CAST(p.manager AS VARCHAR)), ''))"
    p_team = "LOWER(NULLIF(TRIM(CAST(p.team_name AS VARCHAR)), ''))"
    p_team_key = "NULLIF(TRIM(CAST(p.team_key AS VARCHAR)), '')"
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _signals_raw AS
        SELECT CAST(db_name AS VARCHAR) db_name, CAST(year AS INTEGER) season_year,
               CAST(week AS INTEGER) season_week,
               NULLIF(TRIM(CAST(team_key AS VARCHAR)), '') source_team_key,
               LOWER(NULLIF(TRIM(CAST(manager AS VARCHAR)), '')) manager_key,
               LOWER(NULLIF(TRIM(CAST(team_name AS VARCHAR)), '')) team_name_key,
               MAX(CAST(win AS INTEGER)) source_win,
               MAX(team_points) source_team_points,
               MAX(CASE WHEN CAST(is_playoffs AS INTEGER)=1 THEN 1 ELSE 0 END) source_playoffs,
               MAX(CASE WHEN CAST(is_championship AS INTEGER)=1
                          AND CAST(champion AS INTEGER)=1 THEN 1 ELSE 0 END) source_champion,
               MAX(NULLIF(LOWER(TRIM(CAST(platform AS VARCHAR))), '')) source_platform
        FROM _source_matchup
        GROUP BY 1,2,3,4,5,6
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TEMP TABLE _signals AS
        SELECT r.*, COUNT(DISTINCT source_team_key) OVER
          (PARTITION BY db_name, season_year, season_week, manager_key) manager_team_count
        FROM _signals_raw r
        """
    )
    identity = f"""
      (({p_team_key} IS NOT NULL AND s.source_team_key IS NOT NULL
         AND {p_team_key}=s.source_team_key)
       OR ({p_manager}=s.manager_key AND {p_team}=s.team_name_key
           AND s.team_name_key IS NOT NULL)
       OR ({p_manager}=s.manager_key AND s.manager_key IS NOT NULL
           AND s.manager_team_count=1))
    """
    # Crucially, this is an inner join: _player_matches contains only rows
    # that can actually be improved.  The canonical table is never expanded
    # into a 269-million-row staging table.
    con.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _player_matches AS
        SELECT p.rowid player_rowid, COUNT(DISTINCT s.source_team_key) matches,
               MAX(s.source_win) source_win,
               MAX(s.source_team_points) source_team_points,
               MAX(s.source_playoffs) source_playoffs,
               MAX(s.source_champion) source_champion,
               MAX(s.source_platform) source_platform
        FROM public.player_fantasy p
        JOIN _signals s
          ON s.db_name=p.db_name
         AND s.season_year=CAST(p.year AS INTEGER)
         AND s.season_week=CAST(p.week AS INTEGER)
         AND {identity}
        GROUP BY p.rowid
        """
    )
    ambiguous = con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches > 1").fetchone()[0]
    if ambiguous:
        raise SystemExit(f"ambiguous source team joins: {ambiguous}")

    before_player_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    before_matchup_rows = con.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0]
    improvements = {}
    for field, source_field in {
        "win": "source_win", "team_points": "source_team_points",
        "is_playoffs": "source_playoffs", "champion": "source_champion",
        "platform": "source_platform",
    }.items():
        improvements[field] = con.execute(
            f"""
            SELECT COUNT(*) FROM public.player_fantasy p
            JOIN _player_matches m ON p.rowid=m.player_rowid
            WHERE p.{qi(field)} IS NULL AND m.{qi(source_field)} IS NOT NULL
            """
        ).fetchone()[0]

    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            """
            UPDATE public.player_fantasy AS p
            SET win = CASE WHEN p.win IS NULL THEN m.source_win ELSE p.win END,
                team_points = CASE WHEN p.team_points IS NULL THEN m.source_team_points ELSE p.team_points END,
                is_playoffs = CASE WHEN p.is_playoffs IS NULL AND m.source_playoffs=1 THEN 1 ELSE p.is_playoffs END,
                champion = CASE WHEN p.champion IS NULL AND m.source_champion=1 THEN 1 ELSE p.champion END,
                platform = CASE WHEN p.platform IS NULL THEN m.source_platform ELSE p.platform END
            FROM _player_matches m
            WHERE p.rowid=m.player_rowid
            """
        )

        key = "CAST(b.db_name AS VARCHAR) IS NOT DISTINCT FROM CAST(s.db_name AS VARCHAR) AND " \
              "CAST(b.year AS INTEGER) IS NOT DISTINCT FROM CAST(s.year AS INTEGER) AND " \
              "CAST(b.week AS INTEGER) IS NOT DISTINCT FROM CAST(s.week AS INTEGER) AND " \
              "NULLIF(TRIM(CAST(b.team_key AS VARCHAR)), '') IS NOT DISTINCT FROM " \
              "NULLIF(TRIM(CAST(s.team_key AS VARCHAR)), '')"
        set_parts = []
        for field in base_matchup:
            if field in {"is_playoffs", "is_championship", "champion"}:
                set_parts.append(
                    f"{qi(field)}=CASE WHEN CAST(s.{qi(field)} AS INTEGER)=1 "
                    f"THEN 1 ELSE b.{qi(field)} END"
                )
            else:
                set_parts.append(
                    f"{qi(field)}=CASE WHEN b.{qi(field)} IS NULL "
                    f"THEN s.{qi(field)} ELSE b.{qi(field)} END"
                )
        con.execute(
            f"UPDATE public.matchup AS b SET {', '.join(set_parts)} "
            f"FROM _source_matchup s WHERE {key}"
        )
        con.execute(
            f"""
            INSERT INTO public.matchup ({', '.join(qi(c) for c in base_matchup)})
            SELECT {', '.join('s.' + qi(c) for c in base_matchup)}
            FROM _source_matchup s
            WHERE NOT EXISTS (SELECT 1 FROM public.matchup b WHERE {key})
            """
        )

        after_player_rows = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
        after_matchup_rows = con.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0]
        if after_player_rows != before_player_rows:
            raise SystemExit(f"player row count changed: {before_player_rows} -> {after_player_rows}")
        if columns(con, "public.player_fantasy") != PLAYER_COLUMNS:
            raise SystemExit("player schema changed")
        if columns(con, "public.matchup") != base_matchup:
            raise SystemExit("matchup schema changed")
        forbidden = {"loss", "tie", "opponent_points", "is_championship", "is_active", "is_playoffs_bf", "made_po_bf", "made_po"}
        if forbidden & set(PLAYER_COLUMNS):
            raise SystemExit("forbidden player column in canonical schema")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise

    report = {
        "in_place": True,
        "read_only_source": True,
        "player_rows": int(before_player_rows),
        "matchup_rows_before": int(before_matchup_rows),
        "matchup_rows_after": int(after_matchup_rows),
        "matched_player_rows": int(con.execute("SELECT COUNT(*) FROM _player_matches WHERE matches=1").fetchone()[0]),
        "improvements_by_field": {k: int(v) for k, v in improvements.items()},
        "canonical_player_columns": PLAYER_COLUMNS,
        "canonical_matchup_columns": base_matchup,
        "schema_unchanged": True,
        "new_lineage": False,
        "ops_untouched": True,
    }
    args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    con.close()


if __name__ == "__main__":
    main()
