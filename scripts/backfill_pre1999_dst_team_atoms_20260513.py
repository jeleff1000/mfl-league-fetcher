#!/usr/bin/env python3
"""Backfill safe pre-1999 DST team atoms from individual player-week rows.

This intentionally fills gaps only.  If a DST row already has a nonzero value
that disagrees with the player-row team sum, the row is reported as a conflict
and left untouched.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402
from multi_league.core.readers.fly_reader import FlyReader  # noqa: E402


ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
CATALOG_ROOT = ORGANIZED_ROOT / "_catalog"
SUPER_TABLE = "nfl_historical.nfl_player_stats_all"
CONFIRM_TOKEN = "APPLY_DST_TEAM_ATOMS"

SAFE_ATOMS = [
    "def_fumbles_forced",
    "def_tackles_for_loss",
    "def_blk_kick",
    "def_int_ret_td",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "special_teams_tackles_solo",
]


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())


def pos_expr(alias: str) -> str:
    return f"UPPER(TRIM(CAST(COALESCE({alias}.nfl_position, {alias}.position, '') AS VARCHAR)))"


def season_expr(alias: str) -> str:
    return f"COALESCE(NULLIF(CAST({alias}.season_type AS VARCHAR), ''), 'REG')"


def true_dst_expr(alias: str) -> str:
    return (
        f"(COALESCE(CAST({alias}.player_week AS VARCHAR), '') LIKE 'DEF-%' "
        f"OR COALESCE(CAST({alias}.player AS VARCHAR), '') LIKE '% DST')"
    )


def atom_expr(alias: str, col: str) -> str:
    return f"COALESCE(TRY_CAST({alias}.{q_ident(col)} AS DOUBLE), 0.0)"


def build_stage_sql(stage_table: str, start_year: int, end_year: int) -> str:
    aggregate_cols = ",\n        ".join(f"SUM({atom_expr('s', col)}) AS {q_ident('new_' + col)}" for col in SAFE_ATOMS)
    select_cols = []
    flags = []
    for col in SAFE_ATOMS:
        old_col = f"old_{col}"
        new_col = f"new_{col}"
        fill_col = f"fill_{col}"
        conflict_col = f"conflict_{col}"
        select_cols.extend(
            [
                f"{atom_expr('d', col)} AS {q_ident(old_col)}",
                f"i.{q_ident(new_col)} AS {q_ident(new_col)}",
                (
                    f"CASE WHEN ABS({atom_expr('d', col)}) < 0.000001 "
                    f"AND ABS(i.{q_ident(new_col)}) >= 0.000001 THEN 1 ELSE 0 END AS {q_ident(fill_col)}"
                ),
                (
                    f"CASE WHEN ABS({atom_expr('d', col)}) >= 0.000001 "
                    f"AND ABS({atom_expr('d', col)} - i.{q_ident(new_col)}) >= 0.000001 "
                    f"THEN 1 ELSE 0 END AS {q_ident(conflict_col)}"
                ),
            ]
        )
        flags.extend([fill_col, conflict_col])

    select_sql = ",\n      ".join(select_cols)
    fill_any = " + ".join(q_ident(f"fill_{col}") for col in SAFE_ATOMS)
    conflict_any = " + ".join(q_ident(f"conflict_{col}") for col in SAFE_ATOMS)
    where_any = " OR ".join(
        [
            (f"(ABS({atom_expr('d', col)}) < 0.000001 " f"AND ABS(i.{q_ident('new_' + col)}) >= 0.000001)")
            for col in SAFE_ATOMS
        ]
        + [
            (
                f"(ABS({atom_expr('d', col)}) >= 0.000001 "
                f"AND ABS({atom_expr('d', col)} - i.{q_ident('new_' + col)}) >= 0.000001)"
            )
            for col in SAFE_ATOMS
        ]
    )

    return f"""
    CREATE OR REPLACE TABLE {stage_table} AS
    WITH individual_team AS (
      SELECT
        CAST(s.year AS INTEGER) AS year,
        CAST(s.week AS INTEGER) AS week,
        {season_expr('s')} AS season_type,
        s.nfl_team,
        s.opponent_nfl_team,
        {aggregate_cols}
      FROM {SUPER_TABLE} AS s
      WHERE s.year BETWEEN {int(start_year)} AND {int(end_year)}
        AND s.nfl_team IS NOT NULL
        AND s.opponent_nfl_team IS NOT NULL
        AND NOT {true_dst_expr('s')}
      GROUP BY
        CAST(s.year AS INTEGER),
        CAST(s.week AS INTEGER),
        {season_expr('s')},
        s.nfl_team,
        s.opponent_nfl_team
    ),
    dst AS (
      SELECT
        d.rowid AS dst_rowid,
        d.player_week,
        d.player,
        CAST(d.year AS INTEGER) AS year,
        CAST(d.week AS INTEGER) AS week,
        {season_expr('d')} AS season_type,
        d.nfl_team,
        d.opponent_nfl_team,
        {", ".join(f"d.{q_ident(col)}" for col in SAFE_ATOMS)}
      FROM {SUPER_TABLE} AS d
      WHERE d.year BETWEEN {int(start_year)} AND {int(end_year)}
        AND d.player_week IS NOT NULL
        AND d.nfl_team IS NOT NULL
        AND d.opponent_nfl_team IS NOT NULL
        AND {true_dst_expr('d')}
    ),
    joined AS (
      SELECT
        d.dst_rowid,
        d.player_week,
        d.player,
        d.year,
        d.week,
        d.season_type,
        d.nfl_team,
        d.opponent_nfl_team,
        {select_sql}
      FROM dst AS d
      INNER JOIN individual_team AS i
        ON i.year = d.year
       AND i.week = d.week
       AND i.season_type = d.season_type
       AND i.nfl_team = d.nfl_team
       AND i.opponent_nfl_team = d.opponent_nfl_team
      WHERE {where_any}
    )
    SELECT
      *,
      ({fill_any}) AS fill_any,
      ({conflict_any}) AS conflict_any
    FROM joined
    """


def summary_sql(stage_table: str) -> str:
    parts = []
    for col in SAFE_ATOMS:
        parts.append(
            f"""
            SELECT
              '{col}' AS atom,
              COUNT(*) FILTER (WHERE {q_ident('fill_' + col)} = 1) AS fill_rows,
              ROUND(SUM(CASE WHEN {q_ident('fill_' + col)} = 1 THEN {q_ident('new_' + col)} ELSE 0 END), 4) AS fill_new_sum,
              COUNT(*) FILTER (WHERE {q_ident('conflict_' + col)} = 1) AS conflict_rows,
              ROUND(SUM(CASE WHEN {q_ident('conflict_' + col)} = 1 THEN {q_ident('old_' + col)} ELSE 0 END), 4) AS conflict_old_sum,
              ROUND(SUM(CASE WHEN {q_ident('conflict_' + col)} = 1 THEN {q_ident('new_' + col)} ELSE 0 END), 4) AS conflict_new_sum
            FROM {stage_table}
            """
        )
    return "\nUNION ALL\n".join(parts) + "\nORDER BY atom"


def totals_sql(stage_table: str) -> str:
    return f"""
    SELECT
      COUNT(*) AS stage_rows,
      COUNT(*) FILTER (WHERE fill_any > 0) AS rows_with_any_fill,
      COUNT(*) FILTER (WHERE conflict_any > 0) AS rows_with_any_conflict,
      SUM(fill_any) AS fill_cells,
      SUM(conflict_any) AS conflict_cells
    FROM {stage_table}
    """


def update_sql(stage_table: str) -> str:
    assignments = []
    for col in SAFE_ATOMS:
        assignments.append(
            f"{q_ident(col)} = CASE WHEN st.{q_ident('fill_' + col)} = 1 "
            f"THEN st.{q_ident('new_' + col)} ELSE s.{q_ident(col)} END"
        )
    return f"""
    UPDATE {SUPER_TABLE} AS s
    SET {", ".join(assignments)}
    FROM {stage_table} AS st
    WHERE s.rowid = st.dst_rowid
      AND st.fill_any > 0
    """


def backup_sql(backup_table: str, stage_table: str) -> str:
    cols = ",\n      ".join(f"s.{q_ident(col)}" for col in SAFE_ATOMS)
    return f"""
    CREATE OR REPLACE TABLE {backup_table} AS
    SELECT
      s.rowid AS dst_rowid,
      s.player_week,
      s.player,
      s.year,
      s.week,
      s.season_type,
      s.nfl_team,
      s.opponent_nfl_team,
      {cols},
      CURRENT_TIMESTAMP AS backed_up_at
    FROM {SUPER_TABLE} AS s
    INNER JOIN {stage_table} AS st
      ON st.dst_rowid = s.rowid
    WHERE st.fill_any > 0
    """


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-year", type=int, default=1978)
    parser.add_argument("--end-year", type=int, default=1998)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", default="")
    parser.add_argument("--timeout-seconds", type=int, default=600)
    parser.add_argument("--output-dir", type=Path, default=None)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.execute and args.confirm != CONFIRM_TOKEN:
        raise SystemExit(f"Live updates require --confirm {CONFIRM_TOKEN}")

    load_env()
    FlyReader.TIMEOUT_SECONDS = args.timeout_seconds
    FlyWriter.TIMEOUT_SECONDS = args.timeout_seconds

    stamp = now_stamp()
    stage_table = f"public.dst_team_atom_stage_pre1999_{stamp.lower()}"
    backup_table = f"public.dst_team_atom_backup_pre1999_{stamp.lower()}"
    output_dir = args.output_dir or (CATALOG_ROOT / f"dst_team_atom_backfill_pre1999_{stamp}")
    output_dir.mkdir(parents=True, exist_ok=True)

    reader = FlyReader()
    writer = FlyWriter()

    writer.execute(build_stage_sql(stage_table, args.start_year, args.end_year), database="___ops")
    summary = reader.query_df(summary_sql(stage_table), database="___ops")
    totals = reader.query_df(totals_sql(stage_table), database="___ops")
    fills = reader.query_df(
        f"""
        SELECT *
        FROM {stage_table}
        WHERE fill_any > 0
        ORDER BY year, week, nfl_team, opponent_nfl_team
        LIMIT 200
        """,
        database="___ops",
    )
    conflicts = reader.query_df(
        f"""
        SELECT *
        FROM {stage_table}
        WHERE conflict_any > 0
        ORDER BY year, week, nfl_team, opponent_nfl_team
        LIMIT 200
        """,
        database="___ops",
    )

    summary.to_csv(output_dir / "summary_before.csv", index=False)
    totals.to_csv(output_dir / "totals_before.csv", index=False)
    fills.to_csv(output_dir / "sample_fillable_rows.csv", index=False)
    conflicts.to_csv(output_dir / "sample_conflict_rows.csv", index=False)

    manifest = {
        "stamp": stamp,
        "start_year": args.start_year,
        "end_year": args.end_year,
        "safe_atoms": SAFE_ATOMS,
        "stage_table": f"___ops.{stage_table}",
        "backup_table": f"___ops.{backup_table}" if args.execute else None,
        "execute": bool(args.execute),
        "output_dir": str(output_dir),
        "totals_before": totals.to_dict(orient="records"),
        "summary_before": summary.to_dict(orient="records"),
        "policy": "fill only when current DST atom is null/zero and strict team/opponent player rows derive nonzero; nonzero disagreements are conflicts only",
    }

    if args.execute:
        writer.execute(backup_sql(backup_table, stage_table), database="___ops")
        writer.execute(update_sql(stage_table), database="___ops")

        verify_stage = f"public.dst_team_atom_verify_pre1999_{stamp.lower()}"
        writer.execute(build_stage_sql(verify_stage, args.start_year, args.end_year), database="___ops")
        verify_summary = reader.query_df(summary_sql(verify_stage), database="___ops")
        verify_totals = reader.query_df(totals_sql(verify_stage), database="___ops")
        verify_summary.to_csv(output_dir / "summary_after.csv", index=False)
        verify_totals.to_csv(output_dir / "totals_after.csv", index=False)
        manifest["verify_stage_table"] = f"___ops.{verify_stage}"
        manifest["summary_after"] = verify_summary.to_dict(orient="records")
        manifest["totals_after"] = verify_totals.to_dict(orient="records")

    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    (output_dir / "README.md").write_text(
        "\n".join(
            [
                "# Pre-1999 DST Team Atom Backfill",
                "",
                "This artifact fills safe DST team-level raw atoms from already-live individual player-week rows.",
                "",
                f"- Executed: {bool(args.execute)}",
                f"- Stage table: `___ops.{stage_table}`",
                f"- Backup table: `___ops.{backup_table}`"
                if args.execute
                else "- Backup table: not created in dry-run",
                f"- Years: {args.start_year}-{args.end_year}",
                f"- Atoms: {', '.join(SAFE_ATOMS)}",
                "",
                "Policy: strict game key match (`year`, `week`, `season_type`, `nfl_team`, `opponent_nfl_team`).",
                "Only null/zero DST cells are filled. Nonzero disagreements are logged as conflicts and not overwritten.",
                "",
            ]
        ),
        encoding="utf-8",
    )

    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
