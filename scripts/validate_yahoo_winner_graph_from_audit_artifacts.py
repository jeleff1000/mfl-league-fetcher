#!/usr/bin/env python3
"""Validate the DDL bracket tracer against cached Yahoo scoreboard audits.

The raw audit artifacts contain Yahoo scoreboard rows with ``winner_team_key``.
This script loads those rows into a minimal matchup-shaped DuckDB table and
runs the production bracket tracer. It uses Yahoo ``team_key`` as the per-year
``franchise_id`` so the check validates bracket logic without depending on
manager names or live franchise mappings.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
if str(PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(PIPELINE_ROOT))

from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (  # noqa: E402
    trace_championship_bracket_sql,
)


def _int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _next_power_of_two(value: int) -> int:
    out = 1
    while out < value:
        out *= 2
    return out


def _infer_bye_teams(playoff_teams: int) -> int:
    return max(0, _next_power_of_two(playoff_teams) - playoff_teams)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _parse_xml(path: Path) -> ET.Element:
    xml_text = path.read_text(encoding="utf-8")
    xml_text = re.sub(r' xmlns="[^"]+"', "", xml_text, count=1)
    return ET.fromstring(xml_text)


def _text_at(el: ET.Element, path: str) -> str | None:
    found = el.find(path)
    if found is None or found.text is None:
        return None
    return found.text.strip()


def _create_matchup_table(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            team_key VARCHAR,
            winner_team_key VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            champion INTEGER,
            is_championship BOOLEAN,
            final_playoff_seed INTEGER,
            playoff_round VARCHAR,
            placement_rank INTEGER
        )
        """
    )


def _load_scoreboard_rows(conn: duckdb.DuckDBPyConnection, rows: list[dict[str, str]]) -> int:
    inserts: list[tuple[Any, ...]] = []
    for row in rows:
        year = _int(row.get("year"))
        week = _int(row.get("week"))
        team_key = (row.get("team_key") or "").strip()
        opponent_team_key = (row.get("opponent_team_key") or "").strip()
        if year is None or week is None or not team_key or not opponent_team_key:
            continue

        winner_team_key = (row.get("winner_team_key") or "").strip() or None
        team_points = _float(row.get("team_points"))
        opponent_points = _float(row.get("opponent_points"))
        won = 1 if winner_team_key and team_key == winner_team_key else 0
        lost = 1 if winner_team_key and opponent_team_key == winner_team_key else 0
        tied = 1 if winner_team_key is None and team_points == opponent_points else 0
        inserts.append(
            (
                year,
                week,
                team_key,
                opponent_team_key,
                team_key,
                winner_team_key,
                team_points,
                opponent_points,
                won,
                lost,
                tied,
                0,
                0,
                0,
                False,
                None,
                None,
                None,
            )
        )

    if inserts:
        conn.executemany(
            "INSERT INTO matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            inserts,
        )
    return len(inserts)


def _insert_matchup_row(
    inserts: list[tuple[Any, ...]],
    *,
    year: int,
    week: int,
    team_key: str,
    opponent_team_key: str,
    winner_team_key: str | None,
    team_points: float | None,
    opponent_points: float | None,
) -> None:
    won = 1 if winner_team_key and team_key == winner_team_key else 0
    lost = 1 if winner_team_key and opponent_team_key == winner_team_key else 0
    tied = 1 if winner_team_key is None and team_points == opponent_points else 0
    inserts.append(
        (
            year,
            week,
            team_key,
            opponent_team_key,
            team_key,
            winner_team_key,
            team_points,
            opponent_points,
            won,
            lost,
            tied,
            0,
            0,
            0,
            False,
            None,
            None,
            None,
        )
    )


def _trace_playoff_team_keys(audit_row: dict[str, Any]) -> list[str]:
    seen: set[str] = set()
    keys: list[str] = []
    for round_entry in audit_row.get("trace") or []:
        for matchup in round_entry.get("matchups") or []:
            for key in matchup.get("team_keys") or []:
                if key and key not in seen:
                    seen.add(key)
                    keys.append(key)
    return keys


def _load_playoff_xml_with_synthetic_seeds(
    conn: duckdb.DuckDBPyConnection,
    artifact_dir: Path,
    audit_rows: list[dict[str, Any]],
) -> tuple[int, set[int]]:
    inserts: list[tuple[Any, ...]] = []
    loaded_years: set[int] = set()
    for audit_row in audit_rows:
        year = _int(audit_row.get("year"))
        playoff_start = _int(audit_row.get("playoff_start_week_xml"))
        playoff_teams = _int(audit_row.get("playoff_teams_xml"))
        if year is None or playoff_start is None or playoff_teams is None:
            continue

        year_dir = artifact_dir / str(year)
        xml_paths = sorted(year_dir.glob("scoreboard_week_*.xml"))
        if not xml_paths:
            continue

        playoff_team_keys = _trace_playoff_team_keys(audit_row)
        if len(playoff_team_keys) < 2:
            continue

        all_seen: set[str] = set(playoff_team_keys)
        playoff_rows: list[tuple[int, str, str, str | None, float | None, float | None]] = []
        for xml_path in xml_paths:
            week = _int(xml_path.stem.rsplit("_", 1)[-1])
            if week is None:
                continue
            root = _parse_xml(xml_path)
            for matchup_el in root.findall(".//scoreboard/matchups/matchup"):
                winner_team_key = _text_at(matchup_el, "winner_team_key")
                teams = []
                for team_el in matchup_el.findall("./teams/team"):
                    teams.append(
                        {
                            "team_key": _text_at(team_el, "team_key") or "",
                            "points": _float(_text_at(team_el, "team_points/total")),
                        }
                    )
                if len(teams) != 2:
                    continue
                a, b = teams
                if not a["team_key"] or not b["team_key"]:
                    continue
                all_seen.add(a["team_key"])
                all_seen.add(b["team_key"])
                playoff_rows.append((week, a["team_key"], b["team_key"], winner_team_key, a["points"], b["points"]))
                playoff_rows.append((week, b["team_key"], a["team_key"], winner_team_key, b["points"], a["points"]))

        if not playoff_rows:
            continue

        seed_keys = playoff_team_keys[:]
        for key in sorted(all_seen):
            if key not in seed_keys:
                seed_keys.append(key)

        seed_week = max(1, playoff_start - 1)
        for idx, team_key in enumerate(seed_keys, start=1):
            opponent_key = f"synthetic_seed_opp_{year}_{idx}"
            _insert_matchup_row(
                inserts,
                year=year,
                week=seed_week,
                team_key=team_key,
                opponent_team_key=opponent_key,
                winner_team_key=team_key,
                team_points=float(10000 - idx),
                opponent_points=0.0,
            )

        for week, team_key, opponent_key, winner_key, points, opp_points in playoff_rows:
            _insert_matchup_row(
                inserts,
                year=year,
                week=week,
                team_key=team_key,
                opponent_team_key=opponent_key,
                winner_team_key=winner_key,
                team_points=points,
                opponent_points=opp_points,
            )
        loaded_years.add(year)

    if inserts:
        conn.executemany(
            "INSERT INTO matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            inserts,
        )
    return len(inserts), loaded_years


def validate_artifact_dir(path: Path, years: set[int] | None = None) -> dict[str, Any]:
    audit_path = path / "champion_audit.json"
    scoreboard_path = path / "scoreboard_team_rows.csv"
    if not audit_path.exists():
        raise FileNotFoundError(f"{path} must contain champion_audit.json")

    audit_rows = json.loads(audit_path.read_text(encoding="utf-8"))
    if years is not None:
        audit_rows = [row for row in audit_rows if _int(row.get("year")) in years]

    conn = duckdb.connect(":memory:")
    _create_matchup_table(conn)
    loaded_years: set[int] | None = None
    source = "scoreboard_team_rows_csv"
    if scoreboard_path.exists():
        loaded_rows = _load_scoreboard_rows(conn, _read_csv(scoreboard_path))
    else:
        source = "playoff_xml_with_synthetic_seeds"
        loaded_rows, loaded_years = _load_playoff_xml_with_synthetic_seeds(conn, path, audit_rows)

    results: list[dict[str, Any]] = []
    for audit_row in audit_rows:
        year = _int(audit_row.get("year"))
        playoff_teams = _int(audit_row.get("playoff_teams_xml"))
        playoff_start = _int(audit_row.get("playoff_start_week_xml"))
        end_week = _int(audit_row.get("end_week_xml"))
        expected = (audit_row.get("yahoo_champion_team_key") or "").strip()
        if year is None or not expected:
            continue
        if loaded_years is not None and year not in loaded_years:
            results.append(
                {
                    "year": year,
                    "status": "skipped",
                    "expected": expected,
                    "actual": None,
                    "reason": "missing_playoff_xml_or_trace",
                }
            )
            continue
        if playoff_teams is None or playoff_start is None or end_week is None or end_week < playoff_start:
            results.append(
                {
                    "year": year,
                    "status": "skipped",
                    "expected": expected,
                    "actual": None,
                    "reason": "missing_or_invalid_playoff_settings",
                }
            )
            continue

        team_count = conn.execute(
            "SELECT COUNT(DISTINCT team_key) FROM matchup WHERE year = ? AND week < ?",
            [year, playoff_start],
        ).fetchone()[0]
        result = trace_championship_bracket_sql(
            conn,
            year,
            {
                "playoff_teams": playoff_teams,
                "bye_teams": _infer_bye_teams(playoff_teams),
                "playoff_start_week": playoff_start,
                "end_week": end_week,
                "num_teams": int(team_count or playoff_teams),
            },
            table="matchup",
            write_back=True,
        )
        actual = result.get("champion")
        results.append(
            {
                "year": year,
                "status": "match" if actual == expected else "mismatch",
                "expected": expected,
                "actual": actual,
                "reason": None if actual == expected else "tracer_champion_differs_from_yahoo_graph",
            }
        )

    summary = {
        "artifact_dir": str(path),
        "source": source,
        "loaded_scoreboard_rows": loaded_rows,
        "checked": sum(1 for row in results if row["status"] != "skipped"),
        "matched": sum(1 for row in results if row["status"] == "match"),
        "mismatched": sum(1 for row in results if row["status"] == "mismatch"),
        "skipped": sum(1 for row in results if row["status"] == "skipped"),
        "results": results,
    }
    conn.close()
    return summary


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_dirs", nargs="+", type=Path)
    parser.add_argument("--year", action="append", type=int, dest="years")
    parser.add_argument("--json", action="store_true", help="Emit full JSON result")
    args = parser.parse_args()

    years = set(args.years) if args.years else None
    summaries = [validate_artifact_dir(path, years) for path in args.artifact_dirs]
    if args.json:
        print(json.dumps(summaries, indent=2, sort_keys=True))
    else:
        for summary in summaries:
            print(
                f"{summary['artifact_dir']}: "
                f"{summary['matched']}/{summary['checked']} matched, "
                f"{summary['mismatched']} mismatched, {summary['skipped']} skipped "
                f"({summary['loaded_scoreboard_rows']} rows)"
            )
            for row in summary["results"]:
                if row["status"] != "match":
                    print(
                        f"  {row['year']}: {row['status']} "
                        f"expected={row['expected']} actual={row['actual']} reason={row['reason']}"
                    )

    return 1 if any(summary["mismatched"] for summary in summaries) else 0


if __name__ == "__main__":
    raise SystemExit(main())
