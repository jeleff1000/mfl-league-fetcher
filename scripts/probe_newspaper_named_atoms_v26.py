#!/usr/bin/env python
"""Probe whether named newspaper atoms already appear as v26 player-week stats."""

from __future__ import annotations

import glob
import json
import re
import argparse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


NEWSPAPER_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
RELEASE_ROOT = Path(r"D:\league-history-data\nfl\releases")
DEFAULT_OUT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\v26_named_atom_probes")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(value: Any) -> str:
    return "" if value is None else str(value)


def parse_json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    text = clean(value)
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def latest_v26() -> Path:
    files = sorted(
        glob.glob(str(RELEASE_ROOT / "*_v26" / "tables" / "nfl_player_stats_all.parquet")),
        key=lambda p: Path(p).stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError("No v26 parquet found")
    return Path(files[0])


def latest_package_run(con: duckdb.DuckDBPyConnection) -> str:
    return clean(con.execute(
        """
        SELECT package_run_id
        FROM newspaper_review.llm_promotion_package_run
        ORDER BY created_at_utc DESC, package_run_id DESC
        LIMIT 1
        """
    ).fetchone()[0])


def extract_names(package: dict[str, Any]) -> list[str]:
    proposed = parse_json_obj(package.get("proposed_fields_json"))
    names = []
    for key in [
        "scoring_player_raw",
        "passer_raw",
        "receiver_raw",
        "primary_player_raw",
        "secondary_player_raw",
        "player_raw",
        "raw_player_name",
    ]:
        value = clean(proposed.get(key)).strip()
        if value:
            names.append(value)
    return names


def year_from_package(package: dict[str, Any]) -> int | None:
    proposed = parse_json_obj(package.get("proposed_fields_json"))
    year_text = clean(proposed.get("year")).strip()
    if year_text:
        try:
            return int(float(year_text))
        except ValueError:
            pass
    boxscore_id = clean(package.get("boxscore_id"))
    match = re.match(r"(\d{4})", boxscore_id)
    return int(match.group(1)) if match else None


def name_patterns(name: str) -> list[str]:
    name = " ".join(name.split())
    patterns = [name]
    parts = [part for part in re.split(r"\s+", name) if part]
    if len(parts) > 1:
        patterns.append(parts[-1])
    return list(dict.fromkeys(patterns))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--newspaper-db", type=Path, default=NEWSPAPER_DB)
    parser.add_argument("--release-root", type=Path, default=RELEASE_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="newspaper_named_atoms_v26_probe")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    global NEWSPAPER_DB, RELEASE_ROOT
    NEWSPAPER_DB = args.newspaper_db
    RELEASE_ROOT = args.release_root

    ncon = duckdb.connect(str(NEWSPAPER_DB), read_only=True)
    try:
        package_run_id = latest_package_run(ncon)
        packages = ncon.execute(
            """
            SELECT promotion_package_id, target_table, boxscore_id, confidence_bar,
                   max_confidence_score, proposed_fields_json, package_status
            FROM newspaper_review.llm_promotion_package
            WHERE package_run_id = ?
              AND target_table IN ('scoring_event','play_by_play_event','lineup_participation','player_identity_candidate')
            ORDER BY target_table, boxscore_id
            """,
            [package_run_id],
        ).fetchall()
        fields = [item[0] for item in ncon.description]
        package_rows = [dict(zip(fields, row)) for row in packages]
    finally:
        ncon.close()

    v26_path = latest_v26()
    rel = str(v26_path).replace("\\", "/").replace("'", "''")
    con = duckdb.connect()
    con.execute(f"CREATE VIEW st AS SELECT * FROM read_parquet('{rel}')")
    cols = {row[1] for row in con.execute("PRAGMA table_info(st)").fetchall()}
    useful_cols = [
        col for col in [
            "player", "NFL_player_id", "year", "week", "player_week", "nfl_team", "opponent_nfl_team",
            "position", "rushing_yards", "rushing_tds", "attempts", "completions",
            "passing_yards", "passing_tds", "passing_interceptions", "receptions",
            "receiving_yards", "receiving_tds", "punt_return_tds", "kick_return_tds",
            "def_tds", "special_teams_tds", "fg_made", "pat_made", "data_source",
        ]
        if col in cols
    ]
    if "player" not in cols or "year" not in cols:
        print(json.dumps({"error": "v26 lacks player/year columns", "v26_path": str(v26_path), "columns": sorted(cols)[:80]}, indent=2))
        return 1

    probes = []
    for package in package_rows:
        year = year_from_package(package)
        if not year:
            continue
        for name in extract_names(package):
            probes.append({
                "package": package,
                "year": year,
                "name": name,
                "patterns": name_patterns(name),
            })

    results = []
    for probe in probes:
        matches = []
        seen = set()
        for pattern in probe["patterns"]:
            rows = con.execute(
                f"""
                SELECT {', '.join(useful_cols)}
                FROM st
                WHERE year = ?
                  AND player ILIKE ?
                ORDER BY player, week
                LIMIT 12
                """,
                [probe["year"], f"%{pattern}%"],
            ).fetchall()
            for row in rows:
                payload = dict(zip(useful_cols, row))
                key = json.dumps(payload, sort_keys=True, default=str)
                if key not in seen:
                    seen.add(key)
                    matches.append(payload)
        package = probe["package"]
        proposed = parse_json_obj(package.get("proposed_fields_json"))
        nonzero_stat_rows = 0
        for row in matches:
            for stat in [
                "rushing_yards", "rushing_tds", "passing_yards", "passing_tds",
                "receptions", "receiving_yards", "receiving_tds",
                "punt_return_tds", "kick_return_tds", "def_tds", "special_teams_tds",
            ]:
                try:
                    if float(clean(row.get(stat)) or 0) != 0:
                        nonzero_stat_rows += 1
                        break
                except ValueError:
                    continue
        results.append({
            "target_table": clean(package.get("target_table")),
            "boxscore_id": clean(package.get("boxscore_id")),
            "confidence": clean(package.get("max_confidence_score")),
            "package_status": clean(package.get("package_status")),
            "newspaper_name": probe["name"],
            "year": probe["year"],
            "newspaper_fields": proposed,
            "v26_match_count": len(matches),
            "v26_nonzero_stat_match_rows": nonzero_stat_rows,
            "v26_matches": matches[:6],
        })

    summary = {
        "created_at_utc": iso_now(),
        "probe_run_id": f"{stamp()}_{args.label}",
        "v26_path": str(v26_path),
        "package_run_id": package_run_id,
        "probe_count": len(results),
        "target_table_counts": dict(Counter(row["target_table"] for row in results)),
        "name_match_count": sum(1 for row in results if row["v26_match_count"] > 0),
        "name_nonzero_stat_match_count": sum(1 for row in results if row["v26_nonzero_stat_match_rows"] > 0),
        "no_name_match_count": sum(1 for row in results if row["v26_match_count"] == 0),
    }
    out_dir = args.out_root / summary["probe_run_id"]
    out_dir.mkdir(parents=True, exist_ok=True)
    payload = {"summary": summary, "results": results}
    (out_dir / "named_atom_v26_probe.json").write_text(json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8")
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({**summary, "output_dir": str(out_dir)}, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
