"""Combine and validate targeted source matchup rescue artifacts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


KEY = ("db_name", "target_year", "year", "week")


def _entity_key_sql(columns: set[str]) -> str:
    """Return a stable per-team key across platform sidecars.

    Sleeper does not provide ``franchise_id`` in this rescue frame, while MFL
    and Fleaflicker do.  Partitioning on a nullable franchise ID collapses all
    Sleeper teams in a week into one row.  Prefer the native franchise ID and
    fall back to the native team key, then manager as a last resort.
    """
    if "franchise_id" not in columns and "team_key" not in columns and "manager" not in columns:
        raise SystemExit("sidecar missing an entity key: expected franchise_id, team_key, or manager")
    parts = []
    if "franchise_id" in columns:
        parts.append("NULLIF(CAST(franchise_id AS VARCHAR), '')")
    if "team_key" in columns:
        parts.append("CONCAT('team:', CAST(team_key AS VARCHAR))")
    if "manager" in columns:
        parts.append("CONCAT('manager:', CAST(manager AS VARCHAR))")
    return "COALESCE(" + ", ".join(parts) + ")"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--status-out", type=Path, required=True)
    args = ap.parse_args()
    statuses = []
    for path in sorted(args.input_dir.rglob("*.jsonl")):
        statuses.extend(json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if not statuses:
        raise SystemExit("no source rescue status files found")
    bad = [r for r in statuses if r.get("status") not in (
        "done",
        "done_with_source_no_opponent_weeks",
        "done_with_source_no_games_weeks",
        "source_returned_no_matchups",
        "source_returned_no_target_weeks",
        "source_no_opponent_format",
        "source_no_games_weeks",
        "source_ended_before_target_weeks",
        "source_outside_target_weeks",
        "source_fetch_failed",
        "source_returned_partial_target_weeks",
        "missing_historical_source_id",
    )]
    if bad:
        raise SystemExit(f"source rescue contains {len(bad)} failed targets; first={bad[:3]}")
    parquet = sorted(args.input_dir.rglob("*.parquet"))
    if not parquet:
        raise SystemExit("no source rescue parquet files found")
    paths = ",".join("'" + str(p.resolve()).replace("'", "''") + "'" for p in parquet)
    con = duckdb.connect()
    parquet_scan = f"read_parquet([{paths}], union_by_name=true)"
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {parquet_scan}").fetchall()}
    missing = set(KEY) - cols
    if missing:
        raise SystemExit(f"sidecar missing key columns: {sorted(missing)}")
    entity_key = _entity_key_sql(cols)
    partition_key = ",".join((*KEY, entity_key))
    count, distinct, bad_years = con.execute(f"""
      SELECT COUNT(*), COUNT(DISTINCT ({partition_key})),
             COUNT(*) FILTER (WHERE CAST(target_year AS INTEGER) <> CAST(year AS INTEGER))
      FROM {parquet_scan}
    """).fetchone()
    # Older inventory manifests were signal-grained and could fetch the same
    # source season more than once with different reason-code payloads. Keep
    # one canonical source row per matchup key; the source values, not the
    # diagnostic reason payload, are the sidecar's fact grain.
    duplicate_rows = int(count - distinct)
    source_query = f"""
      SELECT * EXCLUDE (_source_row_number)
      FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY {partition_key}
          ORDER BY target_reason_codes
        ) AS _source_row_number
        FROM {parquet_scan}
      )
      WHERE _source_row_number = 1
    """
    if bad_years:
        raise SystemExit(f"rows with target/source year mismatch: {bad_years}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM ({source_query}) ORDER BY db_name,target_year,week,franchise_id) TO '{str(args.out.resolve()).replace(chr(39),chr(39)*2)}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    # Deduplicate target statuses as well; one league-season can carry several
    # missing-signal reasons but is fetched only once.
    status_by_target = {}
    status_priority = {
        "done": 100,
        "done_with_source_no_opponent_weeks": 90,
        "done_with_source_no_games_weeks": 90,
        "source_no_opponent_format": 80,
        "source_no_games_weeks": 80,
        "source_returned_no_matchups": 70,
        "source_returned_no_target_weeks": 70,
        "source_outside_target_weeks": 70,
        "source_ended_before_target_weeks": 70,
        "missing_historical_source_id": 60,
        "source_returned_partial_target_weeks": 20,
        "source_fetch_failed": 10,
    }
    for row in statuses:
        key = (row.get("db_name"), row.get("year"))
        prior = status_by_target.get(key)
        if prior is None or status_priority.get(row.get("status"), 0) > status_priority.get(prior.get("status"), 0):
            status_by_target[key] = row
    statuses = list(status_by_target.values())
    summary = {
        "target_status_rows": len(statuses),
        "done_targets": sum(r.get("status") == "done" for r in statuses),
        "no_matchup_targets": sum(r.get("status") in (
            "source_returned_no_matchups",
            "source_returned_no_target_weeks",
            "source_ended_before_target_weeks",
            "source_outside_target_weeks",
        ) for r in statuses),
        "source_ended_before_target_weeks": sum(
            r.get("status") == "source_ended_before_target_weeks" for r in statuses
        ),
        "source_outside_target_weeks": sum(
            r.get("status") == "source_outside_target_weeks" for r in statuses
        ),
        "failed_targets_for_retry": sum(r.get("status") == "source_fetch_failed" for r in statuses),
        "partial_targets_for_retry": sum(r.get("status") == "source_returned_partial_target_weeks" for r in statuses),
        "sidecar_rows": int(distinct),
        "distinct_sidecar_keys": int(distinct),
        "duplicate_sidecar_rows_removed": duplicate_rows,
        "retry_targets": [
            {
                "db_name": r.get("db_name"),
                "platform": r.get("platform"),
                "year": r.get("year"),
                "status": r.get("status"),
                "weeks": r.get("weeks"),
                "missing_target_weeks": r.get("missing_target_weeks"),
                "source_candidate_ids": r.get("source_candidate_ids"),
                "source_id": r.get("source_id"),
            }
            for r in statuses
            if r.get("status") in {"source_fetch_failed", "source_returned_partial_target_weeks"}
        ],
    }
    args.status_out.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
