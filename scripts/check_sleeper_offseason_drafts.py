#!/usr/bin/env python3
"""
Check Sleeper leagues for offseason draft changes and optionally update them.

Default behavior is read-only: compare the latest complete Sleeper draft(s) for
each league's next season against ___leagues.public.draft. Use --execute to
replace changed draft rows and rebuild draft/homepage aggregates through the
draft-only updater.

Examples:
  python scripts/check_sleeper_offseason_drafts.py --db the_infirmary
  python scripts/check_sleeper_offseason_drafts.py --execute
  python scripts/check_sleeper_offseason_drafts.py --draft-year 2026 --execute
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from update_sleeper_offseason_draft import (  # noqa: E402
    FlyDuckDBConnection,
    OffseasonDraftPlan,
    fetch_sleeper_draft_dataframe,
    load_dotenv,
    rebuild_draft_aggregates,
    rebuild_homepage_tables,
    replace_draft_year,
    resolve_plan,
    run_draft_sql_enrichments,
    select_sleeper_drafts,
    sleeper_api_json,
    sql_literal,
)


@dataclass
class DraftCheckResult:
    db_name: str
    league_name: str | None
    sleeper_league_id: str | None
    draft_year: int | None
    status: str
    api_pick_count: int = 0
    stored_pick_count: int = 0
    api_draft_ids: list[str] | None = None
    stored_draft_ids: list[str] | None = None
    api_fingerprint: str | None = None
    stored_fingerprint: str | None = None
    updated: bool = False
    error: str | None = None


def normalize_token(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in {"", "None", "nan", "<NA>"}:
        return None
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        text = text[:-2]
    return text


def normalize_draft_type(value: Any) -> str | None:
    draft_type = normalize_token(value)
    if draft_type in {"linear", "snake"}:
        return "snake"
    return draft_type


def stable_fingerprint(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def roster_id_from_pick(draft: dict[str, Any], pick: dict[str, Any]) -> str | None:
    roster_id = normalize_token(pick.get("roster_id"))
    if roster_id:
        return roster_id
    draft_slot = pick.get("draft_slot")
    slot_to_roster = draft.get("slot_to_roster_id") or {}
    mapped = slot_to_roster.get(str(draft_slot), slot_to_roster.get(draft_slot))
    return normalize_token(mapped)


def api_draft_snapshot(plan: OffseasonDraftPlan, require_complete: bool) -> tuple[list[dict[str, Any]], list[str]]:
    drafts = sleeper_api_json(f"league/{plan.sleeper_league_id}/drafts")
    matches = [draft for draft in drafts if str(draft.get("season")) == str(plan.draft_year)]
    if require_complete:
        matches = [draft for draft in matches if str(draft.get("status") or "").lower() == "complete"]
    rows: list[dict[str, Any]] = []
    draft_ids: list[str] = []

    for draft in sorted(matches, key=lambda item: str(item.get("draft_id") or "")):
        draft_id = normalize_token(draft.get("draft_id"))
        if not draft_id:
            continue
        draft_ids.append(draft_id)
        picks = sleeper_api_json(f"draft/{draft_id}/picks")
        for pick in sorted(picks or [], key=lambda item: int(item.get("pick_no") or 0)):
            metadata = pick.get("metadata") or {}
            rows.append(
                {
                    "draft_id": draft_id,
                    "draft_type": normalize_draft_type(draft.get("type")),
                    "pick": normalize_token(pick.get("pick_no")),
                    "round": normalize_token(pick.get("round")),
                    "draft_slot": normalize_token(pick.get("draft_slot")),
                    "team_key": roster_id_from_pick(draft, pick),
                    "sleeper_player_id": normalize_token(pick.get("player_id")),
                    "cost": normalize_token(metadata.get("amount")),
                }
            )
    return rows, draft_ids


def stored_draft_snapshot(
    conn: FlyDuckDBConnection, db_name: str, draft_year: int
) -> tuple[list[dict[str, Any]], list[str]]:
    rows_df = conn.execute(
        "SELECT draft_id, draft_type, pick, round, draft_slot, team_key, sleeper_player_id, cost "
        "FROM public.draft "
        f"WHERE db_name = {sql_literal(db_name)} AND year = {int(draft_year)} "
        "ORDER BY draft_id, pick"
    ).fetchdf()
    rows: list[dict[str, Any]] = []
    for raw in rows_df.to_dict("records"):
        row = {key: normalize_token(raw.get(key)) for key in raw}
        row["draft_type"] = normalize_draft_type(row.get("draft_type"))
        rows.append(row)
    draft_ids = sorted({row["draft_id"] for row in rows if row.get("draft_id")})
    return rows, draft_ids


def list_sleeper_db_names(conn: FlyDuckDBConnection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT db_name "
        "FROM public.league_context "
        "WHERE LOWER(COALESCE(platform, '')) = 'sleeper' "
        "  AND league_id IS NOT NULL "
        "  AND TRIM(CAST(league_id AS VARCHAR)) != '' "
        "ORDER BY db_name"
    ).fetchall()
    return [str(row[0]) for row in rows if row and row[0]]


def check_one(
    conn: FlyDuckDBConnection,
    db_name: str,
    *,
    draft_year: int | None,
    allow_incomplete_draft: bool,
) -> DraftCheckResult:
    plan = resolve_plan(conn, db_name, draft_year=draft_year)
    result = DraftCheckResult(
        db_name=plan.db_name,
        league_name=plan.league_name,
        sleeper_league_id=plan.sleeper_league_id,
        draft_year=plan.draft_year,
        status="unknown",
    )

    try:
        api_rows, api_draft_ids = api_draft_snapshot(plan, require_complete=not allow_incomplete_draft)
    except SystemExit as exc:
        result.status = "no_api_draft"
        result.error = str(exc)
        return result

    stored_rows, stored_draft_ids = stored_draft_snapshot(conn, plan.db_name, plan.draft_year)
    result.api_pick_count = len(api_rows)
    result.stored_pick_count = len(stored_rows)
    result.api_draft_ids = api_draft_ids
    result.stored_draft_ids = stored_draft_ids
    result.api_fingerprint = stable_fingerprint(api_rows)
    result.stored_fingerprint = stable_fingerprint(stored_rows)

    if not api_rows:
        result.status = "no_api_picks"
    elif result.api_fingerprint == result.stored_fingerprint:
        result.status = "up_to_date"
    elif not stored_rows:
        result.status = "missing_in_fly"
    else:
        result.status = "changed"
    return result


def execute_update(
    conn: FlyDuckDBConnection,
    result: DraftCheckResult,
    *,
    allow_incomplete_draft: bool,
    skip_sql_enrichments: bool,
    include_ops_enrichments: bool,
    strict_sql_enrichments: bool,
    skip_aggregates: bool,
    skip_homepage: bool,
    chunk_size: int,
) -> None:
    plan = resolve_plan(conn, result.db_name, draft_year=result.draft_year)
    selection = select_sleeper_drafts(plan, require_complete=not allow_incomplete_draft)
    draft_df = fetch_sleeper_draft_dataframe(plan, selection)
    replace_draft_year(conn, plan, draft_df, chunk_size=chunk_size, dry_run=False)

    if not skip_sql_enrichments:
        run_draft_sql_enrichments(
            conn,
            plan.db_name,
            include_ops_enrichments=include_ops_enrichments,
            strict=strict_sql_enrichments,
        )
    if not skip_aggregates:
        rebuild_draft_aggregates(conn, plan.db_name)
    if not skip_homepage:
        rebuild_homepage_tables(conn, plan.db_name, chunk_size=chunk_size)
    result.updated = True


def print_result(result: DraftCheckResult) -> None:
    if result.status in {"missing_in_fly", "changed"}:
        marker = "UPDATE_NEEDED"
    elif result.status == "up_to_date":
        marker = "OK"
    else:
        marker = "SKIP"
    print(
        f"[{marker}] {result.db_name}: status={result.status} "
        f"year={result.draft_year} api_picks={result.api_pick_count} "
        f"stored_picks={result.stored_pick_count} "
        f"api_drafts={result.api_draft_ids or []} stored_drafts={result.stored_draft_ids or []}"
    )
    if result.error:
        print(f"  {result.error}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Check Sleeper offseason drafts and optionally update changed leagues."
    )
    parser.add_argument("--db", action="append", dest="db_names", help="Limit to one db_name; repeatable.")
    parser.add_argument(
        "--draft-year", type=int, help="Draft year to check. Defaults to max matchup year + 1 per league."
    )
    parser.add_argument("--execute", action="store_true", help="Apply draft-only updates for changed/missing leagues.")
    parser.add_argument(
        "--allow-incomplete-draft", action="store_true", help="Compare/update drafts that are not complete."
    )
    parser.add_argument("--limit", type=int, help="Limit number of leagues checked.")
    parser.add_argument("--json-out", help="Write check results to this JSON file.")
    parser.add_argument(
        "--skip-sql-enrichments", action="store_true", help="When executing, skip draft SQL enrichments."
    )
    parser.add_argument(
        "--include-ops-enrichments",
        action="store_true",
        help="When executing, include ops-backed draft SQL enrichments.",
    )
    parser.add_argument(
        "--strict-sql-enrichments", action="store_true", help="When executing, fail on draft SQL enrichment errors."
    )
    parser.add_argument("--skip-aggregates", action="store_true", help="When executing, skip draft aggregate rebuilds.")
    parser.add_argument(
        "--skip-homepage", action="store_true", help="When executing, skip homepage aggregate rebuilds."
    )
    parser.add_argument("--chunk-size", type=int, default=50, help="Rows per Fly INSERT statement during updates.")
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    conn = FlyDuckDBConnection()
    db_names = args.db_names or list_sleeper_db_names(conn)
    if args.limit:
        db_names = db_names[: args.limit]

    results: list[DraftCheckResult] = []
    for db_name in db_names:
        try:
            result = check_one(
                conn,
                db_name,
                draft_year=args.draft_year,
                allow_incomplete_draft=args.allow_incomplete_draft,
            )
            print_result(result)
            if args.execute and result.status in {"missing_in_fly", "changed"}:
                execute_update(
                    conn,
                    result,
                    allow_incomplete_draft=args.allow_incomplete_draft,
                    skip_sql_enrichments=args.skip_sql_enrichments,
                    include_ops_enrichments=args.include_ops_enrichments,
                    strict_sql_enrichments=args.strict_sql_enrichments,
                    skip_aggregates=args.skip_aggregates,
                    skip_homepage=args.skip_homepage,
                    chunk_size=args.chunk_size,
                )
                print(f"  updated={result.updated}")
        except Exception as exc:
            result = DraftCheckResult(
                db_name=db_name,
                league_name=None,
                sleeper_league_id=None,
                draft_year=args.draft_year,
                status="error",
                error=str(exc),
            )
            print_result(result)
        results.append(result)

    summary: dict[str, int] = {}
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
    print("\nSummary:")
    for status, count in sorted(summary.items()):
        print(f"  {status}: {count}")
    if args.execute:
        print(f"  updated: {sum(1 for result in results if result.updated)}")

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps([asdict(result) for result in results], indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return 1 if any(result.status == "error" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
