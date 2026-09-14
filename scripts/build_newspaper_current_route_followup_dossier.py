#!/usr/bin/env python
"""Build a compact review dossier for the current newspaper route queue.

This station is intentionally read-only against local DuckDB. It turns the
latest `review_decision_route_queue` into a human/LLM review packet with the
remaining route reason, proposed fields, source documents, OCR/text paths, and
short source snippets. It never writes to Fly or canonical supertable tables.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "current_route_followup_dossiers"
DEFAULT_TOOL_MIRROR = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor")

DOSSIER_FIELDS = [
    "promotion_apply_run_id",
    "decision_ledger_run_id",
    "decision_id",
    "lane",
    "route_to_lane",
    "decision_status",
    "decision_value",
    "source_document_id",
    "target_table",
    "target_entity_key",
    "boxscore_id",
    "recommended_next_action",
    "reason",
    "notes",
    "source_doc_ids_json",
    "source_doc_issue_dates_json",
    "source_doc_asset_paths_json",
    "text_paths_json",
    "snippet",
    "proposed_fields_json",
    "source_documents_json",
    "artifact_path",
]


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def parse_json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [clean(item) for item in value if clean(item)]
    text = clean(value)
    if not text:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return [text]
    if isinstance(parsed, list):
        return [clean(item) for item in parsed if clean(item)]
    return [clean(parsed)] if clean(parsed) else []


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


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_apply_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT promotion_apply_run_id
        FROM newspaper_review.review_decision_apply_run
        ORDER BY created_at_utc DESC, promotion_apply_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_routes(
    con: duckdb.DuckDBPyConnection,
    promotion_apply_run_id: str,
    include_rejects: bool,
    route_lane: str,
) -> list[dict[str, Any]]:
    filters = ["promotion_apply_run_id = ?"]
    params: list[Any] = [promotion_apply_run_id]
    if not include_rejects:
        filters.append("route_to_lane <> 'reject'")
    if route_lane:
        filters.append("route_to_lane = ?")
        params.append(route_lane)
    return query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.review_decision_route_queue
        WHERE {' AND '.join(filters)}
        ORDER BY route_to_lane, recommended_next_action, target_table, boxscore_id, decision_id
        """,
        params,
    )


def source_doc_ids(row: dict[str, Any]) -> list[str]:
    ids = []
    if clean(row.get("source_document_id")):
        ids.append(clean(row.get("source_document_id")))
    ids.extend(parse_json_list(row.get("source_documents_json")))
    return sorted({item for item in ids if item})


def load_source_documents(
    con: duckdb.DuckDBPyConnection, doc_ids: list[str]
) -> dict[str, dict[str, Any]]:
    if not doc_ids:
        return {}
    placeholders = ",".join(["?"] * len(doc_ids))
    rows = query_dicts(
        con,
        f"""
        SELECT *
        FROM newspaper_review.source_document
        WHERE source_document_id IN ({placeholders})
        """,
        doc_ids,
    )
    return {clean(row.get("source_document_id")): row for row in rows}


def load_text_paths(
    con: duckdb.DuckDBPyConnection, doc_ids: list[str]
) -> dict[str, list[str]]:
    if not doc_ids:
        return {}
    placeholders = ",".join(["?"] * len(doc_ids))
    out: dict[str, list[str]] = defaultdict(list)
    for table, field in [
        ("source_region", "region_text_path"),
        ("source_text_pass", "ocr_text_path"),
    ]:
        rows = query_dicts(
            con,
            f"""
            SELECT source_document_id, {field} AS text_path
            FROM newspaper_review.{table}
            WHERE source_document_id IN ({placeholders})
              AND {field} IS NOT NULL
            """,
            doc_ids,
        )
        for row in rows:
            doc_id = clean(row.get("source_document_id"))
            path = clean(row.get("text_path"))
            if doc_id and path:
                out[doc_id].append(path)
    return {doc_id: sorted(set(paths)) for doc_id, paths in out.items()}


def read_snippet(paths: list[str], max_chars: int) -> str:
    chunks = []
    for path_text in paths:
        path = Path(path_text)
        if not path.exists():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue
        text = " ".join(text.split())
        if text:
            chunks.append(text)
        if sum(len(chunk) for chunk in chunks) >= max_chars:
            break
    return "\n\n".join(chunks)[:max_chars]


def build_rows(
    route_rows: list[dict[str, Any]],
    source_docs: dict[str, dict[str, Any]],
    text_paths: dict[str, list[str]],
    max_snippet_chars: int,
) -> list[dict[str, str]]:
    out = []
    for row in route_rows:
        doc_ids = source_doc_ids(row)
        docs = [source_docs[doc_id] for doc_id in doc_ids if doc_id in source_docs]
        issue_dates = sorted({clean(doc.get("issue_date")) for doc in docs if clean(doc.get("issue_date"))})
        asset_paths = sorted({
            clean(doc.get(field))
            for doc in docs
            for field in ["asset_pdf_path", "asset_image_path", "source_url"]
            if clean(doc.get(field))
        })
        paths = sorted({path for doc_id in doc_ids for path in text_paths.get(doc_id, [])})
        out.append({
            "promotion_apply_run_id": clean(row.get("promotion_apply_run_id")),
            "decision_ledger_run_id": clean(row.get("decision_ledger_run_id")),
            "decision_id": clean(row.get("decision_id")),
            "lane": clean(row.get("lane")),
            "route_to_lane": clean(row.get("route_to_lane")),
            "decision_status": clean(row.get("decision_status")),
            "decision_value": clean(row.get("decision_value")),
            "source_document_id": clean(row.get("source_document_id")),
            "target_table": clean(row.get("target_table")),
            "target_entity_key": clean(row.get("target_entity_key")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "recommended_next_action": clean(row.get("recommended_next_action")),
            "reason": clean(row.get("reason")),
            "notes": clean(row.get("notes")),
            "source_doc_ids_json": json.dumps(doc_ids, sort_keys=True),
            "source_doc_issue_dates_json": json.dumps(issue_dates, sort_keys=True),
            "source_doc_asset_paths_json": json.dumps(asset_paths, sort_keys=True),
            "text_paths_json": json.dumps(paths, sort_keys=True),
            "snippet": read_snippet(paths, max_snippet_chars),
            "proposed_fields_json": clean(row.get("proposed_fields_json")),
            "source_documents_json": clean(row.get("source_documents_json")),
            "artifact_path": clean(row.get("artifact_path")),
        })
    return out


def write_csv(path: Path, rows: list[dict[str, str]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_jsonl(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True, ensure_ascii=False) + "\n")


def write_markdown(path: Path, summary: dict[str, Any], rows: list[dict[str, str]]) -> None:
    lines = [
        "# Current Newspaper Route Followup Dossier",
        "",
        f"Created: `{summary['created_at_utc']}`",
        f"Apply run: `{summary['promotion_apply_run_id']}`",
        "",
        "## Counts",
        "",
        f"- Route rows: `{summary['route_count']}`",
        f"- Route lanes: `{summary['route_lane_counts']}`",
        f"- Recommended actions: `{summary['recommended_next_action_counts']}`",
        "",
        "## Rows",
        "",
    ]
    for idx, row in enumerate(rows, start=1):
        proposed = parse_json_obj(row.get("proposed_fields_json"))
        proposed_text = json.dumps(proposed, indent=2, sort_keys=True, ensure_ascii=False) if proposed else "{}"
        lines.extend([
            f"### {idx}. {row['route_to_lane']} / {row['recommended_next_action'] or 'no_action'}",
            "",
            f"- Decision: `{row['decision_id']}`",
            f"- Target: `{row['target_table'] or 'document_level'}` / `{row['boxscore_id']}`",
            f"- Reason: {row['reason'] or row['notes'] or 'n/a'}",
            f"- Source docs: `{row['source_doc_ids_json']}`",
            f"- Text paths: `{row['text_paths_json']}`",
            "",
            "Proposed fields:",
            "",
            "```json",
            proposed_text,
            "```",
            "",
            "Snippet:",
            "",
            "```text",
            row.get("snippet", ""),
            "```",
            "",
        ])
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--tool-mirror-root", type=Path, default=DEFAULT_TOOL_MIRROR)
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--route-lane", default="")
    parser.add_argument("--include-rejects", action="store_true")
    parser.add_argument("--max-snippet-chars", type=int, default=1600)
    parser.add_argument("--label", default="current_route_followup_dossier")
    parser.add_argument("--no-mirror", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        promotion_apply_run_id = args.promotion_apply_run_id or latest_apply_run(con)
        route_rows = load_routes(con, promotion_apply_run_id, args.include_rejects, args.route_lane)
        doc_ids = sorted({doc_id for row in route_rows for doc_id in source_doc_ids(row)})
        source_docs = load_source_documents(con, doc_ids)
        text_paths = load_text_paths(con, doc_ids)
    finally:
        con.close()

    rows = build_rows(route_rows, source_docs, text_paths, args.max_snippet_chars)
    csv_path = out_dir / "current_route_followup_dossier.csv"
    jsonl_path = out_dir / "current_route_followup_dossier.jsonl"
    markdown_path = out_dir / "current_route_followup_dossier.md"
    summary_path = out_dir / "summary.json"

    summary = {
        "created_at_utc": iso_now(),
        "route_followup_dossier_run_id": run_id,
        "promotion_apply_run_id": promotion_apply_run_id,
        "db_path": str(args.db_path),
        "output_dir": str(out_dir),
        "route_count": len(rows),
        "route_lane_counts": dict(Counter(row["route_to_lane"] for row in rows)),
        "target_table_counts": dict(Counter(row["target_table"] or "document_level" for row in rows)),
        "recommended_next_action_counts": dict(Counter(row["recommended_next_action"] or "no_action" for row in rows)),
        "csv_path": str(csv_path),
        "jsonl_path": str(jsonl_path),
        "markdown_path": str(markdown_path),
        "include_rejects": bool(args.include_rejects),
        "route_lane_filter": args.route_lane,
    }

    write_csv(csv_path, rows, DOSSIER_FIELDS)
    write_jsonl(jsonl_path, rows)
    write_markdown(markdown_path, summary, rows)
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if not args.no_mirror:
        args.tool_mirror_root.mkdir(parents=True, exist_ok=True)
        source = Path(__file__).resolve()
        target = args.tool_mirror_root / source.name
        if source.resolve() != target.resolve():
            target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
