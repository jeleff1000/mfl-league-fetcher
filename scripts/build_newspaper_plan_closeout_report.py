"""Build a plan-level closeout report for local newspaper atom batches.

This is a local-only audit helper. It reads D-drive batch plans, closeout
artifacts, optional run reports, and the local newspaper DuckDB. It writes a
compact report under the D-drive newspaper atom workspace and never writes to
Fly, v26, or production supertable tables.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_PLAN = (
    DEFAULT_ROOT
    / "article_batch_plans"
    / "20260625T131842Z_1920_1939_full_pdf_article_batches_v1"
    / "batch_index.csv"
)
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "plan_closeout_reports"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def safe_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: safe_cell(row.get(field)) for field in fields})


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def batch_id_from_name(name: str) -> str:
    import re

    match = re.search(r"batch_?(\d{4})", name)
    return f"batch_{match.group(1)}" if match else ""


def newest_by_batch(paths: list[Path]) -> dict[str, Path]:
    latest: dict[str, Path] = {}
    for path in paths:
        batch_id = batch_id_from_name(path.name)
        if not batch_id:
            continue
        current = latest.get(batch_id)
        if current is None or path.stat().st_mtime > current.stat().st_mtime:
            latest[batch_id] = path
    return latest


def closeout_dirs_by_batch(root: Path) -> dict[str, list[Path]]:
    closeout_root = root / "batch_closeout_audits"
    if not closeout_root.exists():
        return {}
    grouped: dict[str, list[Path]] = {}
    for path in closeout_root.iterdir():
        batch_id = batch_id_from_name(path.name)
        if not path.is_dir() or "closeout" not in path.name.lower() or not batch_id:
            continue
        grouped.setdefault(batch_id, []).append(path)
    for paths in grouped.values():
        paths.sort(key=lambda path: path.stat().st_mtime, reverse=True)
    return grouped


def closeout_dirs(root: Path) -> dict[str, Path]:
    return {batch_id: paths[0] for batch_id, paths in closeout_dirs_by_batch(root).items() if paths}


def report_files(root: Path) -> dict[str, Path]:
    report_root = root / "run_reports"
    if not report_root.exists():
        return {}
    paths = [
        path
        for path in report_root.iterdir()
        if path.is_file() and batch_id_from_name(path.name)
    ]
    return newest_by_batch(paths)


def local_output_table_counts(db_path: Path) -> list[dict[str, Any]]:
    if not db_path.exists():
        return []
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        rows: list[dict[str, Any]] = []
        for schema in ["newspaper_promoted", "newspaper_final"]:
            tables = con.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = ?
                ORDER BY table_name
                """,
                [schema],
            ).fetchall()
            for (table_name,) in tables:
                count = con.execute(
                    f'SELECT COUNT(1) FROM {schema}."{table_name}"'
                ).fetchone()[0]
                rows.append({"schema": schema, "target_table": table_name, "row_count": int(count or 0)})
        return rows
    finally:
        con.close()


def latest_resolved_package_closeout(root: Path) -> tuple[Path | None, dict[str, Any]]:
    closeout_root = root / "batch_closeout_audits"
    if not closeout_root.exists():
        return None, {}
    candidates: list[tuple[float, Path, dict[str, Any]]] = []
    for path in closeout_root.iterdir():
        summary_path = path / "summary.json"
        if not path.is_dir() or not summary_path.exists():
            continue
        summary = load_json(summary_path)
        if safe_cell(summary.get("apply_run_kind")) != "resolved_package":
            continue
        candidates.append((path.stat().st_mtime, path, summary))
    if not candidates:
        return None, {}
    _, path, summary = sorted(candidates, key=lambda item: item[0])[-1]
    return path, summary


def build_markdown(summary: dict[str, Any], batch_rows: list[dict[str, Any]], promoted_counts: list[dict[str, Any]]) -> str:
    lines = [
        "# Newspaper Plan Closeout Report",
        "",
        f"- Created: {summary['created_at_utc']}",
        f"- Plan: `{summary['batch_index_csv']}`",
        f"- Newspaper DB: `{summary['db_path']}`",
        f"- Planned batches: `{summary['planned_batch_count']}`",
        f"- Batches with closeout: `{summary['batches_with_closeout']}`",
        f"- Ready closeouts: `{summary['ready_closeout_count']}`",
        f"- Active route rows: `{summary['active_route_queue_count']}`",
        f"- Terminal route receipts: `{summary['terminal_route_queue_count']}`",
        f"- Missing closeouts: `{summary['missing_closeout_batches'] or 'none'}`",
        f"- Non-ready closeouts: `{summary['non_ready_batches'] or 'none'}`",
        f"- Latest resolved-package closeout: `{summary['resolved_package_closeout_status'] or 'missing'}`",
        f"- Resolved-package final rows: `{summary['resolved_package_promoted_row_count']}`",
        f"- Batches with any run report: `{summary['batches_with_report']}`",
        f"- Missing run reports: `{summary['missing_report_batches'] or 'none'}`",
        "",
        "## Batch Status",
        "",
        "| Batch | Docs | Games | Closeout | Active routes | Terminal routes | Promoted | Report |",
        "|---|---:|---:|---|---:|---:|---:|---|",
    ]
    for row in batch_rows:
        lines.append(
            f"| `{row['batch_id']}` | {row['documents']} | {row['games']} | "
            f"`{row['closeout_status'] or 'missing'}` | {row['route_queue_count']} | "
            f"{row['terminal_route_queue_count']} | {row['promoted_row_count']} | "
            f"`{'yes' if row['report_path'] else 'no'}` |"
        )

    lines.extend([
        "",
        "## Local Output Tables",
        "",
        "| Schema | Table | Rows |",
        "|---|---|---:|",
    ])
    for row in promoted_counts:
        lines.append(f"| `{row['schema']}` | `{row['target_table']}` | {row['row_count']} |")

    lines.extend([
        "",
        "## Files",
        "",
        f"- Batch CSV: `{summary['batch_csv']}`",
        f"- Promoted counts CSV: `{summary['promoted_counts_csv']}`",
        f"- Latest resolved-package closeout: `{summary['resolved_package_closeout_path']}`",
        f"- Summary JSON: `{summary['summary_json']}`",
    ])
    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--batch-index-csv", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="1920_1939_full_pdf_article_batches_v1_closeout_report")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    created_at = iso_now()
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    planned = read_csv(args.batch_index_csv)
    planned_ids = [row.get("batch_id", "") for row in planned]
    closeout_groups = closeout_dirs_by_batch(args.root)
    closeouts = {batch_id: paths[0] for batch_id, paths in closeout_groups.items() if paths}
    reports = report_files(args.root)

    batch_rows: list[dict[str, Any]] = []
    closeout_status_counts: Counter[str] = Counter()
    active_route_total = 0
    terminal_route_total = 0
    ready_count = 0

    for plan_row in planned:
        batch_id = plan_row.get("batch_id", "")
        closeout_paths = closeout_groups.get(batch_id, [])
        closeout_path = closeout_paths[0] if closeout_paths else None
        closeout_summary = load_json(closeout_path / "summary.json") if closeout_path else {}
        all_closeout_summaries = [
            load_json(path / "summary.json")
            for path in closeout_paths
            if (path / "summary.json").exists()
        ]
        status = safe_cell(closeout_summary.get("closeout_status"))
        if status:
            closeout_status_counts[status] += 1
        if status == "ready_to_advance":
            ready_count += 1
        active_routes = int(closeout_summary.get("route_queue_count") or 0)
        terminal_routes = int(closeout_summary.get("terminal_route_queue_count") or 0)
        active_route_total += active_routes
        terminal_route_total += terminal_routes
        report_path = reports.get(batch_id)
        max_promoted = max(
            [int(summary.get("promoted_row_count") or 0) for summary in all_closeout_summaries] or [0]
        )
        batch_rows.append({
            "batch_id": batch_id,
            "documents": plan_row.get("documents", ""),
            "games": plan_row.get("games", ""),
            "ocr_documents": plan_row.get("ocr_documents", ""),
            "article_documents": plan_row.get("article_documents", ""),
            "closeout_status": status,
            "closeout_path": str(closeout_path or ""),
            "promotion_apply_run_id": safe_cell(closeout_summary.get("promotion_apply_run_id")),
            "promoted_row_count": max_promoted,
            "route_queue_count": active_routes,
            "terminal_route_queue_count": terminal_routes,
            "unresolved_note_count": int(closeout_summary.get("unresolved_note_count") or 0),
            "report_path": str(report_path or ""),
        })

    missing_closeouts = [batch_id for batch_id in planned_ids if batch_id not in closeouts]
    non_ready = [
        row["batch_id"]
        for row in batch_rows
        if row["closeout_status"] and row["closeout_status"] != "ready_to_advance"
    ]
    missing_reports = [batch_id for batch_id in planned_ids if batch_id not in reports]
    promoted_counts = local_output_table_counts(args.db_path)
    resolved_closeout_path, resolved_closeout_summary = latest_resolved_package_closeout(args.root)
    promoted_total_by_schema = Counter()
    for row in promoted_counts:
        promoted_total_by_schema[row["schema"]] += int(row["row_count"])

    batch_csv = out_dir / "batch_closeout_index.csv"
    promoted_csv = out_dir / "promoted_table_counts.csv"
    summary_json = out_dir / "summary.json"
    markdown_path = out_dir / "plan_closeout_report.md"

    summary = {
        "created_at_utc": created_at,
        "run_id": run_id,
        "output_dir": str(out_dir),
        "batch_index_csv": str(args.batch_index_csv),
        "db_path": str(args.db_path),
        "planned_batch_count": len(planned),
        "batches_with_closeout": len(closeouts),
        "ready_closeout_count": ready_count,
        "closeout_status_counts": dict(closeout_status_counts),
        "active_route_queue_count": active_route_total,
        "terminal_route_queue_count": terminal_route_total,
        "missing_closeout_batches": ",".join(missing_closeouts),
        "non_ready_batches": ",".join(non_ready),
        "batches_with_report": len(reports),
        "missing_report_batches": ",".join(missing_reports),
        "promoted_table_total_rows": promoted_total_by_schema["newspaper_promoted"],
        "final_table_total_rows": promoted_total_by_schema["newspaper_final"],
        "local_output_table_total_rows": sum(int(row["row_count"]) for row in promoted_counts),
        "resolved_package_closeout_path": str(resolved_closeout_path or ""),
        "resolved_package_closeout_status": safe_cell(resolved_closeout_summary.get("closeout_status")),
        "resolved_package_apply_run_id": safe_cell(resolved_closeout_summary.get("promotion_apply_run_id")),
        "resolved_package_promoted_row_count": int(resolved_closeout_summary.get("promoted_row_count") or 0),
        "resolved_package_route_queue_count": int(resolved_closeout_summary.get("route_queue_count") or 0),
        "resolved_package_unresolved_note_count": int(resolved_closeout_summary.get("unresolved_note_count") or 0),
        "batch_csv": str(batch_csv),
        "promoted_counts_csv": str(promoted_csv),
        "summary_json": str(summary_json),
        "markdown": str(markdown_path),
    }

    write_csv(
        batch_csv,
        batch_rows,
        [
            "batch_id",
            "documents",
            "games",
            "ocr_documents",
            "article_documents",
            "closeout_status",
            "promotion_apply_run_id",
            "promoted_row_count",
            "route_queue_count",
            "terminal_route_queue_count",
            "unresolved_note_count",
            "closeout_path",
            "report_path",
        ],
    )
    write_csv(promoted_csv, promoted_counts, ["schema", "target_table", "row_count"])
    summary_json.write_text(
        json.dumps(summary, indent=2, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )
    markdown_path.write_text(
        build_markdown(summary, batch_rows, promoted_counts),
        encoding="utf-8",
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
