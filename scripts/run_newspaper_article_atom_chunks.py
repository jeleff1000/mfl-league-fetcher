#!/usr/bin/env python
"""Run newspaper article atom manifests in resumable chunks.

The article atom conveyor can be slow on messy full-page PDFs. This wrapper
keeps the conveyor moving by splitting a manifest into small chunks, running
each chunk as a bounded subprocess, logging stdout/stderr, and writing a
remaining manifest for anything not completed.

All writes stay under the local D-drive newspaper atom workspace and local
DuckDB. No Fly, v26, or production supertable writes are performed.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "article_atom_chunk_runs"
DEFAULT_ARTICLE_OUT_ROOT = DEFAULT_ROOT / "article_atom_rounds"
DEFAULT_DB_PATH = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_ARTICLE_TOOL = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor\newspapers_article_atom_conveyor.py")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    return "" if value is None else str(value)


def stable_id(*parts: Any, length: int = 20) -> str:
    text = "|".join(str(part) for part in parts)
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()[:length]


def source_document_id(row: dict[str, str]) -> str:
    return clean(row.get("source_document_id") or row.get("candidate_id")) or stable_id(
        row.get("boxscore_id"),
        row.get("publication"),
        row.get("page"),
    )


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def csv_fields(rows: list[dict[str, str]]) -> list[str]:
    fields: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for field in row:
            if field not in seen:
                seen.add(field)
                fields.append(field)
    return fields


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True, default=str) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str) + "\n")


def chunks(rows: list[dict[str, str]], chunk_size: int) -> list[list[dict[str, str]]]:
    return [rows[index:index + chunk_size] for index in range(0, len(rows), chunk_size)]


def table_exists(con: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(1)
        FROM information_schema.tables
        WHERE table_schema = ? AND table_name = ?
        """,
        [schema, table],
    ).fetchone()
    return bool(row and row[0])


def done_source_document_ids(db_path: Path) -> set[str]:
    if not db_path.exists():
        return set()
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        if not table_exists(con, "newspaper_review", "conveyor_document_state"):
            return set()
        rows = con.execute(
            """
            SELECT DISTINCT source_document_id
            FROM newspaper_review.conveyor_document_state
            WHERE current_station = 'article_atom_conveyor'
              AND source_document_id IS NOT NULL
              AND TRIM(source_document_id) != ''
            """
        ).fetchall()
        return {str(row[0]) for row in rows}
    finally:
        con.close()


def row_status(row: dict[str, str], done_ids: set[str]) -> dict[str, Any]:
    source_id = source_document_id(row)
    return {
        "source_document_id": source_id,
        "candidate_id": clean(row.get("candidate_id")),
        "boxscore_id": clean(row.get("boxscore_id")),
        "publication": clean(row.get("publication")),
        "page": clean(row.get("page")),
        "article_done": source_id in done_ids,
    }


def parse_article_stdout(stdout: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    summary: dict[str, Any] = {}
    document_events: list[dict[str, Any]] = []
    decoder = json.JSONDecoder()
    index = 0
    while index < len(stdout):
        start = stdout.find("{", index)
        if start < 0:
            break
        try:
            payload, end = decoder.raw_decode(stdout[start:])
        except json.JSONDecodeError:
            index = start + 1
            continue
        index = start + end
        if "documents" in payload and "status_counts" in payload and "atoms" in payload:
            summary = payload
        elif "source_document_id" in payload and "regions" in payload:
            document_events.append(payload)
    return summary, document_events


def run_chunk(
    chunk_manifest: Path,
    chunk_label: str,
    args: argparse.Namespace,
) -> tuple[int, str, str, bool]:
    cmd = [
        sys.executable,
        str(args.article_tool),
        "--manifest",
        str(chunk_manifest),
        "--label",
        chunk_label,
        "--out-root",
        str(args.article_out_root),
        "--db-path",
        str(args.db_path),
        "--dpi",
        str(args.dpi),
        "--psm",
        str(args.psm),
        "--max-anchors",
        str(args.max_anchors),
        "--max-proposals",
        str(args.max_proposals),
        "--upscale",
        str(args.upscale),
        "--contrast",
        str(args.contrast),
        "--timeout-seconds",
        str(args.tesseract_timeout_seconds),
    ]
    if args.threshold is not None:
        cmd.extend(["--threshold", str(args.threshold)])
    try:
        proc = subprocess.run(
            cmd,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=args.chunk_timeout_seconds,
        )
        return proc.returncode, proc.stdout or "", proc.stderr or "", False
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", errors="replace")
        stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", errors="replace")
        return 124, stdout, stderr, True


def write_run_status(
    out_dir: Path,
    chunk_log: Path,
    manifest: Path,
    rows: list[dict[str, str]],
    manifest_fields: list[str],
    chunk_results: list[dict[str, Any]],
    already_done_count: int,
    chunks_planned: int,
    overall_exit: int,
    args: argparse.Namespace,
    *,
    final: bool,
) -> dict[str, Any]:
    done_ids = done_source_document_ids(args.db_path)
    final_row_states = [row_status(row, done_ids) for row in rows]
    pending_rows = [row for row in rows if source_document_id(row) not in done_ids]
    status_counts = Counter("done" if row["article_done"] else "pending" for row in final_row_states)
    chunk_exit_counts = Counter(str(row["exit_code"]) for row in chunk_results)
    row_status_path = out_dir / "article_chunk_row_status.csv"
    remaining_manifest_path = out_dir / "remaining_article_manifest.csv"
    summary_payload = {
        "created_at_utc": iso_now(),
        "run_id": out_dir.name,
        "manifest": str(manifest),
        "output_dir": str(out_dir),
        "article_tool": str(args.article_tool),
        "article_out_root": str(args.article_out_root),
        "db_path": str(args.db_path),
        "chunk_size": args.chunk_size,
        "chunks_attempted": len(chunk_results),
        "chunks_planned": chunks_planned,
        "existing_done_before_run": already_done_count,
        "manifest_rows": len(rows),
        "final_done_rows": status_counts.get("done", 0),
        "final_pending_rows": status_counts.get("pending", 0),
        "chunk_exit_counts": dict(chunk_exit_counts),
        "row_status_path": str(row_status_path),
        "remaining_manifest_path": str(remaining_manifest_path),
        "chunk_log_path": str(chunk_log),
        "overall_exit": overall_exit,
        "status": "complete" if final and status_counts.get("pending", 0) == 0 else ("partial" if final else "in_progress"),
        "live_tables_touched": False,
    }
    write_csv(row_status_path, final_row_states, [
        "source_document_id",
        "candidate_id",
        "boxscore_id",
        "publication",
        "page",
        "article_done",
    ])
    write_csv(remaining_manifest_path, pending_rows, manifest_fields)
    write_json(out_dir / "summary.json", summary_payload)
    return summary_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--article-out-root", type=Path, default=DEFAULT_ARTICLE_OUT_ROOT)
    parser.add_argument("--article-tool", type=Path, default=DEFAULT_ARTICLE_TOOL)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--label", default="newspaper_article_atom_chunks")
    parser.add_argument("--chunk-size", type=int, default=5)
    parser.add_argument("--max-chunks", type=int, default=0)
    parser.add_argument("--dpi", type=int, default=200)
    parser.add_argument("--psm", type=int, default=4)
    parser.add_argument("--chunk-timeout-seconds", type=int, default=360)
    parser.add_argument("--tesseract-timeout-seconds", type=int, default=45)
    parser.add_argument("--max-anchors", type=int, default=1)
    parser.add_argument("--max-proposals", type=int, default=2)
    parser.add_argument("--upscale", type=float, default=1.2)
    parser.add_argument("--contrast", type=float, default=1.7)
    parser.add_argument("--threshold", type=int)
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--stop-on-failure", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_csv(args.manifest)
    manifest_fields = csv_fields(rows)
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    chunk_dir = out_dir / "chunk_manifests"
    log_dir = out_dir / "logs"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    done_before = done_source_document_ids(args.db_path)
    already_done_rows = [row for row in rows if source_document_id(row) in done_before]
    pending_rows = rows if args.overwrite_existing else [row for row in rows if source_document_id(row) not in done_before]
    chunk_rows = chunks(pending_rows, max(1, args.chunk_size))
    if args.max_chunks > 0:
        chunk_rows = chunk_rows[: args.max_chunks]

    chunk_results: list[dict[str, Any]] = []
    chunk_log = out_dir / "chunk_runs.jsonl"
    overall_exit = 0
    for index, rows_for_chunk in enumerate(chunk_rows, start=1):
        chunk_manifest = chunk_dir / f"article_chunk_{index:04d}.csv"
        write_csv(chunk_manifest, rows_for_chunk, manifest_fields)
        chunk_label = f"{args.label}_chunk_{index:04d}"
        started = iso_now()
        exit_code, stdout, stderr, timed_out = run_chunk(chunk_manifest, chunk_label, args)
        finished = iso_now()
        stdout_path = log_dir / f"chunk_{index:04d}.stdout.log"
        stderr_path = log_dir / f"chunk_{index:04d}.stderr.log"
        stdout_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
        article_summary, document_events = parse_article_stdout(stdout)
        done_after = done_source_document_ids(args.db_path)
        row_states = [row_status(row, done_after) for row in rows_for_chunk]
        payload = {
            "chunk_index": index,
            "chunk_manifest": str(chunk_manifest),
            "chunk_label": chunk_label,
            "started_at_utc": started,
            "finished_at_utc": finished,
            "exit_code": exit_code,
            "timed_out": timed_out,
            "stdout_log": str(stdout_path),
            "stderr_log": str(stderr_path),
            "article_summary": article_summary,
            "document_events": document_events,
            "row_states": row_states,
        }
        chunk_results.append(payload)
        append_jsonl(chunk_log, payload)
        if exit_code != 0:
            overall_exit = exit_code
        write_run_status(
            out_dir,
            chunk_log,
            args.manifest,
            rows,
            manifest_fields,
            chunk_results,
            len(already_done_rows),
            len(chunk_rows),
            overall_exit,
            args,
            final=False,
        )
        print(json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str))
        sys.stdout.flush()
        if exit_code != 0 and args.stop_on_failure:
            break

    summary_payload = write_run_status(
        out_dir,
        chunk_log,
        args.manifest,
        rows,
        manifest_fields,
        chunk_results,
        len(already_done_rows),
        len(chunk_rows),
        overall_exit,
        args,
        final=True,
    )
    print(json.dumps(summary_payload, indent=2, ensure_ascii=True, sort_keys=True))
    return 0 if summary_payload["final_pending_rows"] == 0 else (overall_exit or 1)


if __name__ == "__main__":
    raise SystemExit(main())
