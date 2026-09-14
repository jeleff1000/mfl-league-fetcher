#!/usr/bin/env python
"""Run OCR follow-up manifests in small resumable chunks.

The plain OCR runner is good for a bounded manifest, but a long manifest can
hit an outer shell timeout and lose the run summary. This wrapper splits the
latest OCR follow-up manifest into small chunks, skips sidecars that already
exist, logs each subprocess, and continues through row-level failures.
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_MANIFEST_ROOT = DEFAULT_ROOT / "ocr_followup_round_manifests"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "ocr_followup_chunk_runs"
DEFAULT_OCR_ROUND_ROOT = DEFAULT_ROOT / "ocr_rounds"
DEFAULT_OCR_TOOL = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor\newspapers_ocr_round.py")


MANIFEST_FIELDS = [
    "candidate_id",
    "boxscore_id",
    "publication",
    "result_date",
    "page",
    "pdf_path",
    "ocr_sidecar_dir",
    "suggested_ocr_text_path",
    "suggested_ocr_json_path",
    "source_document_id",
    "ocr_followup_prep_run_id",
    "action_queue_run_id",
    "recommended_next_pass",
    "followup_types_json",
    "reason",
    "prior_region_text_paths_json",
    "prior_crop_image_paths_json",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def clean(value: Any) -> str:
    return "" if value is None else str(value)


def latest_manifest(root: Path) -> Path:
    dirs = sorted(path for path in root.iterdir() if path.is_dir()) if root.exists() else []
    for directory in reversed(dirs):
        manifest = directory / "ocr_round_manifest.csv"
        if manifest.exists():
            return manifest
    raise FileNotFoundError(f"No ocr_round_manifest.csv found under {root}")


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def sidecar_done(row: dict[str, str], min_chars: int) -> bool:
    text_path = Path(clean(row.get("suggested_ocr_text_path")))
    json_path = Path(clean(row.get("suggested_ocr_json_path")))
    if not text_path.exists() or not json_path.exists():
        return False
    try:
        return text_path.stat().st_size >= min_chars
    except OSError:
        return False


def chunks(rows: list[dict[str, str]], chunk_size: int) -> list[list[dict[str, str]]]:
    return [rows[index:index + chunk_size] for index in range(0, len(rows), chunk_size)]


def row_status(row: dict[str, str], min_chars: int) -> dict[str, Any]:
    text_path = Path(clean(row.get("suggested_ocr_text_path")))
    json_path = Path(clean(row.get("suggested_ocr_json_path")))
    text_size = text_path.stat().st_size if text_path.exists() else 0
    json_size = json_path.stat().st_size if json_path.exists() else 0
    return {
        "candidate_id": clean(row.get("candidate_id")),
        "boxscore_id": clean(row.get("boxscore_id")),
        "recommended_next_pass": clean(row.get("recommended_next_pass")),
        "ocr_text_path": str(text_path),
        "ocr_json_path": str(json_path),
        "ocr_text_chars": text_size,
        "ocr_json_bytes": json_size,
        "sidecar_done": text_size >= min_chars and json_size > 0,
    }


def parse_ocr_summary(stdout: str) -> dict[str, Any]:
    last_payload: dict[str, Any] = {}
    for line in stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "documents_selected" in payload and "status_counts" in payload:
            last_payload = payload
    return last_payload


def run_chunk(
    chunk_manifest: Path,
    chunk_label: str,
    ocr_tool: Path,
    ocr_out_root: Path,
    dpi: int,
    psm: int,
    timeout_seconds: int,
    tesseract_timeout_seconds: int,
    overwrite: bool,
) -> tuple[int, str, str, bool]:
    cmd = [
        sys.executable,
        str(ocr_tool),
        "--manifest",
        str(chunk_manifest),
        "--label",
        chunk_label,
        "--out-root",
        str(ocr_out_root),
        "--dpi",
        str(dpi),
        "--psm",
        str(psm),
        "--timeout-seconds",
        str(tesseract_timeout_seconds),
    ]
    if overwrite:
        cmd.append("--overwrite")
    try:
        proc = subprocess.run(
            cmd,
            text=True,
            encoding="utf-8",
            errors="replace",
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
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
    ocr_tool: Path,
    args: argparse.Namespace,
    rows: list[dict[str, str]],
    chunk_results: list[dict[str, Any]],
    already_done_count: int,
    chunks_planned: int,
    overall_exit: int,
    *,
    final: bool,
) -> dict[str, Any]:
    row_status_path = out_dir / "ocr_followup_row_status.csv"
    final_row_states = [row_status(row, args.min_existing_chars) for row in rows]
    status_counts = Counter("done" if row["sidecar_done"] else "pending" for row in final_row_states)
    chunk_exit_counts = Counter(str(row["exit_code"]) for row in chunk_results)
    summary_payload = {
        "created_at_utc": iso_now(),
        "run_id": out_dir.name,
        "manifest": str(manifest),
        "output_dir": str(out_dir),
        "ocr_tool": str(ocr_tool),
        "chunk_size": args.chunk_size,
        "chunks_attempted": len(chunk_results),
        "chunks_planned": chunks_planned,
        "existing_done_before_run": already_done_count,
        "manifest_rows": len(rows),
        "final_done_rows": status_counts.get("done", 0),
        "final_pending_rows": status_counts.get("pending", 0),
        "chunk_exit_counts": dict(chunk_exit_counts),
        "total_followup_ocr_text_chars": sum(int(row["ocr_text_chars"] or 0) for row in final_row_states),
        "row_status_path": str(row_status_path),
        "chunk_log_path": str(chunk_log),
        "overall_exit": overall_exit,
        "status": "complete" if final else "in_progress",
    }
    write_csv(row_status_path, final_row_states, [
        "candidate_id",
        "boxscore_id",
        "recommended_next_pass",
        "ocr_text_path",
        "ocr_json_path",
        "ocr_text_chars",
        "ocr_json_bytes",
        "sidecar_done",
    ])
    write_json(out_dir / "summary.json", summary_payload)
    return summary_payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=None)
    parser.add_argument("--manifest-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--ocr-out-root", type=Path, default=DEFAULT_OCR_ROUND_ROOT)
    parser.add_argument("--ocr-tool", type=Path, default=DEFAULT_OCR_TOOL)
    parser.add_argument("--label", default="newspaper_ocr_followup_chunks")
    parser.add_argument("--chunk-size", type=int, default=1)
    parser.add_argument("--max-chunks", type=int, default=0)
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--psm", type=int, default=6)
    parser.add_argument("--chunk-timeout-seconds", type=int, default=360)
    parser.add_argument("--tesseract-timeout-seconds", type=int, default=180)
    parser.add_argument("--min-existing-chars", type=int, default=1)
    parser.add_argument("--overwrite-existing", action="store_true")
    parser.add_argument("--stop-on-failure", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = args.manifest or latest_manifest(args.manifest_root)
    run_id = f"{stamp()}_{args.label}"
    out_dir = args.out_root / run_id
    chunk_dir = out_dir / "chunk_manifests"
    log_dir = out_dir / "logs"
    chunk_dir.mkdir(parents=True, exist_ok=True)
    log_dir.mkdir(parents=True, exist_ok=True)

    rows = read_csv(manifest)
    already_done = [row for row in rows if sidecar_done(row, args.min_existing_chars)]
    pending = rows if args.overwrite_existing else [row for row in rows if not sidecar_done(row, args.min_existing_chars)]
    chunk_rows = chunks(pending, max(1, args.chunk_size))
    if args.max_chunks > 0:
        chunk_rows = chunk_rows[: args.max_chunks]

    chunk_results: list[dict[str, Any]] = []
    chunk_log = out_dir / "chunk_runs.jsonl"
    overall_exit = 0
    for index, rows_for_chunk in enumerate(chunk_rows, start=1):
        chunk_manifest = chunk_dir / f"ocr_followup_chunk_{index:04d}.csv"
        write_csv(chunk_manifest, rows_for_chunk, MANIFEST_FIELDS)
        chunk_label = f"{args.label}_chunk_{index:04d}"
        started = iso_now()
        exit_code, stdout, stderr, timed_out = run_chunk(
            chunk_manifest,
            chunk_label,
            args.ocr_tool,
            args.ocr_out_root,
            args.dpi,
            args.psm,
            args.chunk_timeout_seconds,
            args.tesseract_timeout_seconds,
            args.overwrite_existing,
        )
        finished = iso_now()
        stdout_path = log_dir / f"chunk_{index:04d}.stdout.log"
        stderr_path = log_dir / f"chunk_{index:04d}.stderr.log"
        stdout_path.write_text(stdout, encoding="utf-8")
        stderr_path.write_text(stderr, encoding="utf-8")
        summary = parse_ocr_summary(stdout)
        row_states = [row_status(row, args.min_existing_chars) for row in rows_for_chunk]
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
            "ocr_summary": summary,
            "row_states": row_states,
        }
        chunk_results.append(payload)
        append_jsonl(chunk_log, payload)
        write_run_status(
            out_dir,
            chunk_log,
            manifest,
            args.ocr_tool,
            args,
            rows,
            chunk_results,
            len(already_done),
            len(chunk_rows),
            overall_exit,
            final=False,
        )
        print(json.dumps(payload, ensure_ascii=False))
        sys.stdout.flush()
        if exit_code != 0:
            overall_exit = exit_code
            if args.stop_on_failure:
                break

    summary_payload = write_run_status(
        out_dir,
        chunk_log,
        manifest,
        args.ocr_tool,
        args,
        rows,
        chunk_results,
        len(already_done),
        len(chunk_rows),
        overall_exit,
        final=True,
    )
    print(json.dumps(summary_payload, indent=2, sort_keys=True))
    return 0 if summary_payload["final_pending_rows"] == 0 else overall_exit


if __name__ == "__main__":
    raise SystemExit(main())
