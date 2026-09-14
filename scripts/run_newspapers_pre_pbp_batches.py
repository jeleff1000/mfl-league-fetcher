#!/usr/bin/env python
"""Run v26 pre-PBP Newspapers.com acquisition batches without hand-holding.

This is an orchestration wrapper around newspapers_com_acquire_canary.mjs and
newspapers_com_checkpoint.py. It favors breadth over perfection:

- process fixed-size chunks in queue order;
- checkpoint every successful run manifest;
- record no-candidate games instead of retrying them immediately;
- stop on browser/no-manifest failures rather than spinning.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen


DEFAULT_QUEUE_ROOT = Path(
    r"D:\league-history-data\nfl\derived\validation\newspaper_acquisition_queue"
    r"\20260620T_v26_pre_pbp_newspaper_acquisition_queue"
)
DEFAULT_DOWNLOAD_ROOT = Path(
    r"D:\league-history-data\nfl\raw\newspaper_archives\source_horizon_game_candidates"
    r"\game_completeness_download_pilots"
)
DEFAULT_EDGE_PROFILE = Path(r"C:\Users\joeye\AppData\Local\Temp\codex-edge-newspapers-9224")
RUN_MANIFEST_RE = re.compile(r"^run manifest:\s*(.+?)\s*$", re.MULTILINE)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def cdp_healthy(port: int, timeout: int = 10) -> bool:
    try:
        with urlopen(f"http://127.0.0.1:{port}/json/version", timeout=timeout) as resp:
            return resp.status == 200
    except (OSError, URLError):
        return False


def find_edge() -> Path:
    candidates = [
        Path(r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"),
        Path(r"C:\Program Files\Microsoft\Edge\Application\msedge.exe"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    raise FileNotFoundError("Could not locate msedge.exe")


def start_edge(port: int, profile: Path, visible: bool = False) -> subprocess.Popen:
    args = [
        str(find_edge()),
        f"--remote-debugging-port={port}",
        f"--user-data-dir={profile}",
        "--no-first-run",
        "--no-default-browser-check",
        "https://www.newspapers.com/browse/",
    ]
    creationflags = 0 if visible else getattr(subprocess, "CREATE_NO_WINDOW", 0)
    return subprocess.Popen(args, creationflags=creationflags)


def load_batch_rows(batch_csv: Path) -> list[dict[str, str]]:
    with batch_csv.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def chunked(rows: list[dict[str, str]], size: int, start_after: str | None) -> list[list[dict[str, str]]]:
    if start_after:
        idx = next((i for i, row in enumerate(rows) if row["boxscore_id"] == start_after), -1)
        if idx >= 0:
            rows = rows[idx + 1 :]
    return [rows[i : i + size] for i in range(0, len(rows), size)]


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def run_command(
    command: list[str],
    cwd: Path,
    stdout_path: Path,
    stderr_path: Path,
    timeout_seconds: int,
) -> tuple[int, bool]:
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
        proc = subprocess.Popen(command, cwd=str(cwd), stdout=stdout, stderr=stderr)
        try:
            return proc.wait(timeout=timeout_seconds), False
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=30)
            return proc.returncode or 124, True


def parse_manifest_path(stdout_path: Path) -> Path | None:
    text = stdout_path.read_text(encoding="utf-8", errors="ignore") if stdout_path.exists() else ""
    match = RUN_MANIFEST_RE.search(text)
    if not match:
        return None
    return Path(match.group(1).replace("/", "\\"))


def summarize_manifest(manifest_path: Path) -> dict:
    manifest = read_json(manifest_path)
    rows = []
    for game in manifest.get("games", []):
        selected = game.get("selectedCandidates") or []
        captured = game.get("captured") or []
        rows.append(
            {
                "boxscore_id": game.get("boxscoreId"),
                "candidate_count": game.get("candidateCount", 0),
                "selected_count": len(selected),
                "captured_count": len(captured),
            }
        )
    return {
        "games": len(rows),
        "selected_games": sum(1 for row in rows if row["selected_count"] > 0),
        "no_candidate_games": [row for row in rows if row["selected_count"] == 0],
        "manifest_game_rows": rows,
    }


def print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, ensure_ascii=True, default=str), flush=True)


def append_jsonl(path: Path, payload: dict) -> tuple[bool, str | None]:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True, default=str) + "\n")
        return True, None
    except OSError as exc:
        return False, f"{type(exc).__name__}: {exc}"


def emit_payload(summary_jsonl: Path, payload: dict, *, quiet: bool = False, force_print: bool = False) -> bool:
    appended, error = append_jsonl(summary_jsonl, payload)
    output = payload
    if not appended:
        output = {
            **payload,
            "summary_jsonl": str(summary_jsonl),
            "summary_jsonl_write_error": error,
        }
    if force_print or not quiet or not appended:
        print_json(output)
    return appended


def path_root(path: Path) -> Path | None:
    if path.drive:
        return Path(f"{path.drive}\\")
    if path.anchor:
        return Path(path.anchor)
    return None


def root_available(path: Path) -> bool:
    root = path_root(path)
    return root is None or root.exists()


def storage_unavailable_payload(status: str, path: Path, **extra: object) -> dict:
    root = path_root(path)
    return {
        "status": status,
        "path": str(path),
        "storage_root": str(root) if root else None,
        "note": "Storage root is not currently mounted or reachable.",
        **extra,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-csv", type=Path, default=DEFAULT_QUEUE_ROOT / "pre_pbp_batches" / "pre_pbp_batch_0001.csv")
    parser.add_argument("--batch-dir", type=Path, default=None)
    parser.add_argument("--batch-label", default=None)
    parser.add_argument("--start-batch-index", type=int, default=1)
    parser.add_argument("--max-batches", type=int, default=1)
    parser.add_argument("--start-after", default=None)
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--max-chunks", type=int, default=1)
    parser.add_argument("--port", type=int, default=9226)
    parser.add_argument("--edge-profile", type=Path, default=DEFAULT_EDGE_PROFILE)
    parser.add_argument("--start-edge-if-needed", action="store_true")
    parser.add_argument("--visible-edge", action="store_true")
    parser.add_argument("--pages-per-game", type=int, default=3)
    parser.add_argument("--max-queries-per-game", type=int, default=3)
    parser.add_argument("--chunk-timeout-seconds", type=int, default=1800)
    parser.add_argument("--checkpoint-timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-python", default="python")
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--run-root", type=Path, default=DEFAULT_DOWNLOAD_ROOT / "_canary_runs")
    parser.add_argument(
        "--quiet-chunks",
        action="store_true",
        help="Keep detailed chunk payloads in runner_summary.jsonl but print only failures and final aggregate summary.",
    )
    args = parser.parse_args()

    queue_path = args.batch_dir if args.batch_dir else args.batch_csv
    for required_path, role in ((queue_path, "queue"), (args.run_root, "run_root")):
        if required_path and not root_available(required_path):
            print_json(storage_unavailable_payload("blocked_storage_unavailable", required_path, role=role))
            return 2

    if args.batch_dir:
        if not args.batch_dir.exists():
            raise FileNotFoundError(args.batch_dir)
        batch_files = sorted(args.batch_dir.glob("pre_pbp_batch_*.csv"))
        batch_files = batch_files[max(args.start_batch_index - 1, 0) :]
        batch_files = batch_files[: args.max_batches]
    else:
        if not args.batch_csv.exists():
            raise FileNotFoundError(args.batch_csv)
        batch_files = [args.batch_csv]
    if not batch_files:
        raise FileNotFoundError("No batch CSV files selected")

    batch_label = args.batch_label or (args.batch_dir.name if args.batch_dir else args.batch_csv.stem)
    session_dir = args.run_root / f"{stamp()}_{batch_label}_overnight_runner"
    try:
        session_dir.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print_json(
            storage_unavailable_payload(
                "blocked_storage_unavailable",
                session_dir,
                role="session_dir",
                error=f"{type(exc).__name__}: {exc}",
            )
        )
        return 2
    summary_jsonl = session_dir / "runner_summary.jsonl"

    if not cdp_healthy(args.port):
        if not args.start_edge_if_needed:
            payload = {
                "status": "blocked_cdp_unhealthy",
                "port": args.port,
                "note": "CDP not healthy and --start-edge-if-needed was not set.",
            }
            emit_payload(summary_jsonl, payload, force_print=True)
            return 2
        start_edge(args.port, args.edge_profile, visible=args.visible_edge)
        time.sleep(8)
        if not cdp_healthy(args.port):
            payload = {"status": "blocked_cdp_unhealthy_after_start", "port": args.port}
            emit_payload(summary_jsonl, payload, force_print=True)
            return 2

    completed = []
    for batch_file in batch_files:
        rows = load_batch_rows(batch_file)
        batch_start_after = args.start_after if batch_file == batch_files[0] else None
        chunks = chunked(rows, args.chunk_size, batch_start_after)[: args.max_chunks]
        if not chunks:
            payload = {
                "status": "nothing_to_do_for_batch",
                "batch_csv": str(batch_file),
                "start_after": batch_start_after,
            }
            emit_payload(summary_jsonl, payload, quiet=args.quiet_chunks)
            continue

        for chunk_idx, chunk in enumerate(chunks, start=1):
            ids = [row["boxscore_id"] for row in chunk]
            chunk_label = f"{batch_file.stem}_chunk{chunk_idx:04d}_{ids[0]}_through_{ids[-1]}"
            launcher = session_dir / f"{stamp()}_{chunk_label}_launcher"
            if not root_available(args.run_root):
                payload = storage_unavailable_payload(
                    "stopped_storage_unavailable_before_chunk",
                    args.run_root,
                    batch_csv=str(batch_file),
                    chunk_index=chunk_idx,
                    chunk_label=chunk_label,
                    boxscore_ids=ids,
                )
                emit_payload(summary_jsonl, payload, force_print=True)
                return 2
            try:
                launcher.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                payload = storage_unavailable_payload(
                    "stopped_storage_unavailable_before_chunk",
                    launcher,
                    batch_csv=str(batch_file),
                    chunk_index=chunk_idx,
                    chunk_label=chunk_label,
                    boxscore_ids=ids,
                    error=f"{type(exc).__name__}: {exc}",
                )
                emit_payload(summary_jsonl, payload, force_print=True)
                return 2
            stdout_path = launcher / "stdout.log"
            stderr_path = launcher / "stderr.log"

            command = [
                "node",
                "scripts\\newspapers_com_acquire_canary.mjs",
                "--port",
                str(args.port),
                "--queue",
                str(batch_file),
                "--boxscores",
                ",".join(ids),
                "--pages-per-game",
                str(args.pages_per_game),
                "--max-queries-per-game",
                str(args.max_queries_per_game),
                "--pdf-only",
                "--browser-print-pdf-fallback",
                "--pdf-wait-ms",
                "15000",
                "--capture-delay-ms",
                "0",
                "--search-attempts",
                "2",
                "--search-retry-delay-ms",
                "3000",
                "--image-fetch-attempts",
                "1",
                "--image-fetch-retry-delay-ms",
                "3000",
            ]
            returncode, timed_out = run_command(
                command, args.repo_root, stdout_path, stderr_path, args.chunk_timeout_seconds
            )
            manifest_path = parse_manifest_path(stdout_path)
            chunk_payload = {
                "status": "chunk_finished",
                "batch_csv": str(batch_file),
                "chunk_index": chunk_idx,
                "chunk_label": chunk_label,
                "boxscore_ids": ids,
                "returncode": returncode,
                "timed_out": timed_out,
                "launcher": str(launcher),
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
                "manifest": str(manifest_path) if manifest_path else None,
            }
            if returncode != 0 or timed_out or not manifest_path or not manifest_path.exists():
                if not manifest_path or not manifest_path.exists():
                    chunk_payload["status"] = "stopped_no_clean_manifest"
                    emit_payload(summary_jsonl, chunk_payload, force_print=True)
                    return 1

            checkpoint_dir = args.run_root / f"{stamp()}_{chunk_label}_checkpoint"
            checkpoint_stdout = launcher / "checkpoint_stdout.json"
            checkpoint_stderr = launcher / "checkpoint_stderr.log"
            checkpoint_command = [
                args.checkpoint_python,
                "scripts\\newspapers_com_checkpoint.py",
                "--label",
                chunk_label,
                "--out-dir",
                str(checkpoint_dir),
                "--manifest",
                str(manifest_path),
            ]
            cp_returncode, cp_timed_out = run_command(
                checkpoint_command,
                args.repo_root,
                checkpoint_stdout,
                checkpoint_stderr,
                args.checkpoint_timeout_seconds,
            )
            manifest_summary = summarize_manifest(manifest_path)
            checkpoint_summary = {}
            if checkpoint_stdout.exists():
                text = checkpoint_stdout.read_text(encoding="utf-8", errors="ignore").strip()
                if text:
                    try:
                        checkpoint_summary = json.loads(text)
                    except json.JSONDecodeError:
                        checkpoint_summary = {"raw": text[-2000:]}

            if returncode != 0 or timed_out:
                chunk_payload["status"] = "stopped_after_partial_manifest"

            chunk_payload.update(
                {
                    "checkpoint_returncode": cp_returncode,
                    "checkpoint_timed_out": cp_timed_out,
                    "checkpoint_dir": str(checkpoint_dir),
                    "manifest_summary": manifest_summary,
                    "checkpoint_summary": checkpoint_summary,
                }
            )
            emit_payload(summary_jsonl, chunk_payload, quiet=args.quiet_chunks)
            if cp_returncode != 0 or cp_timed_out or returncode != 0 or timed_out:
                return 1
            completed.append(chunk_payload)

    status_counts: dict[str, int] = {}
    problem_chunks = []
    totals = {
        "games": 0,
        "selected_games": 0,
        "selected_candidates": 0,
        "pdf_count": 0,
        "structural_ok": 0,
        "no_candidate_games": 0,
    }
    for payload in completed:
        manifest_summary = payload.get("manifest_summary") or {}
        checkpoint_summary = payload.get("checkpoint_summary") or {}
        totals["games"] += int(manifest_summary.get("games") or 0)
        totals["selected_games"] += int(manifest_summary.get("selected_games") or 0)
        totals["no_candidate_games"] += len(manifest_summary.get("no_candidate_games") or [])
        totals["selected_candidates"] += int(checkpoint_summary.get("selected_candidates") or 0)
        totals["pdf_count"] += int(checkpoint_summary.get("pdf_count") or 0)
        totals["structural_ok"] += int(checkpoint_summary.get("structural_ok") or 0)
        for status, count in (checkpoint_summary.get("status_counts") or {}).items():
            status_counts[status] = status_counts.get(status, 0) + int(count or 0)
        if (
            int(manifest_summary.get("selected_games") or 0) < int(manifest_summary.get("games") or 0)
            or int(checkpoint_summary.get("structural_ok") or 0)
            < int(checkpoint_summary.get("selected_candidates") or 0)
        ):
            problem_chunks.append(
                {
                    "chunk_label": payload.get("chunk_label"),
                    "games": manifest_summary.get("games"),
                    "selected_games": manifest_summary.get("selected_games"),
                    "selected_candidates": checkpoint_summary.get("selected_candidates"),
                    "structural_ok": checkpoint_summary.get("structural_ok"),
                    "checkpoint_dir": checkpoint_summary.get("checkpoint_dir"),
                }
            )

    final_payload = {
        "status": "complete",
        "session_dir": str(session_dir),
        "chunks_completed": len(completed),
        "last_boxscore_id": completed[-1]["boxscore_ids"][-1] if completed else None,
        "totals": totals,
        "status_counts": status_counts,
        "problem_chunks": problem_chunks,
        "summary_jsonl": str(summary_jsonl),
    }
    emit_payload(summary_jsonl, final_payload, force_print=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
