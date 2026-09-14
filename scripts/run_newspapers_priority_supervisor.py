#!/usr/bin/env python
"""Unattended supervisor for v26 Newspapers.com acquisition.

This wrapper is intentionally boring: it scans the queue and prior runner
summaries, writes a remaining-game plan, then repeatedly calls
run_newspapers_pre_pbp_batches.py on temporary queue slices. The priority order
is remaining 1920s/1930s games first, then everything else in queue order.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_QUEUE_ROOT = Path(
    r"D:\league-history-data\nfl\derived\validation\newspaper_acquisition_queue"
    r"\20260620T_v26_pre_pbp_newspaper_acquisition_queue"
)
DEFAULT_BATCH_DIR = DEFAULT_QUEUE_ROOT / "pre_pbp_batches"
DEFAULT_RUN_ROOT = Path(
    r"D:\league-history-data\nfl\raw\newspaper_archives\source_horizon_game_candidates"
    r"\game_completeness_download_pilots\_canary_runs"
)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def path_root(path: Path) -> Path | None:
    if path.drive:
        return Path(f"{path.drive}\\")
    if path.anchor:
        return Path(path.anchor)
    return None


def require_root(path: Path, role: str) -> None:
    root = path_root(path)
    if root and not root.exists():
        raise FileNotFoundError(f"{role} storage root is unavailable: {root}")


def read_batch_rows(batch_dir: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for batch_file in sorted(batch_dir.glob("pre_pbp_batch_*.csv")):
        batch_name = batch_file.stem
        try:
            batch_index = int(batch_name.rsplit("_", 1)[-1])
        except ValueError:
            batch_index = -1
        with batch_file.open(newline="", encoding="utf-8-sig") as handle:
            for row_index, row in enumerate(csv.DictReader(handle), start=1):
                row = dict(row)
                row["_batch_csv"] = str(batch_file)
                row["_batch_name"] = batch_name
                row["_batch_index"] = str(batch_index)
                row["_row_index"] = str(row_index)
                rows.append(row)
    return rows


def iter_runner_summaries(run_root: Path):
    for root, _dirs, files in os.walk(run_root):
        if "runner_summary.jsonl" in files:
            yield Path(root) / "runner_summary.jsonl"


def load_attempts(run_root: Path) -> dict[str, dict[str, int]]:
    attempts: dict[str, dict[str, int]] = defaultdict(
        lambda: {
            "attempts": 0,
            "selected_count": 0,
            "captured_count": 0,
            "structural_ok": 0,
        }
    )
    for summary_path in iter_runner_summaries(run_root):
        try:
            lines = summary_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        except OSError:
            continue
        for line in lines:
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            manifest_summary = payload.get("manifest_summary") or {}
            for row in manifest_summary.get("manifest_game_rows") or []:
                boxscore_id = row.get("boxscore_id")
                if not boxscore_id:
                    continue
                rec = attempts[boxscore_id]
                rec["attempts"] += 1
                rec["selected_count"] = max(rec["selected_count"], int(row.get("selected_count") or 0))
                rec["captured_count"] = max(rec["captured_count"], int(row.get("captured_count") or 0))
            checkpoint_summary = payload.get("checkpoint_summary") or {}
            structural_ok = int(checkpoint_summary.get("structural_ok") or 0)
            if structural_ok and (payload.get("boxscore_ids") or []):
                per_game_floor = 1 if structural_ok > 0 else 0
                for boxscore_id in payload.get("boxscore_ids") or []:
                    attempts[boxscore_id]["structural_ok"] = max(
                        attempts[boxscore_id]["structural_ok"], per_game_floor
                    )
    return dict(attempts)


def priority_bucket(row: dict[str, str]) -> int:
    year = int(row.get("year") or 0)
    if 1920 <= year <= 1939:
        return 0
    return 1


def write_plan(plan_path: Path, rows: list[dict[str, str]], attempts: dict[str, dict[str, int]]) -> None:
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "priority_bucket",
        "batch_index",
        "row_index",
        "boxscore_id",
        "year",
        "game_date",
        "away_team",
        "home_team",
        "attempts",
        "selected_count",
        "captured_count",
        "batch_csv",
    ]
    with plan_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            rec = attempts.get(row["boxscore_id"], {})
            writer.writerow(
                {
                    "priority_bucket": priority_bucket(row),
                    "batch_index": row["_batch_index"],
                    "row_index": row["_row_index"],
                    "boxscore_id": row["boxscore_id"],
                    "year": row.get("year"),
                    "game_date": row.get("game_date"),
                    "away_team": row.get("away_team"),
                    "home_team": row.get("home_team"),
                    "attempts": rec.get("attempts", 0),
                    "selected_count": rec.get("selected_count", 0),
                    "captured_count": rec.get("captured_count", 0),
                    "batch_csv": row["_batch_csv"],
                }
            )


def write_slice(slice_path: Path, rows: list[dict[str, str]]) -> None:
    slice_path.parent.mkdir(parents=True, exist_ok=True)
    clean_rows = [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]
    with slice_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(clean_rows[0].keys()))
        writer.writeheader()
        writer.writerows(clean_rows)


def run_slice(args, slice_path: Path, label: str, log_dir: Path) -> tuple[int, Path, Path]:
    stdout_path = log_dir / f"{label}_stdout.json"
    stderr_path = log_dir / f"{label}_stderr.log"
    cmd = [
        sys.executable,
        "scripts\\run_newspapers_pre_pbp_batches.py",
        "--batch-csv",
        str(slice_path),
        "--batch-label",
        label,
        "--chunk-size",
        str(args.chunk_size),
        "--max-chunks",
        str(args.max_chunks_per_slice),
        "--port",
        str(args.port),
        "--start-edge-if-needed",
        "--pages-per-game",
        str(args.pages_per_game),
        "--max-queries-per-game",
        str(args.max_queries_per_game),
        "--chunk-timeout-seconds",
        str(args.chunk_timeout_seconds),
        "--checkpoint-timeout-seconds",
        str(args.checkpoint_timeout_seconds),
        "--checkpoint-python",
        args.checkpoint_python,
        "--quiet-chunks",
    ]
    with stdout_path.open("w", encoding="utf-8") as stdout, stderr_path.open("w", encoding="utf-8") as stderr:
        proc = subprocess.Popen(cmd, cwd=str(args.repo_root), stdout=stdout, stderr=stderr)
        returncode = proc.wait()
    return returncode, stdout_path, stderr_path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-dir", type=Path, default=DEFAULT_BATCH_DIR)
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    parser.add_argument("--supervisor-root", type=Path, default=DEFAULT_RUN_ROOT / "_priority_supervisor")
    parser.add_argument("--only-plan", action="store_true")
    parser.add_argument("--priority-year-max", type=int, default=1939)
    parser.add_argument("--include-attempted-empty", action="store_true")
    parser.add_argument("--stop-on-slice-failure", action="store_true")
    parser.add_argument("--max-slices", type=int, default=0, help="0 means no limit")
    parser.add_argument("--slice-size", type=int, default=50)
    parser.add_argument("--chunk-size", type=int, default=10)
    parser.add_argument("--max-chunks-per-slice", type=int, default=5)
    parser.add_argument("--port", type=int, default=9226)
    parser.add_argument("--pages-per-game", type=int, default=3)
    parser.add_argument("--max-queries-per-game", type=int, default=3)
    parser.add_argument("--chunk-timeout-seconds", type=int, default=2700)
    parser.add_argument("--checkpoint-timeout-seconds", type=int, default=600)
    parser.add_argument("--checkpoint-python", default="py")
    args = parser.parse_args()

    require_root(args.batch_dir, "batch_dir")
    require_root(args.run_root, "run_root")
    if not args.batch_dir.exists():
        raise FileNotFoundError(args.batch_dir)
    if not args.run_root.exists():
        raise FileNotFoundError(args.run_root)

    all_rows = read_batch_rows(args.batch_dir)
    attempts = load_attempts(args.run_root)
    remaining = []
    for row in all_rows:
        rec = attempts.get(row["boxscore_id"])
        if rec is None:
            remaining.append(row)
        elif args.include_attempted_empty and int(rec.get("captured_count") or 0) == 0:
            remaining.append(row)

    remaining.sort(key=lambda row: (priority_bucket(row), int(row["_batch_index"]), int(row["_row_index"])))

    run_id = stamp()
    supervisor_dir = args.supervisor_root / run_id
    supervisor_dir.mkdir(parents=True, exist_ok=True)
    plan_path = supervisor_dir / "remaining_priority_plan.csv"
    write_plan(plan_path, remaining, attempts)

    counts = {
        "total_queue_rows": len(all_rows),
        "attempted_rows": len(attempts),
        "remaining_rows": len(remaining),
        "remaining_1920s_1930s": sum(1 for row in remaining if priority_bucket(row) == 0),
        "remaining_other": sum(1 for row in remaining if priority_bucket(row) != 0),
        "plan_path": str(plan_path),
        "supervisor_dir": str(supervisor_dir),
    }
    summary_path = supervisor_dir / "supervisor_summary.jsonl"
    with summary_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"status": "planned", **counts}, ensure_ascii=True) + "\n")
    print(json.dumps({"status": "planned", **counts}, indent=2))

    if args.only_plan or not remaining:
        return 0

    completed_slices = 0
    for offset in range(0, len(remaining), args.slice_size):
        if args.max_slices and completed_slices >= args.max_slices:
            break
        slice_rows = remaining[offset : offset + args.slice_size]
        first_id = slice_rows[0]["boxscore_id"]
        last_id = slice_rows[-1]["boxscore_id"]
        label = f"priority_slice_{completed_slices + 1:04d}_{first_id}_through_{last_id}"
        slice_path = supervisor_dir / "slices" / f"{label}.csv"
        write_slice(slice_path, slice_rows)
        returncode, stdout_path, stderr_path = run_slice(args, slice_path, label, supervisor_dir)
        payload = {
            "status": "slice_finished" if returncode == 0 else "slice_failed",
            "slice_index": completed_slices + 1,
            "returncode": returncode,
            "first_boxscore_id": first_id,
            "last_boxscore_id": last_id,
            "slice_csv": str(slice_path),
            "stdout": str(stdout_path),
            "stderr": str(stderr_path),
        }
        with summary_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True) + "\n")
        print(json.dumps(payload, indent=2))
        completed_slices += 1
        if returncode != 0 and args.stop_on_slice_failure:
            return returncode

    final_payload = {
        "status": "complete",
        "slices_completed": completed_slices,
        "summary_jsonl": str(summary_path),
        "plan_path": str(plan_path),
    }
    with summary_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(final_payload, ensure_ascii=True) + "\n")
    print(json.dumps(final_payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
