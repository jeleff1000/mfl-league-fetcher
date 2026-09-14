#!/usr/bin/env python
"""Run the newspaper atom conveyor behind one stable command.

This wrapper exists to keep D-drive conveyor work auditable while avoiding a
long series of tiny approval prompts. It calls the existing stations, captures
their JSON receipts, writes a run receipt to D, and optionally applies an
explicit/safe local decision input. It never writes to Fly or the live
supertable.
"""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_TOOL_MIRROR = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor")
LOCAL_REPO_SCRIPT_ROOT = Path(__file__).resolve().parent
MIRRORED_STATION_SCRIPTS = [
    "run_newspaper_conveyor_once.py",
    "run_newspaper_current_route_resolvers.py",
    "build_newspaper_llm_read_state.py",
    "ingest_newspaper_llm_reviews.py",
    "materialize_newspaper_llm_review_rows.py",
    "build_newspaper_llm_promotion_packages.py",
    "compare_newspaper_promotions_to_v26.py",
    "build_newspaper_conveyor_action_queues.py",
    "build_newspaper_ocr_followup_preps.py",
    "build_newspaper_ocr_followup_round_manifest.py",
    "build_newspaper_ocr_followup_review_packets.py",
    "build_newspaper_semantic_followup_preps.py",
    "build_newspaper_quality_review_preps.py",
    "build_newspaper_promotion_review_preps.py",
    "build_newspaper_review_decision_ledger.py",
    "build_newspaper_safe_decision_input.py",
    "build_newspaper_review_decision_apply.py",
    "build_newspaper_lineup_route_resolution_prep.py",
    "build_newspaper_event_detail_resolution_prep.py",
    "build_newspaper_semantic_game_key_resolution_prep.py",
    "build_newspaper_quality_lane_resolution_prep.py",
    "build_newspaper_current_route_followup_dossier.py",
    "build_newspaper_conveyor_status_report.py",
]


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean_label(value: str) -> str:
    allowed = []
    for char in value.strip():
        if char.isalnum() or char in {"_", "-", "."}:
            allowed.append(char)
        else:
            allowed.append("_")
    return "".join(allowed).strip("_") or "newspaper_conveyor"


def discover_script_root(explicit: Path | None) -> Path:
    if explicit:
        return explicit
    here = Path(__file__).resolve().parent
    candidates = [
        here,
        Path.cwd() / "scripts",
        LOCAL_REPO_SCRIPT_ROOT,
    ]
    for candidate in candidates:
        if (candidate / "build_newspaper_llm_read_state.py").exists():
            return candidate
    return here


def extract_json(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
        return payload if isinstance(payload, dict) else {"json": payload}
    except json.JSONDecodeError:
        pass

    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            payload = json.loads(text[start : end + 1])
            return payload if isinstance(payload, dict) else {"json": payload}
        except json.JSONDecodeError:
            return {"stdout_parse_error": "could_not_parse_json_object"}
    return {"stdout_parse_error": "no_json_object_found"}


class ConveyorRun:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.script_root = discover_script_root(args.script_root)
        self.run_id = f"{stamp()}_{clean_label(args.label_prefix)}_conveyor_once"
        self.run_dir = args.root / "conveyor_refresh_runs" / self.run_id
        self.steps: list[dict[str, Any]] = []
        self.outputs: dict[str, dict[str, Any]] = {}

    def write_summary(self, status: str, error: str = "") -> None:
        payload = {
            "created_at_utc": iso_now(),
            "status": status,
            "error": error,
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "mode": self.args.mode,
            "script_root": str(self.script_root),
            "root": str(self.args.root),
            "db_path": str(self.args.db_path),
            "packet_run_dir": str(self.args.packet_run_dir or ""),
            "review_output_dir": str(self.args.review_output_dir or ""),
            "steps": self.steps,
            "outputs": self.outputs,
        }
        self.run_dir.mkdir(parents=True, exist_ok=True)
        (self.run_dir / "conveyor_once_summary.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def mirror_tools(self) -> None:
        if self.args.no_mirror:
            return
        self.args.tool_mirror_root.mkdir(parents=True, exist_ok=True)
        for script_name in MIRRORED_STATION_SCRIPTS:
            source = self.script_root / script_name
            if not source.exists():
                continue
            target = self.args.tool_mirror_root / script_name
            if source.resolve() == target.resolve():
                continue
            shutil.copy2(source, target)

    def script_path(self, script_name: str) -> Path:
        path = self.script_root / script_name
        if not path.exists():
            raise FileNotFoundError(f"Missing conveyor station script: {path}")
        return path

    def run_step(self, key: str, label: str, script_name: str, step_args: list[Any]) -> dict[str, Any]:
        script = self.script_path(script_name)
        cmd = [sys.executable, str(script), *[str(item) for item in step_args]]
        print(f"\n== {label} ==")
        print(" ".join(cmd))
        result = subprocess.run(
            cmd,
            cwd=str(self.script_root.parent),
            capture_output=True,
            text=True,
            check=False,
        )
        payload = extract_json(result.stdout)
        record = {
            "key": key,
            "label": label,
            "script": str(script),
            "returncode": result.returncode,
            "json": payload,
            "stdout_tail": result.stdout[-4000:],
            "stderr_tail": result.stderr[-4000:],
        }
        self.steps.append(record)
        self.outputs[key] = payload
        if result.stdout.strip():
            print(result.stdout.strip()[-2000:])
        if result.stderr.strip():
            print(result.stderr.strip()[-2000:], file=sys.stderr)
        if result.returncode != 0:
            self.write_summary("failed", f"{label} failed with exit code {result.returncode}")
            raise SystemExit(result.returncode)
        return payload

    def step_label(self, station: str) -> str:
        return clean_label(f"{self.args.label_prefix}_{station}_{stamp()}")

    def run_status(self) -> dict[str, Any]:
        return self.run_step(
            "status",
            "Write conveyor status report",
            "build_newspaper_conveyor_status_report.py",
            [
                "--root",
                self.args.root,
                "--db-path",
                self.args.db_path,
                "--label",
                self.step_label("status"),
            ],
        )

    def run_refresh(self) -> None:
        common_packet_args: list[Any] = []
        if self.args.packet_run_dir:
            common_packet_args.extend(["--packet-run-dir", self.args.packet_run_dir])
        if self.args.review_output_dir:
            common_packet_args.extend(["--review-output-dir", self.args.review_output_dir])

        if not self.args.skip_read_state:
            self.run_step(
                "read_state",
                "Refresh LLM read state",
                "build_newspaper_llm_read_state.py",
                [
                    *common_packet_args,
                    "--label",
                    self.step_label("read_state"),
                ],
            )

        ingest = self.run_step(
            "ingest",
            "Ingest LLM review JSON",
            "ingest_newspaper_llm_reviews.py",
            [
                *common_packet_args,
                "--db-path",
                self.args.db_path,
                "--label",
                self.step_label("ingest"),
            ],
        )
        ingest_run_id = ingest.get("ingest_run_id", "")

        materialize = self.run_step(
            "materialize",
            "Materialize reviewed atom rows",
            "materialize_newspaper_llm_review_rows.py",
            [
                "--db-path",
                self.args.db_path,
                "--ingest-run-id",
                ingest_run_id,
                "--label",
                self.step_label("materialize"),
            ],
        )

        package = self.run_step(
            "packages",
            "Build promotion packages",
            "build_newspaper_llm_promotion_packages.py",
            [
                "--db-path",
                self.args.db_path,
                "--ingest-run-id",
                ingest_run_id or materialize.get("ingest_run_id", ""),
                "--label",
                self.step_label("packages"),
            ],
        )
        package_run_id = package.get("package_run_id", "")

        comparison_csv = ""
        if not self.args.skip_v26_compare:
            comparison = self.run_step(
                "v26_compare",
                "Compare packages to v26",
                "compare_newspaper_promotions_to_v26.py",
                [
                    "--newspaper-db",
                    self.args.db_path,
                    "--package-run-id",
                    package_run_id,
                    "--label",
                    self.step_label("v26_compare"),
                ],
            )
            comparison_dir = Path(str(comparison.get("output_dir") or ""))
            comparison_csv = str(comparison_dir / "package_comparison.csv") if comparison_dir else ""

        action_args: list[Any] = [
            "--db-path",
            self.args.db_path,
            "--ingest-run-id",
            ingest_run_id,
            "--package-run-id",
            package_run_id,
            "--label",
            self.step_label("action_queue"),
        ]
        if comparison_csv:
            action_args.extend(["--v26-comparison-csv", comparison_csv])
        action = self.run_step(
            "action_queue",
            "Build conveyor action queue",
            "build_newspaper_conveyor_action_queues.py",
            action_args,
        )
        action_queue_run_id = action.get("action_queue_run_id", "")

        ocr_prep = self.run_step(
            "ocr_prep",
            "Prepare OCR follow-up lane",
            "build_newspaper_ocr_followup_preps.py",
            [
                "--db-path",
                self.args.db_path,
                "--action-queue-run-id",
                action_queue_run_id,
                "--label",
                self.step_label("ocr_prep"),
            ],
        )

        if not self.args.skip_ocr_manifest:
            manifest = self.run_step(
                "ocr_manifest",
                "Build OCR follow-up manifest",
                "build_newspaper_ocr_followup_round_manifest.py",
                [
                    "--db-path",
                    self.args.db_path,
                    "--ocr-followup-prep-run-id",
                    ocr_prep.get("ocr_followup_prep_run_id", ""),
                    "--label",
                    self.step_label("ocr_manifest"),
                ],
            )
            if self.args.build_ocr_review_packets:
                review_args: list[Any] = [
                    "--db-path",
                    self.args.db_path,
                    "--manifest",
                    manifest.get("manifest_path", ""),
                    "--packet-docs",
                    self.args.ocr_packet_docs,
                    "--max-ocr-chars",
                    self.args.max_ocr_chars,
                    "--label",
                    self.step_label("ocr_review_packets"),
                ]
                if self.args.max_ocr_review_docs:
                    review_args.extend(["--max-docs", self.args.max_ocr_review_docs])
                self.run_step(
                    "ocr_review_packets",
                    "Build OCR follow-up review packets",
                    "build_newspaper_ocr_followup_review_packets.py",
                    review_args,
                )

        semantic_prep = self.run_step(
            "semantic_prep",
            "Prepare semantic follow-up lane",
            "build_newspaper_semantic_followup_preps.py",
            [
                "--db-path",
                self.args.db_path,
                "--action-queue-run-id",
                action_queue_run_id,
                "--label",
                self.step_label("semantic_prep"),
            ],
        )
        quality_prep = self.run_step(
            "quality_prep",
            "Prepare quality-review lane",
            "build_newspaper_quality_review_preps.py",
            [
                "--db-path",
                self.args.db_path,
                "--action-queue-run-id",
                action_queue_run_id,
                "--package-run-id",
                package_run_id,
                "--label",
                self.step_label("quality_prep"),
            ],
        )
        promotion_prep = self.run_step(
            "promotion_prep",
            "Prepare promotion-review lane",
            "build_newspaper_promotion_review_preps.py",
            [
                "--db-path",
                self.args.db_path,
                "--action-queue-run-id",
                action_queue_run_id,
                "--package-run-id",
                package_run_id,
                "--label",
                self.step_label("promotion_prep"),
            ],
        )

        ledger_args: list[Any] = [
            "--db-path",
            self.args.db_path,
            "--ocr-followup-prep-run-id",
            ocr_prep.get("ocr_followup_prep_run_id", ""),
            "--semantic-followup-prep-run-id",
            semantic_prep.get("semantic_followup_prep_run_id", ""),
            "--action-queue-run-id",
            action_queue_run_id,
            "--quality-review-prep-run-id",
            quality_prep.get("quality_review_prep_run_id", ""),
            "--promotion-review-prep-run-id",
            promotion_prep.get("promotion_review_prep_run_id", ""),
            "--label",
            self.step_label("decision_ledger"),
        ]
        if self.args.decision_input_csv:
            ledger_args.extend(["--decision-input-csv", self.args.decision_input_csv])
        ledger = self.run_step(
            "decision_ledger",
            "Build review decision ledger",
            "build_newspaper_review_decision_ledger.py",
            ledger_args,
        )
        ledger_run_id = ledger.get("decision_ledger_run_id", "")

        decision_input_csv = str(self.args.decision_input_csv or "")
        if self.args.build_safe_decision_input or self.args.apply_safe_decision_input:
            safe_args: list[Any] = [
                "--db-path",
                self.args.db_path,
                "--decision-ledger-run-id",
                ledger_run_id,
                "--label",
                self.step_label("safe_decision_input"),
            ]
            for boxscore_id in self.args.hold_boxscore_id:
                safe_args.extend(["--hold-boxscore-id", boxscore_id])
            safe = self.run_step(
                "safe_decision_input",
                "Build safe local decision input",
                "build_newspaper_safe_decision_input.py",
                safe_args,
            )
            decision_input_csv = str(safe.get("decision_input_csv") or decision_input_csv)

        should_apply = bool(self.args.apply_decision_input or self.args.apply_safe_decision_input)
        if should_apply:
            apply_args: list[Any] = [
                "--db-path",
                self.args.db_path,
                "--decision-ledger-run-id",
                ledger_run_id,
                "--label",
                self.step_label("decision_apply"),
            ]
            if decision_input_csv:
                apply_args.extend(["--decision-input-csv", decision_input_csv])
            if self.args.auto_accept_promotion_review_ready:
                apply_args.append("--auto-accept-promotion-review-ready")
            self.run_step(
                "decision_apply",
                "Apply decisions locally",
                "build_newspaper_review_decision_apply.py",
                apply_args,
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["refresh", "status"], default="refresh")
    parser.add_argument("--status-only", action="store_true", help="Alias for --mode status.")
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--script-root", type=Path, default=None)
    parser.add_argument("--tool-mirror-root", type=Path, default=DEFAULT_TOOL_MIRROR)
    parser.add_argument("--packet-run-dir", type=Path, default=None)
    parser.add_argument("--review-output-dir", type=Path, default=None)
    parser.add_argument("--label-prefix", default="newspaper_conveyor")
    parser.add_argument("--skip-read-state", action="store_true")
    parser.add_argument("--skip-v26-compare", action="store_true")
    parser.add_argument("--skip-ocr-manifest", action="store_true")
    parser.add_argument("--build-ocr-review-packets", action="store_true")
    parser.add_argument("--max-ocr-review-docs", type=int, default=0)
    parser.add_argument("--ocr-packet-docs", type=int, default=2)
    parser.add_argument("--max-ocr-chars", type=int, default=50000)
    parser.add_argument("--decision-input-csv", type=Path, default=None)
    parser.add_argument("--build-safe-decision-input", action="store_true")
    parser.add_argument("--apply-decision-input", action="store_true")
    parser.add_argument("--apply-safe-decision-input", action="store_true")
    parser.add_argument("--auto-accept-promotion-review-ready", action="store_true")
    parser.add_argument("--hold-boxscore-id", action="append", default=[])
    parser.add_argument("--no-mirror", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.status_only:
        args.mode = "status"

    run = ConveyorRun(args)
    try:
        run.mirror_tools()
        if args.mode == "status":
            run.run_status()
        else:
            run.run_refresh()
            run.run_status()
        run.write_summary("complete")
    except Exception as exc:
        run.write_summary("failed", f"{type(exc).__name__}: {exc}")
        raise

    print(
        json.dumps(
            {
                "status": "complete",
                "run_id": run.run_id,
                "run_dir": str(run.run_dir),
                "summary_path": str(run.run_dir / "conveyor_once_summary.json"),
                "status_report_dir": run.outputs.get("status", {}).get("output_dir", ""),
                "step_count": len(run.steps),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
