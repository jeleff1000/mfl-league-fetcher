#!/usr/bin/env python
"""Run current-route newspaper resolver stations behind one stable command.

This runner starts from the latest local review-decision apply run, works the
current route queue through conservative resolver stations, applies generated
decision inputs locally, and emits a follow-up dossier/status report. It never
writes to Fly or canonical supertable tables.
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

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_TOOL_MIRROR = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor")

STATION_SCRIPTS = [
    "run_newspaper_current_route_resolvers.py",
    "build_newspaper_event_detail_resolution_prep.py",
    "build_newspaper_semantic_game_key_resolution_prep.py",
    "build_newspaper_lineup_route_resolution_prep.py",
    "build_newspaper_quality_lane_resolution_prep.py",
    "build_newspaper_current_route_followup_dossier.py",
    "build_newspaper_review_decision_apply.py",
    "build_newspaper_conveyor_status_report.py",
]


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(value: Any) -> str:
    return "" if value is None else str(value)


def clean_label(value: str) -> str:
    out = []
    for char in value.strip():
        out.append(char if char.isalnum() or char in {"_", "-", "."} else "_")
    return "".join(out).strip("_") or "current_route_resolvers"


def extract_json(stdout: str) -> dict[str, Any]:
    text = stdout.strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"json": parsed}
    except json.JSONDecodeError:
        pass
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(text[start : end + 1])
            return parsed if isinstance(parsed, dict) else {"json": parsed}
        except json.JSONDecodeError:
            return {"stdout_parse_error": "could_not_parse_json"}
    return {"stdout_parse_error": "no_json_object_found"}


def query_one(db_path: Path, sql: str, params: list[Any] | None = None) -> str:
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        row = con.execute(sql, params or []).fetchone()
    finally:
        con.close()
    return clean(row[0]) if row else ""


def latest_apply_run(db_path: Path) -> str:
    return query_one(
        db_path,
        """
        SELECT promotion_apply_run_id
        FROM newspaper_review.review_decision_apply_run
        ORDER BY created_at_utc DESC, promotion_apply_run_id DESC
        LIMIT 1
        """,
    )


def latest_ledger_for_apply(db_path: Path, promotion_apply_run_id: str) -> str:
    return query_one(
        db_path,
        """
        SELECT decision_ledger_run_id
        FROM newspaper_review.review_decision_apply_run
        WHERE promotion_apply_run_id = ?
        LIMIT 1
        """,
        [promotion_apply_run_id],
    )


class CurrentRouteResolverRun:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.script_root = Path(__file__).resolve().parent
        self.run_id = f"{stamp()}_{clean_label(args.label_prefix)}"
        self.run_dir = args.root / "current_route_resolver_runs" / self.run_id
        self.steps: list[dict[str, Any]] = []
        self.outputs: dict[str, dict[str, Any]] = {}

    def mirror_tools(self) -> None:
        if self.args.no_mirror:
            return
        self.args.tool_mirror_root.mkdir(parents=True, exist_ok=True)
        for script_name in STATION_SCRIPTS:
            source = self.script_root / script_name
            if not source.exists():
                continue
            target = self.args.tool_mirror_root / script_name
            if source.resolve() != target.resolve():
                shutil.copy2(source, target)

    def write_summary(self, status: str, error: str = "") -> None:
        self.run_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            "created_at_utc": iso_now(),
            "status": status,
            "error": error,
            "run_id": self.run_id,
            "run_dir": str(self.run_dir),
            "root": str(self.args.root),
            "db_path": str(self.args.db_path),
            "steps": self.steps,
            "outputs": self.outputs,
        }
        (self.run_dir / "current_route_resolver_summary.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    def step_label(self, name: str) -> str:
        return clean_label(f"{self.args.label_prefix}_{name}_{stamp()}")

    def run_step(self, key: str, script_name: str, step_args: list[Any]) -> dict[str, Any]:
        script = self.script_root / script_name
        cmd = [sys.executable, str(script), *[str(arg) for arg in step_args]]
        print(f"\n== {key} ==")
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
            self.write_summary("failed", f"{key} failed with exit code {result.returncode}")
            raise SystemExit(result.returncode)
        return payload

    def apply_decisions(self, key: str, ledger_run_id: str, decision_input_csv: str) -> str:
        payload = self.run_step(
            key,
            "build_newspaper_review_decision_apply.py",
            [
                "--db-path",
                self.args.db_path,
                "--decision-ledger-run-id",
                ledger_run_id,
                "--decision-input-csv",
                decision_input_csv,
                "--label",
                self.step_label(key),
            ],
        )
        return clean(payload.get("promotion_apply_run_id"))

    def run(self) -> None:
        current_apply = self.args.promotion_apply_run_id or latest_apply_run(self.args.db_path)
        if not current_apply:
            raise SystemExit("No local review decision apply run found.")
        ledger_run_id = self.args.decision_ledger_run_id or latest_ledger_for_apply(self.args.db_path, current_apply)
        if not ledger_run_id:
            raise SystemExit(f"No decision ledger found for apply run {current_apply}.")

        event = self.run_step(
            "event_detail_resolution",
            "build_newspaper_event_detail_resolution_prep.py",
            [
                "--db-path",
                self.args.db_path,
                "--promotion-apply-run-id",
                current_apply,
                "--decision-ledger-run-id",
                ledger_run_id,
                "--emit-hold-overrides",
                "--label",
                self.step_label("event_detail_resolution"),
            ],
        )
        current_apply = self.apply_decisions(
            "apply_after_event_detail",
            ledger_run_id,
            clean(event.get("combined_decision_input_csv")),
        )

        semantic = self.run_step(
            "semantic_game_key_resolution",
            "build_newspaper_semantic_game_key_resolution_prep.py",
            [
                "--db-path",
                self.args.db_path,
                "--promotion-apply-run-id",
                current_apply,
                "--decision-ledger-run-id",
                ledger_run_id,
                "--emit-hold-overrides",
                "--label",
                self.step_label("semantic_game_key_resolution"),
            ],
        )
        current_apply = self.apply_decisions(
            "apply_after_semantic_game_key",
            ledger_run_id,
            clean(semantic.get("combined_decision_input_csv")),
        )

        lineup = self.run_step(
            "lineup_route_resolution",
            "build_newspaper_lineup_route_resolution_prep.py",
            [
                "--db-path",
                self.args.db_path,
                "--promotion-apply-run-id",
                current_apply,
                "--include-visual-verify-holds",
                "--label",
                self.step_label("lineup_route_resolution"),
            ],
        )
        current_apply = self.apply_decisions(
            "apply_after_lineup_route",
            ledger_run_id,
            clean(lineup.get("decision_input_csv")),
        )

        quality = self.run_step(
            "quality_lane_resolution",
            "build_newspaper_quality_lane_resolution_prep.py",
            [
                "--db-path",
                self.args.db_path,
                "--promotion-apply-run-id",
                current_apply,
                "--decision-ledger-run-id",
                ledger_run_id,
                "--emit-hold-overrides",
                "--label",
                self.step_label("quality_lane_resolution"),
            ],
        )
        current_apply = self.apply_decisions(
            "apply_after_quality_lane",
            ledger_run_id,
            clean(quality.get("combined_decision_input_csv")),
        )

        if not self.args.skip_dossier:
            self.run_step(
                "followup_dossier",
                "build_newspaper_current_route_followup_dossier.py",
                [
                    "--db-path",
                    self.args.db_path,
                    "--promotion-apply-run-id",
                    current_apply,
                    "--route-lane",
                    self.args.dossier_route_lane,
                    "--label",
                    self.step_label("followup_dossier"),
                ],
            )

        self.run_step(
            "status",
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--tool-mirror-root", type=Path, default=DEFAULT_TOOL_MIRROR)
    parser.add_argument("--promotion-apply-run-id", default="")
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument("--label-prefix", default="current_route_resolvers")
    parser.add_argument("--dossier-route-lane", default="semantic_followup")
    parser.add_argument("--skip-dossier", action="store_true")
    parser.add_argument("--no-mirror", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    run = CurrentRouteResolverRun(args)
    try:
        run.mirror_tools()
        run.run()
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
                "summary_path": str(run.run_dir / "current_route_resolver_summary.json"),
                "step_count": len(run.steps),
                "final_apply_run_id": run.outputs.get("apply_after_quality_lane", {}).get("promotion_apply_run_id", ""),
                "followup_dossier_dir": run.outputs.get("followup_dossier", {}).get("output_dir", ""),
                "status_report_dir": run.outputs.get("status", {}).get("output_dir", ""),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
