"""Emit a small, immutable-input preflight receipt for the position closeout."""

from __future__ import annotations

import hashlib
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
LAKE_ROOT = Path(r"D:\league-history-data\nfl")
OUTPUT = REPO_ROOT / "docs/audits/sota-recon/position/closeout-preflight.json"

REQUIRED_REPO_PATHS = [
    "pyproject.toml",
    "scripts/sota_recon/sources.py",
    "scripts/sota_recon/source_lineage.py",
    "scripts/sota_recon/witness_map.py",
    "scripts/sota_recon/build_position_declaration.py",
    "scripts/sota_recon/extract_nflcom_positions.py",
    "scripts/sota_recon/witness_gate/position_taxonomy.py",
    "scripts/sota_recon/witness_gate/position_law.py",
    "scripts/sota_recon/witness_gate/contracts/position_taxonomy.v1.json",
    "scripts/sota_recon/witness_gate/contracts/witness_locks.v1.json",
    "scripts/sota_recon/test_position_law.py",
    "scripts/sota_recon/test_position_wire.py",
    "docs/closure-scoreboard.json",
    "docs/column-dossier.json",
]

REQUIRED_LAKE_PATHS = [
    r"derived\validation\sota_recon_master\pfr_season_position_declaration.parquet",
    r"derived\validation\sota_recon_master\nflcom_player_page_positions.parquet",
]


def _run_git(repo: Path, *args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()


def _sha256(path: Path) -> str | None:
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _path_record(root: Path, relative: str) -> dict:
    path = root / relative
    return {
        "path": relative,
        "exists": path.exists(),
        "kind": "file" if path.is_file() else "directory" if path.is_dir() else None,
        "size": path.stat().st_size if path.is_file() else None,
        "sha256": _sha256(path),
    }


def build_receipt(repo_root: Path = REPO_ROOT, lake_root: Path = LAKE_ROOT) -> dict:
    status = _run_git(repo_root, "status", "--short")
    dirty_paths = [line[3:] for line in status.splitlines() if len(line) >= 4]
    focused_command = [
        "python",
        "-m",
        "pytest",
        "scripts/sota_recon/test_position_law.py",
        "scripts/sota_recon/test_position_wire.py",
        "-q",
    ]
    return {
        "schema_version": "position-closeout-preflight.v1",
        "scope": "position-closeout",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "branch": _run_git(repo_root, "branch", "--show-current"),
        "head": _run_git(repo_root, "rev-parse", "HEAD"),
        "worktree_clean": not bool(status),
        "dirty_paths": dirty_paths,
        "required_paths": {
            "repository": [_path_record(repo_root, item) for item in REQUIRED_REPO_PATHS],
            "lake": [_path_record(lake_root, item) for item in REQUIRED_LAKE_PATHS],
        },
        "focused_tests": {
            "command": focused_command,
            "status": "TIMEOUT_64S_NO_OUTPUT",
            "note": "Existing focused suite was attempted by the takeover preflight and exceeded the command timeout.",
        },
        "authority": {
            "position_declaration": str(lake_root / REQUIRED_LAKE_PATHS[0]),
            "nflcom_position": str(lake_root / REQUIRED_LAKE_PATHS[1]),
        },
    }


def main() -> None:
    receipt = build_receipt()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(OUTPUT), "head": receipt["head"], "dirty_paths": len(receipt["dirty_paths"])}, indent=2))


if __name__ == "__main__":
    main()
