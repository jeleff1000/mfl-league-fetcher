#!/usr/bin/env python3
"""Reject executable references to the retired private worker boundary."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import subprocess


@dataclass(frozen=True)
class Violation:
    rule: str
    path: Path
    line: int


_RULES = {
    "enterprise-worker-repository": re.compile(
        r"league-history-workers/mfl-league-fetcher", re.IGNORECASE
    ),
    "private-application-checkout": re.compile(
        r"(?:repository\s*:\s*|repos/|github\.com/)jeleff1000/yahoo_oauth",
        re.IGNORECASE,
    ),
    "private-repository-token": re.compile(r"PRIVATE_REPO_PAT"),
    "private-source-ref": re.compile(r"yahoo_oauth_ref", re.IGNORECASE),
    "mutable-worker-ref": re.compile(r"worker_ref", re.IGNORECASE),
}

_EXECUTABLE_ROOTS = (
    Path(".github") / "workflows",
    Path(".github") / "actions",
    Path("frontend") / "src",
    Path("scripts"),
)
_EXECUTABLE_SUFFIXES = {".js", ".json", ".jsx", ".mjs", ".ps1", ".py", ".sh", ".ts", ".tsx", ".yaml", ".yml"}


def _candidate_files(root: Path):
    validator = Path(__file__).resolve()
    for relative_root in _EXECUTABLE_ROOTS:
        scan_root = root / relative_root
        if not scan_root.exists():
            continue
        for path in scan_root.rglob("*"):
            if path.is_file() and path.suffix.lower() in _EXECUTABLE_SUFFIXES:
                if path.resolve() != validator:
                    yield path


def _scan_git_repository(root: Path) -> list[Violation] | None:
    if not (root / ".git").exists():
        return None
    command = [
        "git",
        "-C",
        str(root),
        "grep",
        "-n",
        "-I",
        "-F",
        "-e",
        "league-history-workers/mfl-league-fetcher",
        "-e",
        "jeleff1000/yahoo_oauth",
        "-e",
        "PRIVATE_REPO_PAT",
        "-e",
        "yahoo_oauth_ref",
        "-e",
        "worker_ref",
        "--",
        ".github/workflows",
        ".github/actions",
        "frontend/src",
        "scripts",
        ":(exclude)scripts/validate_public_worker_boundaries.py",
    ]
    result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
    if result.returncode not in (0, 1):
        raise RuntimeError(result.stderr.strip() or "git grep failed")
    violations: list[Violation] = []
    for matched_line in result.stdout.splitlines():
        relative_path, line_number, line = matched_line.split(":", 2)
        for rule, pattern in _RULES.items():
            if pattern.search(line):
                violations.append(
                    Violation(rule=rule, path=Path(relative_path), line=int(line_number))
                )
    return violations


def scan_repository(root: Path) -> list[Violation]:
    """Return all forbidden executable dependencies beneath *root*."""
    git_violations = _scan_git_repository(root)
    if git_violations is not None:
        return sorted(
            git_violations,
            key=lambda item: (item.rule, str(item.path), item.line),
        )
    violations: list[Violation] = []
    for path in _candidate_files(root):
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except UnicodeDecodeError:
            continue
        for line_number, line in enumerate(lines, start=1):
            for rule, pattern in _RULES.items():
                if pattern.search(line):
                    violations.append(
                        Violation(rule=rule, path=path.relative_to(root), line=line_number)
                    )
    return sorted(violations, key=lambda item: (item.rule, str(item.path), item.line))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("root", nargs="?", type=Path, default=Path.cwd())
    args = parser.parse_args()
    violations = scan_repository(args.root.resolve())
    for violation in violations:
        print(f"{violation.rule}: {violation.path}:{violation.line}")
    if violations:
        print(f"Forbidden worker-boundary references: {len(violations)}")
        return 1
    print("Public worker boundary verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
