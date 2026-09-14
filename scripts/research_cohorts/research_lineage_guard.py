"""Static fail-fast guard for research-matchup cache lineage writes."""

from __future__ import annotations

from pathlib import Path
import re

from research_lineage_policy import CANONICAL_RESEARCH_MATCHUP_CACHE_KEY


_CACHE_SAVE = re.compile(r"actions/cache/save@")
_CACHE_SAVE_BLOCK = re.compile(
    r"uses:\s*actions/cache/save@[^\n]*\n(?P<body>(?:(?!\n\s*-\s).){0,800})",
    re.DOTALL,
)
_CACHE_KEY = re.compile(r"^\s*key:\s*(.+?)\s*$", re.MULTILINE)
_FIXED_ENV_KEY = re.compile(
    r"^\s*CANONICAL_CACHE_KEY:\s*['\"]?([^'\"\s]+)['\"]?\s*$",
    re.MULTILINE,
)


def workflow_files(root: Path) -> list[Path]:
    workflows = root / ".github" / "workflows"
    return sorted(p for p in workflows.glob("*.y*ml") if p.is_file())


def check_workflow_text(path: Path, text: str) -> list[str]:
    """Return violations for workflows that write Actions research caches."""
    name = path.name.lower()
    is_research_surface = (
        name.startswith("research_")
        or name.startswith("research-")
        or name.startswith("rebuild_research_")
        or name.startswith("seed_research_")
    )
    if not is_research_surface or not _CACHE_SAVE.search(text):
        return []

    violations: list[str] = []
    blocks = _CACHE_SAVE_BLOCK.findall(text)
    for block in blocks:
        keys = _CACHE_KEY.findall(block)
        if not keys:
            violations.append(f"{path}: cache save has no explicit canonical key")
        for raw_key in keys:
            key = raw_key.strip().strip('"').strip("'")
            if key == "${{ env.CANONICAL_CACHE_KEY }}":
                fixed = _FIXED_ENV_KEY.search(text)
                if fixed and fixed.group(1) == CANONICAL_RESEARCH_MATCHUP_CACHE_KEY:
                    continue
            if key != CANONICAL_RESEARCH_MATCHUP_CACHE_KEY:
                violations.append(
                    f"{path}: cache save key {key!r} is not the frozen canonical key"
                )
    for block in blocks:
        if "output_cache_key" in block:
            violations.append(f"{path}: caller-selected output_cache_key is forbidden")
        if "${{ github.run_id }}" in block:
            violations.append(f"{path}: run-specific cache lineage is forbidden")
    return violations


def check_repository(root: Path) -> list[str]:
    violations: list[str] = []
    for path in workflow_files(root):
        violations.extend(check_workflow_text(path, path.read_text(encoding="utf-8")))
    return violations


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    violations = check_repository(args.root)
    if violations:
        raise SystemExit("\n".join(["RESEARCH LINEAGE POLICY VIOLATION:", *violations]))
    print("PASS: no research-matchup cache write can create a new lineage")


if __name__ == "__main__":
    main()
