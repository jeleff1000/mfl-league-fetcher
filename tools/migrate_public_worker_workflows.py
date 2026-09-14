#!/usr/bin/env python3
"""One-time codemod for moving worker workflows to the public repository."""

from __future__ import annotations

import argparse
from pathlib import Path
import re


_PRIVATE_REPOSITORY = "jeleff1000/" + "yahoo_" + "oauth"
_PRIVATE_TOKEN = "PRIVATE_REPO" + "_PAT"
_PRIVATE_REF = "yahoo_" + "oauth_ref"
_PRIVATE_REF_ENV = "YAHOO_" + "OAUTH_REF"
_PUBLIC_REPOSITORY = "jeleff1000/league-history-workers"
_RETIRED_REPOSITORIES = (
    "league-history-workers/mfl-league-fetcher",
    "league-history-workers/league-history-workers",
)


def _indent(line: str) -> int:
    return len(line) - len(line.lstrip(" "))


def _remove_private_checkout_credentials(lines: list[str]) -> list[str]:
    remove: set[int] = set()
    private_checkout_indexes = [
        index
        for index, line in enumerate(lines)
        if re.search(rf"repository\s*:\s*{re.escape(_PRIVATE_REPOSITORY)}\s*$", line)
    ]
    for repository_index in private_checkout_indexes:
        repository_indent = _indent(lines[repository_index])
        with_index = repository_index - 1
        while with_index >= 0:
            candidate = lines[with_index]
            if candidate.strip() == "with:" and _indent(candidate) < repository_indent:
                break
            with_index -= 1
        if with_index < 0:
            continue

        with_indent = _indent(lines[with_index])
        block_end = with_index + 1
        while block_end < len(lines):
            candidate = lines[block_end]
            if candidate.strip() and _indent(candidate) <= with_indent:
                break
            block_end += 1

        for index in range(with_index + 1, block_end):
            stripped = lines[index].strip()
            if _indent(lines[index]) == repository_indent and (
                stripped.startswith("repository:")
                or stripped.startswith("token:")
                or stripped.startswith("ref:")
            ):
                remove.add(index)

        remaining_children = [
            lines[index]
            for index in range(with_index + 1, block_end)
            if index not in remove and lines[index].strip()
        ]
        if not remaining_children:
            remove.add(with_index)

    return [line for index, line in enumerate(lines) if index not in remove]


def _remove_yaml_key(lines: list[str], key: str) -> list[str]:
    remove: set[int] = set()
    pattern = re.compile(rf"^(\s*){re.escape(key)}:\s*(.*)$")
    for index, line in enumerate(lines):
        match = pattern.match(line)
        if not match:
            continue
        remove.add(index)
        if match.group(2):
            continue
        key_indent = len(match.group(1))
        child = index + 1
        while child < len(lines):
            candidate = lines[child]
            if candidate.strip() and _indent(candidate) <= key_indent:
                break
            remove.add(child)
            child += 1
    return [line for index, line in enumerate(lines) if index not in remove]


def transform_workflow(source: str) -> str:
    had_final_newline = source.endswith("\n")
    lines = _remove_private_checkout_credentials(source.splitlines())
    transformed = "\n".join(lines)
    transformed = transformed.replace(
        "${{ secrets." + _PRIVATE_TOKEN + " || github.token }}",
        "${{ github.token }}",
    )
    transformed = transformed.replace(
        "${{ secrets." + _PRIVATE_TOKEN + " }}",
        "${{ github.token }}",
    )
    transformed = transformed.replace(_PRIVATE_REPOSITORY, _PUBLIC_REPOSITORY)
    for repository in _RETIRED_REPOSITORIES:
        transformed = transformed.replace(repository, _PUBLIC_REPOSITORY)
    transformed = transformed.replace(_PRIVATE_REF_ENV, "WORKER_REF")
    transformed = transformed.replace(_PRIVATE_REF, "worker_ref")
    transformed = re.sub(
        r"\$\{\{\s*(?:(?:github\.event\.(?:inputs|client_payload)|inputs)\.)worker_ref"
        r"(?:\s*\|\|\s*'[^']*')?\s*\}\}",
        "main",
        transformed,
    )
    transformed = re.sub(
        r"\$\{\{\s*(?:needs|steps)\.[^.]+\.outputs\.worker_ref\s*\}\}",
        "main",
        transformed,
    )
    transformed = "\n".join(_remove_yaml_key(transformed.splitlines(), "worker_ref"))
    if had_final_newline:
        transformed += "\n"
    return transformed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("workflow_dir", nargs="?", type=Path, default=Path(".github/workflows"))
    args = parser.parse_args()
    changed = 0
    for pattern in ("*.yml", "*.yaml"):
        for path in sorted(args.workflow_dir.glob(pattern)):
            source = path.read_text(encoding="utf-8")
            transformed = transform_workflow(source)
            if transformed != source:
                path.write_text(transformed, encoding="utf-8", newline="\n")
                changed += 1
    print(f"Migrated {changed} workflow files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
