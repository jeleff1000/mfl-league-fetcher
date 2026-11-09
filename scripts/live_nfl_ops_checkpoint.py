#!/usr/bin/env python3
"""Keep a checksum-pinned NFL candidate across scheduled refresh attempts."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory


LEGACY = "ops_nfl.duckdb"
GENERATION = re.compile(r"ops_nfl-\d+-\d+(?:-ops-\d{4}-\d{2}-\d{2})?")


class Release:
    def __init__(self, repository: str, tag: str):
        self.repository = repository
        self.tag = tag

    def gh(self, *args: str) -> str:
        return subprocess.check_output(["gh", *args], text=True)

    def assets(self) -> list[dict]:
        release = json.loads(self.gh("api", f"repos/{self.repository}/releases/tags/{self.tag}"))
        pages = json.loads(self.gh(
            "api", "--paginate", "--slurp", f"repos/{self.repository}/releases/{release['id']}/assets?per_page=100",
        ))
        return sorted((asset for page in pages for asset in page), key=lambda asset: asset["id"], reverse=True)

    def download(self, pattern: str, directory: Path) -> None:
        self.gh("release", "download", self.tag, "--repo", self.repository, "--pattern", pattern, "--dir", str(directory))

    def upload(self, *paths: Path) -> None:
        self.gh("release", "upload", self.tag, "--repo", self.repository, *(str(path) for path in paths))

    def delete(self, asset_id: int) -> None:
        self.gh("api", "--method", "DELETE", f"repos/{self.repository}/releases/assets/{asset_id}")


def prefix_for(name: str) -> str:
    prefix = name.removesuffix(".checkpoint.json")
    return prefix if name.endswith(".checkpoint.json") and GENERATION.fullmatch(prefix) else ""


def checkpoints(assets: list[dict]) -> list[str]:
    return [asset["name"] for asset in assets if asset["state"] == "uploaded" and prefix_for(asset["name"])]


def read_checkpoint(release: Release, name: str) -> dict:
    with TemporaryDirectory() as tmp:
        release.download(name, Path(tmp))
        metadata = json.loads((Path(tmp) / name).read_text(encoding="utf-8"))
    if metadata["prefix"] != prefix_for(name):
        raise ValueError(f"Invalid checkpoint identity in {name}")
    return metadata


def restore(release: Release, target: Path, metadata: dict | None, *, expected_sha: str = "") -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(dir=target.parent) as tmp:
        directory = Path(tmp)
        if metadata:
            prefix = metadata["prefix"] + ".duckdb"
            digest = metadata["manifest"]["sha256"]
        else:
            prefix = LEGACY
            release.download(f"{prefix}.sha256", directory)
            digest = (directory / f"{prefix}.sha256").read_text(encoding="utf-8").split()[0]
        if expected_sha and digest != expected_sha:
            raise ValueError("Durable candidate does not match the requested SHA-256")
        release.download(f"{prefix}.part-*", directory)
        parts = sorted(directory.glob(f"{prefix}.part-*"))
        if not parts or (metadata and [part.name for part in parts] != metadata["parts"]):
            raise ValueError("Durable candidate is missing release parts")
        with target.open("wb") as output:
            for part in parts:
                with part.open("rb") as source:
                    shutil.copyfileobj(source, output)
                part.unlink()
        with target.open("rb") as restored:
            if hashlib.file_digest(restored, "sha256").hexdigest() != digest:
                raise ValueError("Durable candidate SHA-256 mismatch")


def publish(release: Release, candidate: Path, scope: Path, key: str) -> None:
    from verify_live_nfl_ops_receipt import _candidate_game_rows, _normalized_scope

    suffix = f"-{key}" if key else ""
    prefix = f"ops_nfl-{os.environ['GITHUB_RUN_ID']}-{os.environ['GITHUB_RUN_ATTEMPT']}{suffix}"
    if not GENERATION.fullmatch(prefix):
        raise ValueError("Invalid candidate generation")
    with TemporaryDirectory(dir=candidate.parent) as tmp:
        directory = Path(tmp)
        part_prefix = directory / f"{prefix}.duckdb.part-"
        subprocess.run(
            ["split", "--bytes=1800M", "--numeric-suffixes=0", "--suffix-length=2", str(candidate), str(part_prefix)],
            check=True,
        )
        parts = sorted(directory.glob(f"{part_prefix.name}*"))
        scope_data = json.loads(scope.read_text(encoding="utf-8"))
        metadata = {
            "prefix": prefix,
            "scope": scope_data,
            "manifest": json.loads(candidate.with_suffix(".manifest.json").read_text(encoding="utf-8")),
            "parts": [part.name for part in parts],
            "game_rows": _candidate_game_rows(candidate, _normalized_scope(scope_data["scope"])),
        }
        release.upload(*parts)
        # A generation becomes selectable only after every part has landed.
        commit = directory / f"{prefix}.checkpoint.json"
        commit.write_text(json.dumps(metadata), encoding="utf-8")
        release.upload(commit)


def prune(release: Release) -> None:
    # Keep tiny dated checkpoints so an older retry can verify its scope
    # read-only even after a newer candidate has superseded it.
    assets = release.assets()
    current = {prefix_for(name) + ".duckdb" for name in checkpoints(assets)[:2]}
    if len(current) < 2:
        current.add(LEGACY)
    for asset in assets:
        name = asset["name"]
        prefix = name.split(".part-", 1)[0] if ".part-" in name else name.removesuffix(".sha256")
        managed = prefix == LEGACY or GENERATION.fullmatch(prefix.removesuffix(".duckdb"))
        unfinished_marker = asset["state"] == "starter" and prefix_for(name)
        if unfinished_marker or (managed and prefix not in current):
            release.delete(asset["id"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("select", "restore", "publish", "prune"))
    parser.add_argument("--key", default="")
    parser.add_argument("--state", type=Path)
    parser.add_argument("--scope", type=Path)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--sha256", default="")
    args = parser.parse_args()
    release = Release(os.environ["OPS_REPOSITORY"], os.environ["BASELINE_RELEASE_TAG"])
    if args.action == "select":
        names = checkpoints(release.assets())
        selected = next((name for name in names if args.key and name.endswith(f"-{args.key}.checkpoint.json")), None)
        metadata = read_checkpoint(release, selected or names[0]) if names else None
        mode = ("resume" if selected == names[0] else "verify") if selected else "build"
        args.state.write_text(json.dumps(metadata), encoding="utf-8")
        if selected:
            args.scope.write_text(json.dumps(metadata["scope"]), encoding="utf-8")
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write(f"mode={mode}\n")
    elif args.action == "restore":
        if args.state:
            metadata = json.loads(args.state.read_text(encoding="utf-8"))
        else:
            metadata = None
            for name in checkpoints(release.assets())[:2]:
                saved = read_checkpoint(release, name)
                if saved["manifest"]["sha256"] == args.sha256:
                    metadata = saved
                    break
        restore(release, args.database, metadata, expected_sha=args.sha256)
        if metadata:
            args.database.with_suffix(".manifest.json").write_text(json.dumps(metadata["manifest"]), encoding="utf-8")
    elif args.action == "publish":
        publish(release, args.database, args.scope, args.key)
    else:
        prune(release)


if __name__ == "__main__":
    main()
