"""Download a GitHub release asset with bounded retry for transient failures."""

from __future__ import annotations

import argparse
import os
import subprocess
import time
from pathlib import Path


def download_release_asset(
    *,
    release_tag: str,
    release_repository: str,
    pattern: str,
    output_dir: Path,
    attempts: int,
) -> int:
    """Download an asset, retrying only the transient GitHub CLI boundary."""
    output_dir.mkdir(parents=True, exist_ok=True)
    delay_seconds = float(os.environ.get("RELEASE_DOWNLOAD_RETRY_DELAY_SECONDS", "2"))

    for attempt in range(1, attempts + 1):
        for partial_asset in output_dir.glob(pattern):
            if partial_asset.is_file():
                partial_asset.unlink()
        result = subprocess.run(
            [
                "gh",
                "release",
                "download",
                release_tag,
                "--repo",
                release_repository,
                "--pattern",
                pattern,
                "--dir",
                str(output_dir),
            ],
            check=False,
        )
        if result.returncode == 0:
            return 0
        if attempt == attempts:
            return result.returncode

        print(
            f"Release asset download failed (attempt {attempt}/{attempts}); retrying.",
            flush=True,
        )
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release-tag", required=True)
    parser.add_argument("--release-repository", required=True)
    parser.add_argument("--pattern", required=True)
    parser.add_argument("--dir", required=True, type=Path)
    parser.add_argument("--attempts", type=int, default=3)
    args = parser.parse_args()
    if args.attempts < 1:
        parser.error("--attempts must be at least 1")

    return download_release_asset(
        release_tag=args.release_tag,
        release_repository=args.release_repository,
        pattern=args.pattern,
        output_dir=args.dir,
        attempts=args.attempts,
    )


if __name__ == "__main__":
    raise SystemExit(main())
