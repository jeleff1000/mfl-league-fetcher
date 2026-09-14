"""Split a release archive into GitHub-safe, lexically ordered parts."""

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("archive", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--part-size", type=int, default=1_887_436_800)
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    index = 0
    with args.archive.open("rb") as source:
        while True:
            written = 0
            part = args.output_dir / f"research-public-lake.tar.part-{index:02d}"
            with part.open("wb") as destination:
                while written < args.part_size:
                    chunk = source.read(min(8 * 1024 * 1024, args.part_size - written))
                    if not chunk:
                        break
                    destination.write(chunk)
                    written += len(chunk)
            if not written:
                part.unlink()
                break
            index += 1
    print(index)


if __name__ == "__main__":
    main()
