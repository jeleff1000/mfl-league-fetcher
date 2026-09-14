"""Lake manifest v0 — disaster-recovery inventory of D:/league-history-data (plan §24.1).

Walks the lake and emits a gzipped CSV of (relative_path, size_bytes, mtime_iso) so loss and
bitrot become detectable. Content hashing is deferred to the full manifest (v1); this v0 makes
the file universe itself versioned. Run from repo root:

    python -m scripts.sota_recon.build_lake_manifest
"""

from __future__ import annotations

import csv
import gzip
import io
import os
from datetime import datetime, timezone
from pathlib import Path

LAKE = Path("D:/league-history-data")
OUT = Path("docs/lake-manifest-v0.csv.gz")


def main() -> None:
    rows = 0
    total = 0
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["relative_path", "size_bytes", "mtime_utc"])
    for root, _dirs, files in os.walk(LAKE):
        for name in files:
            p = Path(root) / name
            try:
                stat = p.stat()
            except OSError:
                writer.writerow([str(p.relative_to(LAKE)), "STAT_FAILED", ""])
                continue
            writer.writerow(
                [
                    str(p.relative_to(LAKE)).replace("\\", "/"),
                    stat.st_size,
                    datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(timespec="seconds"),
                ]
            )
            rows += 1
            total += stat.st_size
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(OUT, "wt", encoding="utf-8", newline="") as handle:
        handle.write(buf.getvalue())
    print(f"manifest v0: {rows} files, {total / 1e9:.1f} GB -> {OUT}")


if __name__ == "__main__":
    main()
