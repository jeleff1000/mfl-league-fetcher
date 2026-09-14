#!/usr/bin/env python3
"""G10 remediation: delete live artifacts from the PUBLIC workers repo.

Artifacts on a public repo are downloadable by any logged-in GitHub user, and
these contain league/manager-identifying logs and data files. Inventory as of
2026-07-06: 16 live artifacts (8 league-import, 6 offseason-draft-results,
2 quick-import incl. league_context.json), 190 KB, 2026-06-08..2026-07-05.
See scripts/_artifacts/g10_live_artifacts_20260706.json and the G10 section
of docs/runbooks/weekly-update-system-plan.md.

DRY-RUN by default. Deletion is irreversible — run with --delete only after
Joe's sign-off:

    python scripts/g10_delete_public_artifacts.py             # dry run
    python scripts/g10_delete_public_artifacts.py --delete    # actually delete

Requires: gh CLI authenticated with delete rights on the repo.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys

REPO = "jeleff1000/league-history-workers"


def gh_json(path: str) -> dict:
    out = subprocess.run(["gh", "api", path], capture_output=True, text=True, check=True).stdout
    return json.loads(out)


def list_live_artifacts() -> list[dict]:
    artifacts, page = [], 1
    while True:
        batch = gh_json(f"repos/{REPO}/actions/artifacts?per_page=100&page={page}")["artifacts"]
        if not batch:
            break
        artifacts.extend(a for a in batch if not a["expired"])
        if len(batch) < 100:
            break
        page += 1
    return artifacts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--delete", action="store_true", help="Actually delete (default: dry run)")
    args = parser.parse_args()

    live = list_live_artifacts()
    print(f"{len(live)} live artifacts on {REPO}:")
    for a in sorted(live, key=lambda a: a["created_at"]):
        print(f"  [{a['created_at'][:10]}] {a['name']} ({a['size_in_bytes'] / 1024:.0f} KB) id={a['id']}")

    if not args.delete:
        print("\nDRY RUN — nothing deleted. Re-run with --delete after sign-off.")
        return 0

    failures = 0
    for a in live:
        result = subprocess.run(
            ["gh", "api", "-X", "DELETE", f"repos/{REPO}/actions/artifacts/{a['id']}"],
            capture_output=True, text=True,
        )
        if result.returncode == 0:
            print(f"deleted {a['name']} (id={a['id']})")
        else:
            failures += 1
            print(f"FAILED  {a['name']} (id={a['id']}): {result.stderr.strip()[:200]}")

    remaining = list_live_artifacts()
    print(f"\nverification: {len(remaining)} live artifacts remain (expected 0)")
    return 1 if failures or remaining else 0


if __name__ == "__main__":
    sys.exit(main())
