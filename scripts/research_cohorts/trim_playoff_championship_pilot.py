"""Trim a validated playoff/championship target manifest for a source pilot."""
from __future__ import annotations

import argparse
import json
from pathlib import Path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", type=Path, required=True)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--per-platform", type=int, default=15)
    args = ap.parse_args()
    payload = json.loads(args.input.read_text(encoding="utf-8"))
    targets = payload.get("targets", [])
    selected = []
    for platform in ("fleaflicker", "mfl", "sleeper"):
        selected.extend([r for r in targets if r.get("platform") == platform][:args.per_platform])
    if not selected:
        raise SystemExit("pilot manifest is empty")
    result = dict(payload)
    result["pilot"] = True
    result["pilot_source_target_count"] = len(selected)
    result["targets"] = selected
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({
        "pilot": True,
        "target_count": len(selected),
        "by_platform": {p: sum(r.get("platform") == p for r in selected) for p in ("fleaflicker", "mfl", "sleeper")},
    }, sort_keys=True))


if __name__ == "__main__":
    main()
