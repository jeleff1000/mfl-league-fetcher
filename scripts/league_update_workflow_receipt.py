#!/usr/bin/env python3
"""Classify the existing active-refresh receipt at the Actions publication boundary."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any


MANUAL_NO_OP_STATUSES = {"NO_FINALIZED_WEEKS", "NO_ACTIVE_RENEWAL"}


def classify_publication(receipt: Mapping[str, Any] | None, *, require_publication: bool) -> bool:
    """Never warm cache or mark UI success without an executed Fly commit."""
    if not receipt:
        raise ValueError("refresh publication receipt is missing")
    status = str(receipt.get("status") or "").upper()
    if status == "COMMITTED":
        if receipt.get("executed") is not True:
            raise ValueError("COMMITTED receipt did not execute the publication")
        return True
    if status in MANUAL_NO_OP_STATUSES:
        if require_publication:
            raise ValueError(f"UI league update did not publish: {status}")
        return False
    raise ValueError(f"refresh publication receipt is not valid: {status or 'missing status'}")


def failure_status(receipt: Mapping[str, Any] | None) -> str:
    """Keep an already committed publication recoverable if cache finalization fails."""
    status = str((receipt or {}).get("status") or "").upper()
    if status == "COMMITTED" and (receipt or {}).get("executed") is True and (
        (receipt or {}).get("source_manifest_digest") or (receipt or {}).get("source_fingerprint")
    ):
        return "committed_cache_pending"
    if status in MANUAL_NO_OP_STATUSES:
        return "incomplete_source"
    return "failed"


def _read_receipt(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--require-publication", action="store_true")
    parser.add_argument("--failure-status", action="store_true")
    args = parser.parse_args(argv)
    receipt = _read_receipt(args.receipt)
    if args.failure_status:
        print(failure_status(receipt))
        return 0
    committed = classify_publication(receipt, require_publication=args.require_publication)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            handle.write(f"committed={str(committed).lower()}\n")
    print("COMMITTED" if committed else "NO_PUBLICATION")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
