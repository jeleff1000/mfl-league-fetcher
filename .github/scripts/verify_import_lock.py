"""Reject import dispatches queued under a key other than their resolved Fly target."""

import argparse
import re
import sys


DB_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,62}\Z")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--lock", required=True)
    parser.add_argument("--target", required=True)
    args = parser.parse_args()
    if not DB_NAME_RE.fullmatch(args.lock) or not DB_NAME_RE.fullmatch(args.target):
        print("Import lock or target is not a valid database name.", file=sys.stderr)
        return 1
    if args.lock != args.target:
        print("Import lock does not match the resolved publication target.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
