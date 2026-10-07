"""Validate an import concurrency key and its collision-safe Fly target."""

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
    # A dispatch is serialized by its caller's readable slug.  Resolution may
    # append a deterministic collision suffix when that slug belongs to a
    # different league.  The target must therefore be either the exact key or
    # that key's collision-safe child; an unrelated target remains forbidden.
    if args.target != args.lock and not args.target.startswith(f"{args.lock}_"):
        print("Import lock is not an ancestor of the resolved publication target.", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
