"""Comprehensive audit of silent-failure patterns in the import path.
Find anything that could swallow errors / no-op when env vars are missing on Fly.
"""

import re
from pathlib import Path

roots = [
    "fantasy_football_data_scripts/multi_league/data_fetchers",
    "fantasy_football_data_scripts/multi_league/transformations",
    "fantasy_football_data_scripts/multi_league/core",
    "fantasy_football_data_scripts/multi_league/external_ingest",
    "fantasy_football_data_scripts/multi_league/utils",
]

skip = [
    "build_ops_cache",
    "aggregate_nfl_stats",
    "update_nfl_super_table",
    "apply_headshot",
    "backfill_fantasy_points",
    "backfill_def_modular",
    "backfill_fg_blocked",
    "backfill_headshot_urls",
    "/tests/",
    "__pycache__",
]


def audit_files():
    for root in roots:
        for fp in Path(root).rglob("*.py"):
            if any(s in fp.as_posix() for s in skip):
                continue
            yield fp


print("=" * 70)
print("PATTERN 1: MOTHERDUCK_TOKEN early-return WITHOUT backend check")
print("=" * 70)
for fp in audit_files():
    text = fp.read_text(encoding="utf-8", errors="replace")
    for m in re.finditer(r"os\.environ\.get\(\s*[\"']MOTHERDUCK_TOKEN[\"']", text):
        line_no = text[: m.start()].count("\n") + 1
        # Look at next 200 chars
        ctx = text[m.start() : m.start() + 300]
        # Check if there's a backend check ABOVE within 5 lines
        before = text[max(0, m.start() - 500) : m.start()]
        has_backend_check = "DATABASE_BACKEND" in before[-300:] or "backend" in before[-300:]
        if "if not" in ctx and ("return" in ctx[:200] or "raise" in ctx[:200]):
            tag = "[OK]" if has_backend_check else "[BAD]"
            print(f"  {tag} {fp.as_posix()}:{line_no}")
print()

print("=" * 70)
print("PATTERN 2: reader.query/query_df WITHOUT database= arg")
print("=" * 70)
for fp in audit_files():
    text = fp.read_text(encoding="utf-8", errors="replace")
    for m in re.finditer(r"\breader\.query(?:_df)?\s*\(", text):
        depth = 1
        i = m.end()
        while i < len(text) and depth > 0:
            if text[i] == "(":
                depth += 1
            elif text[i] == ")":
                depth -= 1
            i += 1
        snippet = text[m.start() : i]
        if "database=" not in snippet:
            line_no = text[: m.start()].count("\n") + 1
            print(f"  [BAD] {fp.as_posix()}:{line_no}")
print()

print("=" * 70)
print("PATTERN 3: duckdb.connect('md:...') in import path")
print("=" * 70)
for fp in audit_files():
    text = fp.read_text(encoding="utf-8", errors="replace")
    for m in re.finditer(r"duckdb\.connect\(\s*[\"']?f?[\"']md:", text):
        line_no = text[: m.start()].count("\n") + 1
        # Check if Fly path is checked first
        before = text[max(0, m.start() - 800) : m.start()]
        guarded = "DATABASE_BACKEND" in before[-500:] or "ops_cache" in before[-500:].lower()
        tag = "[OK]" if guarded else "[BAD]"
        print(f"  {tag} {fp.as_posix()}:{line_no}")
print()

print("=" * 70)
print("PATTERN 4: get_motherduck_connection() / RuntimeError on no token")
print("=" * 70)
for fp in audit_files():
    text = fp.read_text(encoding="utf-8", errors="replace")
    for m in re.finditer(r"raise RuntimeError\([\"\'].*MOTHERDUCK_TOKEN", text):
        line_no = text[: m.start()].count("\n") + 1
        # check what calls this
        print(f"  [BAD] {fp.as_posix()}:{line_no}  {text[m.start(): m.start() + 100].splitlines()[0]}")
print()

print("=" * 70)
print("PATTERN 5: bare 'except Exception: return X' in cache/resolver files")
print("=" * 70)
for fp in audit_files():
    if not any(k in fp.name.lower() for k in ["cache", "mapping", "resolver", "normalize", "merge"]):
        continue
    text = fp.read_text(encoding="utf-8", errors="replace")
    for m in re.finditer(r"except Exception(?:\s+as\s+\w+)?:\s*\n(\s+)([^\n]+)", text):
        line_no = text[: m.start()].count("\n") + 1
        body = m.group(2).strip()
        if body.startswith("return ") or body == "pass":
            print(f"  {fp.as_posix()}:{line_no}  except: {body}")
