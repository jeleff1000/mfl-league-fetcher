"""One-shot audit: find reader.query/query_df calls missing `database=` arg.

The FlyReader requires `database` explicitly; legacy code from the
MotherDuck era often omits it because MotherDuck path was implicit.
Without the kwarg, FlyReader raises TypeError which exception handlers
silently swallow, leading to "cache disabled" / "stale data" fallbacks
that look like bugs elsewhere downstream.
"""

import os
import re
from pathlib import Path

bad = []
root = Path("fantasy_football_data_scripts")
for fp in root.rglob("*.py"):
    sp = fp.as_posix()
    if "/tests/" in sp or "/__pycache__/" in sp or sp.endswith("/_audit_reader_calls.py"):
        continue
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
        if "database=" not in snippet and "database =" not in snippet:
            line_no = text[: m.start()].count("\n") + 1
            preview = snippet.replace("\n", " ").strip()[:100]
            bad.append((sp, line_no, preview))

for f, n, p in bad:
    print(f"  {f}:{n}  {p}")
print(f"\nTotal: {len(bad)} sites missing database= arg")
