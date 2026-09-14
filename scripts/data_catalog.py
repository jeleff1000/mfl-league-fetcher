"""
scripts/data_catalog.py  --  THE directory of every data source we have, by category.

Problem this solves: the D: lake has ~4,000 dataset directories across sports (nfl, cfb,
fantasy), providers, and scrape scopes, plus hundreds of versioned releases / Fly snapshots
/ staging candidates. Without one organized index we keep (a) missing sources we already
have, (b) double-counting overlapping ones, and (c) rebuilding the same thing per league.

This scans the lake, collapses partition shards (year=YYYY / key=val) into LOGICAL tables,
classifies each by sport + provider + scope + CATEGORY, marks its TIER (canonical vs
versioned/snapshot/staging/tmp), reads cheap metadata (rows, and year-range + key columns
for canonical tables), and emits:

    D:/league-history-data/DATA_CATALOG.json   - complete machine inventory (every table)
    D:/league-history-data/DATA_CATALOG.md     - readable directory: canonical detailed,
                                                 intermediate summarized

The grammar the lake follows (both sports):
    {sport}/raw/{provider}/{scope}/[year=Y/]{section}/tables/{table}
        section: boxscores -> game stats | players -> player season | team_pages -> team
                 season | context -> awards/standings/ratings | logs -> game logs
Other roots: ops_data (identity/ops), derived, releases, fly_snapshots, staging, curated,
tmp/artifacts.

    python -m scripts.data_catalog            # scan + write catalog
    python -m scripts.data_catalog --rows     # also sum row counts (slower)
"""

from __future__ import annotations

import argparse
import json
import os
import re
from collections import defaultdict

import pyarrow.parquet as pq

ROOT = "D:/league-history-data"
PART = re.compile(r"^[^/]+=[^/]+$")  # a key=val partition component
# a timestamped run id (20260612T033351Z / 20260606T1840Z) => an intermediate version, not canonical
TS = re.compile(r"\d{8}T\d{4,6}Z?", re.I)
INTERMEDIATE_WORDS = ("smoke", "pilot", "candidate", "sidecar", "scaffold", "_tmp")

# section (raw scrape) -> functional category
SECTION_CATEGORY = {
    "boxscores": "game_stats",
    "players": "player_season",
    "team_pages": "team_season",
    "context": "context_awards",
    "logs": "game_logs",
}


def _logical(rel: str) -> str:
    """Collapse any key=val partition component anywhere in the path."""
    return "/".join(p for p in rel.split("/") if not PART.match(p))


def _classify(rel: str) -> dict:
    """Map a logical table path to sport / provider / scope / category / tier / table."""
    parts = rel.split("/")
    sport = parts[0] if parts else "?"
    root2 = parts[1] if len(parts) > 1 else ""

    tier, category, provider, scope, table = "canonical", "other", "", "", parts[-1]

    if root2 == "releases":
        tier, category = "versioned_release", "release"
    elif root2 == "fly_snapshots":
        tier, category = "fly_snapshot", "fly_snapshot"
    elif root2 == "staging":
        tier, category = "staging", "staging_candidate"
    elif root2 in ("tmp", "artifacts"):
        tier, category = "tmp", "tmp"
    elif root2 == "curated":
        tier, category = "canonical", "curated_recovery"
    elif root2 == "derived":
        tier, category = "canonical", "derived"
    elif root2 == "ops_data":
        tier = "canonical"
        low = rel.lower()
        category = ("identity" if any(k in low for k in ("bio", "identity", "map", "id_"))
                    else "ops")
    elif root2 == "raw":
        tier = "canonical"
        provider = parts[2] if len(parts) > 2 else ""
        section = next((s for s in SECTION_CATEGORY if f"/{s}/" in f"/{rel}/" or rel.endswith(f"/{s}")), None)
        # find the scope (component right before the section), and table (after tables/)
        if "tables" in parts:
            ti = parts.index("tables")
            table = "/".join(parts[ti + 1:]) or parts[-1]
            sec = parts[ti - 1] if ti >= 1 else ""
            section = sec if sec in SECTION_CATEGORY else section
            scope = parts[ti - 2] if ti >= 2 else ""
        low = rel.lower()
        if "pbp" in low or "play_by_play" in low:
            category = "play_by_play"
        elif "schedule" in low:
            category = "schedule"
        elif "team_games" in low:
            category = "schedule"
        elif section:
            category = SECTION_CATEGORY[section]
        else:
            category = "raw_other"
    # demote intermediate/versioned artifacts out of canonical (the "misorganized" bloat):
    # a timestamped run id or smoke/pilot/candidate/sidecar component is NOT a canonical source
    if tier == "canonical":
        comps = rel.split("/")
        if any(TS.search(c) for c in comps):
            tier = "versioned_intermediate"
        elif any(w in rel.lower() for w in INTERMEDIATE_WORDS):
            tier = "sample"
    return {"sport": sport, "provider": provider, "scope": scope,
            "category": category, "tier": tier, "table": table}


def scan(with_rows: bool = False) -> list[dict]:
    # gather physical dataset dirs -> group by logical table
    logical: dict[str, dict] = {}
    for dp, _dns, fns in os.walk(ROOT):
        pqs = [f for f in fns if f.endswith(".parquet")]
        if not pqs:
            continue
        rel = dp.replace(ROOT, "").replace("\\", "/").lstrip("/")
        log = _logical(rel)
        e = logical.setdefault(log, {"phys_dirs": 0, "files": 0, "has_combined": False,
                                     "combined_path": None})
        e["phys_dirs"] += 1
        e["files"] += len(pqs)
        comb = [f for f in pqs if "_combined" in f.lower()]
        if comb:
            e["has_combined"] = True
            e["combined_path"] = os.path.join(dp, comb[0])
        elif e["combined_path"] is None:
            e["combined_path"] = os.path.join(dp, sorted(f for f in pqs if not PART.match(f))[0]) \
                if any(not PART.match(f) for f in pqs) else os.path.join(dp, pqs[0])

    out = []
    for log, e in logical.items():
        rec = {"logical_table": log, **_classify(log),
               "phys_dirs": e["phys_dirs"], "files": e["files"],
               "has_combined": e["has_combined"]}
        if with_rows and e["combined_path"] and os.path.isfile(e["combined_path"]):
            try:
                rec["rows"] = int(pq.read_metadata(e["combined_path"]).num_rows)
            except Exception:
                rec["rows"] = None
        # cheap year-range + key cols for canonical stat/identity/schedule tables
        if (e["combined_path"] and rec["tier"] in ("canonical",)
                and rec["category"] in ("game_stats", "player_season", "team_season",
                                        "schedule", "identity", "play_by_play", "derived")):
            try:
                names = pq.read_schema(e["combined_path"]).names
                rec["n_cols"] = len(names)
                yc = next((c for c in ("season", "year", "year_id") if c in names), None)
                rec["year_col"] = yc
            except Exception:
                pass
        out.append(rec)
    return out


def write_catalog(records: list[dict]) -> tuple[str, str]:
    jpath = os.path.join(ROOT, "DATA_CATALOG.json")
    with open(jpath, "w", encoding="utf-8") as f:
        json.dump({"root": ROOT, "n_logical_tables": len(records), "tables": records},
                  f, indent=2)

    # markdown: canonical detailed, everything else summarized
    by_tier = defaultdict(list)
    for r in records:
        by_tier[r["tier"]].append(r)
    lines = ["# Data Lake Catalog", "",
             f"Root: `{ROOT}` — **{len(records)} logical tables** "
             "(partition shards collapsed). Generated by `scripts/data_catalog.py`.", "",
             "## Tier summary", ""]
    for tier in ("canonical", "sample", "versioned_intermediate", "versioned_release",
                 "fly_snapshot", "staging", "tmp"):
        n = len(by_tier.get(tier, []))
        if n:
            lines.append(f"- **{tier}**: {n} logical tables")
    lines.append("")

    # canonical, grouped sport -> category -> table
    lines += ["## Canonical sources (what we build FROM)", ""]
    canon = [r for r in records if r["tier"] == "canonical"]
    bys = defaultdict(lambda: defaultdict(list))
    for r in canon:
        bys[r["sport"]][r["category"]].append(r)
    for sport in sorted(bys):
        lines.append(f"### {sport}")
        for cat in sorted(bys[sport]):
            lines.append(f"\n**{cat}** ({len(bys[sport][cat])})\n")
            lines.append("| table | provider/scope | rows | cols | yr col | files |")
            lines.append("|---|---|---|---|---|---|")
            for r in sorted(bys[sport][cat], key=lambda x: x["logical_table"]):
                rows = f"{r['rows']:,}" if r.get("rows") is not None else ""
                ps = "/".join(x for x in (r.get("provider"), r.get("scope")) if x)
                lines.append(f"| `{r['table']}` | {ps} | {rows} | {r.get('n_cols','')} "
                             f"| {r.get('year_col','')} | {r['files']} |")
        lines.append("")

    # duplication risk: same (sport, table name) as a canonical source under >1 scope/provider.
    # These are where we double-count or pick the wrong source if we're not careful.
    dup = defaultdict(list)
    for r in canon:
        dup[(r["sport"], r["table"])].append(r)
    dups = {k: v for k, v in dup.items() if len(v) > 1}
    lines += ["## Duplication risk (same table under multiple scopes — pick ONE)", ""]
    if not dups:
        lines.append("_None at the (sport, table-name) level._\n")
    for (sport, table), rs in sorted(dups.items()):
        scopes = ", ".join(sorted(f"{x.get('provider','')}/{x.get('scope','')}".strip("/")
                                  for x in rs))
        lines.append(f"- **{sport} `{table}`** ({len(rs)}): {scopes}")
    lines.append("")

    # intermediate tiers: just family counts, not a noise dump
    lines += ["## Intermediate tiers (summarized — see JSON for full list)", ""]
    for tier in ("versioned_intermediate", "versioned_release", "fly_snapshot",
                 "staging", "sample", "tmp"):
        rs = by_tier.get(tier, [])
        if not rs:
            continue
        fams = defaultdict(int)
        for r in rs:
            fams["/".join(r["logical_table"].split("/")[:3])] += 1
        lines.append(f"### {tier} ({len(rs)})")
        for fam in sorted(fams, key=lambda x: -fams[x])[:20]:
            lines.append(f"- {fam} — {fams[fam]}")
        lines.append("")

    mpath = os.path.join(ROOT, "DATA_CATALOG.md")
    with open(mpath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return jpath, mpath


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", action="store_true", help="also read row counts (slower)")
    args = ap.parse_args()
    recs = scan(with_rows=args.rows)
    j, m = write_catalog(recs)
    tiers = defaultdict(int)
    cats = defaultdict(int)
    for r in recs:
        tiers[r["tier"]] += 1
        if r["tier"] == "canonical":
            cats[f"{r['sport']}/{r['category']}"] += 1
    print(f"{len(recs)} logical tables ->")
    for t in sorted(tiers, key=lambda x: -tiers[x]):
        print(f"  {tiers[t]:>5}  {t}")
    print("\ncanonical by sport/category:")
    for c in sorted(cats, key=lambda x: -cats[x]):
        print(f"  {cats[c]:>4}  {c}")
    print(f"\nwrote {m}\n      {j}")
