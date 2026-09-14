"""
sota_recon/inventory.py  --  the canonical D-drive data-lake inventory

ONE command that proves what we have, where it is, that it is PRESENT, and what it is
for. The catalog DERIVES from sources.registry() -- the single source of truth for
every source the SOTA pipeline depends on -- so a newly registered source can never be
missing from the inventory (Phase-2 sync; the old hand-kept 15-list is gone). Verifies
presence and reports coverage (rows + year range) per source, plus v26 correction
provenance.

    python -m scripts.sota_recon.inventory

Outputs (under derived/validation/sota_recon_master/):
    DATA_LAKE_INVENTORY.json   - machine catalog (every path, present?, coverage)
    DATA_LAKE_INVENTORY.md     - human catalog, grouped by role

This is the auditable answer to "is everything on the D drive and accounted for?"
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import duckdb

from .sources import DATA_LAKE, latest_v26, registry

OUT_DIR = os.path.join(DATA_LAKE, "derived", "validation", "sota_recon_master")

# preferred role ordering for the human catalog; unlisted roles append alphabetically
_ROLE_ORDER = ["subject", "identity", "anchor", "authority", "oracle", "appearance",
               "negative", "team", "scoring", "context", "derived", "legacy"]


def _parquet_coverage(con, path):
    try:
        n = con.execute(f"SELECT COUNT(*) FROM '{path}'").fetchone()[0]
        yr = None
        cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()]
        ycol = "year" if "year" in cols else ("season" if "season" in cols else None)
        if ycol:
            mn, mx = con.execute(f"SELECT MIN({ycol}), MAX({ycol}) FROM '{path}'").fetchone()
            yr = f"{mn}-{mx}"
        return {"rows": int(n), "years": yr}
    except Exception as e:
        return {"error": str(e)[:120]}


def build() -> dict:
    con = duckdb.connect(); con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    items = []
    for key, src in registry().items():
        present = os.path.exists(src.path)
        rec = {"key": key, "role": src.role, "lineage": src.lineage,
               "witness_class": src.witness_class, "path": src.path,
               "present": present, "declared_years": f"{src.year_min}-{src.year_max}",
               "purpose": src.note}
        if present and not os.path.isdir(src.path):
            rec.update(_parquet_coverage(con, src.path))
            rec["size_bytes"] = os.path.getsize(src.path)
        items.append(rec)

    # v26 provenance: correction tags present (proves the corrections landed)
    v26 = latest_v26()
    prov = {}
    try:
        rows = con.execute(f"""
            SELECT recon_correction_log, COUNT(*) FROM '{v26}'
            WHERE recon_correction_log IS NOT NULL GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()
        prov = {r[0]: int(r[1]) for r in rows}
    except Exception:
        pass

    manifest = {
        "generated_at_utc": stamp,
        "data_lake_root": DATA_LAKE,
        "subject_release": v26,
        "sources_total": len(items),
        "sources_present": sum(1 for i in items if i["present"]),
        "sources_missing": [i["key"] for i in items if not i["present"]],
        "items": items,
        "v26_correction_provenance": prov,
    }
    con.close()

    os.makedirs(OUT_DIR, exist_ok=True)
    with open(os.path.join(OUT_DIR, "DATA_LAKE_INVENTORY.json"), "w") as f:
        json.dump(manifest, f, indent=2, default=str)
    _write_md(manifest)
    return manifest


def _write_md(m: dict) -> None:
    lines = ["# D-Drive Data-Lake Inventory", "",
             f"- generated: `{m['generated_at_utc']}`",
             f"- root: `{m['data_lake_root']}`",
             f"- subject: `{m['subject_release']}`",
             f"- sources present: **{m['sources_present']}/{m['sources_total']}**"
             + (f"  ⚠ MISSING: {m['sources_missing']}" if m['sources_missing'] else "  ✓ all present"),
             ""]
    by_role: dict[str, list] = {}
    for it in m["items"]:
        by_role.setdefault(it["role"], []).append(it)
    roles = [r for r in _ROLE_ORDER if r in by_role] + sorted(set(by_role) - set(_ROLE_ORDER))
    for role in roles:
        lines += [f"## {role}", "",
                  "| key | present | lineage/class | coverage | purpose |",
                  "|---|---|---|---|---|"]
        for it in by_role[role]:
            cov = (f"{it.get('rows'):,} rows {it.get('years','') or ''}" if "rows" in it
                   else it.get("error", ""))
            mark = "✓" if it["present"] else "✗ MISSING"
            lin = f"{it['lineage']}/{it['witness_class']}"
            lines.append(f"| {it['key']} | {mark} | {lin} | {cov} | {it['purpose']} |")
        lines.append("")
    if m["v26_correction_provenance"]:
        lines += ["## v26 correction provenance (recon_correction_log)", "",
                  "Proof the correction layer landed (tag → rows):", ""]
        for tag, n in m["v26_correction_provenance"].items():
            lines.append(f"- `{tag}`: {n:,}")
        lines.append("")
    with open(os.path.join(OUT_DIR, "DATA_LAKE_INVENTORY.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    m = build()
    print(f"sources present: {m['sources_present']}/{m['sources_total']}")
    if m["sources_missing"]:
        print("MISSING:", m["sources_missing"])
    print("catalog:", os.path.join(OUT_DIR, "DATA_LAKE_INVENTORY.md"))
