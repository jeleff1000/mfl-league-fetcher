"""Bounded 2025 MapSpec audit and disagreement ledger.

This is an audit artifact only.  It never edits a v26 plane or a witness atom.  The
validator's source-key denominator is retained so missing/NULL target values do not get
silently removed from the result.

Run from the repository root::

    python -m scripts.sota_recon.audit_mapspec_2025

The output is written beside the other reconciliation receipts in the NFL data lake.
"""
from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
from pathlib import Path

from .witness_map import WITNESS_MAP, validate
from . import sources as S

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
REPORT = OUT / "MAPPING_AUDIT_2025.json"


def _run_source(item: tuple[str, list]) -> list[dict]:
    """Validate one source in one worker so DuckDB can reuse its local object cache."""
    import duckdb

    source, specs = item
    # 2025 is intentionally explicit: this is the current-season audit, not the older
    # 2005-2019 licensing stratum.  Sources without 2025 rows return NO-OVERLAP.
    if not any(s.grain == "player_static" for s in specs):
        src = S.registry()[source]
        path = Path(src.path).as_posix()
        con = duckdb.connect()
        try:
            cols = {r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}', union_by_name=true)"
            ).fetchall()}
            year_col = next((c for c in ("year", "season", "year_id") if c in cols), None)
            if year_col:
                max_year = con.execute(
                    f"SELECT MAX(TRY_CAST(\"{year_col}\" AS INT)) "
                    f"FROM read_parquet('{path}', union_by_name=true)"
                ).fetchone()[0]
                if max_year is not None and int(max_year) < 2025:
                    return [dict(source=s.source_key, v26_col=s.v26_col, n=0,
                                 agree_pct=None, verdict="NO-2025-OVERLAP")
                            for s in specs]
        finally:
            con.close()
    return validate([replace(s, validation_years=(2025, 2025)) for s in specs])


def _plane_for(spec: dict) -> str:
    # The same canonical column often exists on several planes, so column membership
    # alone is ambiguous (career is a superset of many season columns).  MapSpec's
    # declared validation grain is the authoritative plane selector.
    validation_grain = spec.get("validation_grain")
    if validation_grain == "week":
        return "weekly"
    if validation_grain == "static" or spec.get("grain") == "player_static":
        return "player_bio"
    if validation_grain == "season":
        return "season"
    if spec.get("v26_col") in _CAREER_COLS:
        return "career"
    if spec.get("v26_col") in _SEASON_COLS or spec.get("agg") == "value":
        return "season"
    return "weekly"


def classify(row: dict) -> str:
    """Turn the retained denominator fields into an explicit conflict class."""
    if str(row.get("verdict", "")).startswith("BROKEN"):
        return "MAPPING_DEFECT_SQL"
    if row.get("verdict") in {"NO-OVERLAP", "NO-MODERN-STRATUM", "NO-2025-OVERLAP"}:
        return "NO_2025_OVERLAP"
    if row.get("source_zero_supertable_nonzero"):
        return "SOURCE_ZERO_OR_BLANK_SUPERTABLE_POSITIVE"
    if row.get("source_nonzero_supertable_zero"):
        return "SUPERTABLE_ZERO_SOURCE_POSITIVE"
    missing = (row.get("n_we_hold_no_row") or 0) + (row.get("n_we_hold_null") or 0)
    src_gt = row.get("source_exceeds_us") or 0
    us_gt = row.get("we_exceed_source") or 0
    if missing and not src_gt and not us_gt:
        return "SUPERTABLE_NULL_OR_MISSING"
    if src_gt and not us_gt:
        return "SOURCE_EXCEEDS_SUPERTABLE"
    if us_gt and not src_gt:
        return "SUPERTABLE_EXCEEDS_SOURCE"
    if src_gt or us_gt:
        return "TWO_SIDED_VALUE_CONFLICT"
    if row.get("zero_vs_zero"):
        return "AGREES_ZERO_BASE"
    return "AGREES"


def main() -> int:
    global _SEASON_COLS, _CAREER_COLS
    import duckdb

    def cols(path: str) -> set[str]:
        con = duckdb.connect()
        try:
            return {r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{Path(path).as_posix()}')"
            ).fetchall()}
        finally:
            con.close()

    _SEASON_COLS = cols(S.v26_plane("season"))
    _CAREER_COLS = cols(S.v26_plane("career"))
    groups: dict[str, list] = defaultdict(list)
    for spec in WITNESS_MAP:
        groups[spec.source_key].append(spec)

    workers = min(12, max(1, os.cpu_count() or 1), len(groups))
    rows: list[dict] = []
    errors: list[dict] = []
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_run_source, item): item[0] for item in groups.items()}
        for fut, source in [(f, s) for f, s in futures.items()]:
            try:
                rows.extend(fut.result())
            except Exception as exc:  # preserve the source-level failure in the receipt
                errors.append({"source": source, "error": repr(exc)})

    for row in rows:
        row["audit_year"] = 2025
        row["target_plane"] = _plane_for(row)
        row["conflict_class"] = classify(row)
        row["newspaper_atoms_preserved"] = True

    OUT.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps({
        "audit": "mapspec_vs_supertable",
        "years": [2025, 2025],
        "spec_count": len(WITNESS_MAP),
        "source_count": len(groups),
        "workers": workers,
        "rows": rows,
        "errors": errors,
        "class_counts": dict(Counter(r["conflict_class"] for r in rows)),
        "plane_counts": dict(Counter(r["target_plane"] for r in rows)),
        "notes": [
            "This receipt audits each executable MapSpec at the current 2025 stratum. Non-week MapSpecs are season-grain value paths; direct career-plane disagreement is a separate projection audit and is not silently represented as a season result.",
            "Source NULL/blank cells are abstentions unless the source shape explicitly declares blank_zero.",
            "source_zero_supertable_nonzero and source_nonzero_supertable_zero separate zero-direction conflicts; zero_vs_zero is retained as an agreement base and is never treated as a positive witness by itself.",
            "Newspaper atoms are immutable witness inputs; this report does not delete or rewrite them.",
        ],
    }, indent=1), encoding="utf-8")
    print(json.dumps({
        "report": str(REPORT),
        "specs": len(WITNESS_MAP),
        "rows": len(rows),
        "errors": errors,
        "class_counts": dict(Counter(r["conflict_class"] for r in rows)),
        "plane_counts": dict(Counter(r["target_plane"] for r in rows)),
    }, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
