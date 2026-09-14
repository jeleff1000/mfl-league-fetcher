"""Full-history MapSpec collision audit: every spec vs the local planes, all years.

Phase A of docs/runbooks/supertable-collision-audit-plan-2026-08-01.md.  The 2025
audit measured the current stratum; this one widens each spec to (1920, 2025) — the
validator clips to the source registry span — so collisions OUTSIDE the licensing
stratum become visible (the `sfty` 1982-93 class).  Semantics are unchanged: same
validator, same conflict classifier, same denominator-preserving abstention rules.

One receipt per SOURCE, written the moment that source finishes, so a killed run
loses only the in-flight source and a re-run skips finished receipts (resumable).

Run from the repository root::

    python -m scripts.sota_recon.audit_mapspec_full --sources pfr_player_kicking,player_bio
    python -m scripts.sota_recon.audit_mapspec_full            # everything not yet receipted
    python -m scripts.sota_recon.audit_mapspec_full --force    # re-measure even if receipted

This is an audit artifact only.  It never edits a v26 plane or a witness atom.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import replace
from pathlib import Path

from . import audit_mapspec_2025 as audit
from . import sources as S
from .witness_map import WITNESS_MAP

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
RECEIPT_DIR = OUT / "mapping_audit_full"
FULL_YEARS = (1920, 2025)


def _json_safe(o):
    """DuckDB hands back Decimal for some aggregate medians (the scoring-log shape);
    a receipt writer that crashes on them loses the whole source's audit silently."""
    from decimal import Decimal
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


def _run_source_full(item: tuple[str, list]) -> list[dict]:
    """Validate one source over its full registry overlap.

    The spec's DECLARED validation_years are recorded on every row before being
    overridden: they encode measured knowledge (broken/absent strata), and the merge
    marks known-finding overlap from them instead of silently excluding the years.
    """
    from .witness_map import validate

    source, specs = item
    declared = {(s.source_key, s.source_table or "", s.v26_col):
                (list(s.validation_years) if s.validation_years else None)
                for s in specs}
    rows = validate([replace(s, validation_years=FULL_YEARS) for s in specs])
    for row in rows:
        key = (row.get("source"), row.get("source_table", "") or "",
               row.get("v26_col"))
        row["declared_validation_years"] = declared.get(key)
    return rows


def receipt_path(source: str) -> Path:
    return RECEIPT_DIR / f"{source}.json"


def enrich_cells(row: dict) -> None:
    """Attach cell-level collision magnitudes to a spec row.

    The 2025 classifier's first-nonzero-category-wins rule is honest on one sparse
    year but wrong at full stratum: over 90+ years nearly every spec accrues SOME
    rows in every category, so `passing_yards` at 98.4% agreement read as a
    zero-direction conflict off 33 cells in 8,666 (pilot, 2026-08-01).  Magnitudes
    are the signal; the class is only a headline over them.

    source_exceeds_us / we_exceed_source are tolerance-gated and DISJOINT, and
    together cover every compared disagreement; the zero-direction counts are
    diagnostic SUBSETS of them, never added on top.
    """
    n = row.get("n") or 0
    ap = row.get("agree_pct")
    if "source_exceeds_us" in row:
        src_gt = row.get("source_exceeds_us") or 0
        us_gt = row.get("we_exceed_source") or 0
        conflict = src_gt + us_gt
        row["conflict_directional"] = True
    else:
        # week-grain / static / referee paths report only n + agree_pct
        conflict = round(n * (1 - ap)) if (n and ap is not None) else 0
        row["conflict_directional"] = False
    row["value_conflict_cells"] = conflict
    row["backfill_cells"] = ((row.get("n_we_hold_no_row") or 0)
                             + (row.get("n_we_hold_null") or 0))
    row["stored_zero_conflict_cells"] = row.get("source_nonzero_supertable_zero") or 0
    row["source_zero_conflict_cells"] = row.get("source_zero_supertable_nonzero") or 0
    informative = row.get("informative_n")
    if informative is None:
        informative = n
    row["conflict_share"] = (round(conflict / informative, 4) if informative else None)


def classify_full(row: dict) -> str:
    """Magnitude-aware headline class for a full-stratum row.

    Requires enrich_cells() to have run.  The dominant lane by CELL COUNT names the
    row; subordinate lanes stay visible in the cell fields and are never erased.
    """
    if str(row.get("verdict", "")).startswith("BROKEN"):
        return "MAPPING_DEFECT_SQL"
    if row.get("verdict") in {"NO-OVERLAP", "NO-MODERN-STRATUM"} or not row.get("n"):
        return "NO_OVERLAP"
    conflict = row["value_conflict_cells"]
    backfill = row["backfill_cells"]
    if not conflict and not backfill:
        return "AGREES_ZERO_BASE" if row.get("informative_n") == 0 else "AGREES"
    if backfill > conflict:
        return "SUPERTABLE_NULL_OR_MISSING"
    if not row["conflict_directional"]:
        return "VALUE_CONFLICT_UNDIRECTED"
    src_gt = row.get("source_exceeds_us") or 0
    us_gt = row.get("we_exceed_source") or 0
    # zero-direction refinement only when it accounts for the bulk of the conflict
    if row["stored_zero_conflict_cells"] * 2 >= conflict and row["stored_zero_conflict_cells"]:
        return "SUPERTABLE_ZERO_SOURCE_POSITIVE"
    if row["source_zero_conflict_cells"] * 2 >= conflict and row["source_zero_conflict_cells"]:
        return "SOURCE_ZERO_OR_BLANK_SUPERTABLE_POSITIVE"
    if src_gt and us_gt:
        return "TWO_SIDED_VALUE_CONFLICT"
    return "SOURCE_EXCEEDS_SUPERTABLE" if src_gt else "SUPERTABLE_EXCEEDS_SOURCE"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sources", default="",
                    help="comma-separated source keys; default = all without a receipt")
    ap.add_argument("--exclude", default="",
                    help="comma-separated source keys to leave out (recorded, not silent)")
    ap.add_argument("--force", action="store_true",
                    help="re-measure sources that already have a receipt")
    ap.add_argument("--workers", type=int, default=4)
    args = ap.parse_args()

    import duckdb
    con = duckdb.connect()
    try:
        def cols(path: str) -> set[str]:
            p = Path(path).as_posix()
            return {r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{p}')").fetchall()}
        audit._SEASON_COLS = cols(S.v26_plane("season"))
        audit._CAREER_COLS = cols(S.v26_plane("career"))
    finally:
        con.close()

    groups: dict[str, list] = defaultdict(list)
    for spec in WITNESS_MAP:
        groups[spec.source_key].append(spec)

    if args.sources:
        selected = [s.strip() for s in args.sources.split(",") if s.strip()]
        unknown = [s for s in selected if s not in groups]
        if unknown:
            raise SystemExit(f"unknown sources: {unknown}")
    else:
        selected = sorted(groups)
    if args.exclude:
        excluded = [s.strip() for s in args.exclude.split(",") if s.strip()]
        unknown = [s for s in excluded if s not in groups]
        if unknown:
            raise SystemExit(f"unknown exclusions: {unknown}")
        selected = [s for s in selected if s not in excluded]
        # no silent caps: an exclusion is a scope decision and must be visible
        print(json.dumps({"excluded_by_decision": excluded,
                          "excluded_spec_count": sum(len(groups[s]) for s in excluded)}))
    skipped = []
    if not args.force:
        skipped = [s for s in selected if receipt_path(s).exists()]
        selected = [s for s in selected if not receipt_path(s).exists()]
    if skipped:
        print(f"skipping {len(skipped)} already-receipted sources (use --force to redo)")
    if not selected:
        print("nothing to do")
        return 0

    RECEIPT_DIR.mkdir(parents=True, exist_ok=True)
    errors: list[dict] = []
    workers = min(args.workers, len(selected))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(_run_source_full, (s, groups[s])): s for s in selected}
        for future, source in [(f, s) for f, s in futures.items()]:
            try:
                rows = future.result()
            except Exception as exc:
                errors.append({"source": source, "error": repr(exc)})
                print(json.dumps({"source": source, "error": repr(exc)}))
                continue
            for row in rows:
                row["audit_years"] = list(FULL_YEARS)
                row["target_plane"] = audit._plane_for(row)
                enrich_cells(row)
                row["conflict_class"] = classify_full(row)
                row["newspaper_atoms_preserved"] = True
            receipt = {
                "audit": "mapspec_vs_supertable_full",
                "source": source,
                "years": list(FULL_YEARS),
                "spec_count": len(groups[source]),
                "rows": rows,
                "class_counts": dict(Counter(r["conflict_class"] for r in rows)),
                "plane_counts": dict(Counter(r["target_plane"] for r in rows)),
                "notes": [
                    "Full-stratum widening of audit_mapspec_2025; validator and classifier semantics unchanged.",
                    "declared_validation_years is retained per row so known-broken strata are marked at merge, never silently excluded.",
                    "Source NULL/blank cells remain denominator-preserving abstentions.",
                    "Newspaper atoms are immutable witness inputs.",
                ],
            }
            receipt_path(source).write_text(
                json.dumps(receipt, indent=1, default=_json_safe), encoding="utf-8")
            print(json.dumps({"receipt": str(receipt_path(source)),
                              "source": source, "rows": len(rows),
                              "class_counts": receipt["class_counts"]}))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
