"""Assemble the read-only reconciliation run into a promotion-ready conflict ledger."""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


def _measure_family(path: Path) -> str:
    text = f"{path.parent.name}/{path.name}".lower()
    if "pts_allow" in text or "pa_tier" in text or path.parent.name == "golden_points":
        return "fantasy_eligible_points_allowed"
    if "score" in text or "scoring" in text:
        return "total_points_or_scoring"
    return "unspecified"


def _conflict_class(path: Path, fieldnames: list[str]) -> str:
    fields = set(fieldnames)
    if "unmatched" in path.name.lower() or "unmapped" in path.name.lower():
        return "identity_or_key_gap"
    if fields & {"violation", "flag", "dup_suspects", "missing_suspects"}:
        return "structural_or_identity_violation"
    if fields & {"resid", "abs_resid", "mismatch", "score_mismatch", "conflict_rows"}:
        return "source_disagreement_or_reconstruction_residual"
    if fields & {"delta", "frac_change", "stat"}:
        return "temporal_definition_or_era_residual"
    return "review_artifact"


def build_ledger(run_dir: str | Path) -> dict:
    run_path = Path(run_dir)
    manifest = json.loads((run_path / "recon_master_manifest.json").read_text(encoding="utf-8"))
    lane_summary = []
    for lane, record in manifest.get("lanes", {}).items():
        lane_summary.append({
            "lane": lane,
            "status": record.get("status"),
            "headline": record.get("headline", ""),
            "counts": record.get("counts", {}),
            "error_type": record.get("error_type"),
            "error_message": record.get("error_message"),
        })

    conflicts = []
    for path in sorted(run_path.rglob("*.csv")):
        with path.open(newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)
            rows = list(reader)
            fields = reader.fieldnames or []
        if not rows:
            continue
        conflicts.append({
            "lane": path.parent.name,
            "artifact": str(path.relative_to(run_path)),
            "row_count": len(rows),
            "conflict_class": _conflict_class(path, fields),
            "measure_family": _measure_family(path),
            "columns": fields,
            "sample_rows": rows[:3],
        })

    return {
        "generated_at_utc": manifest.get("generated_at_utc"),
        "overall_status": manifest.get("overall_status"),
        "run_dir": str(run_path),
        "cloud_write_performed": bool(manifest.get("cloud_write_performed", False)),
        "destructive_actions_performed": bool(manifest.get("destructive_actions_performed", False)),
        "lane_summary": lane_summary,
        "conflicts": conflicts,
        "conflict_artifact_count": len(conflicts),
        "conflict_row_count": sum(item["row_count"] for item in conflicts),
    }


def write_outputs(ledger: dict, output_dir: str | Path) -> None:
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    (out / "pre-fly-reconciliation-ledger.json").write_text(
        json.dumps(ledger, indent=2, default=str) + "\n", encoding="utf-8")

    with (out / "pre-fly-reconciliation-conflicts.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = ["lane", "artifact", "row_count", "conflict_class", "measure_family", "columns"]
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in ledger["conflicts"]:
            writer.writerow({key: ",".join(row["columns"]) if key == "columns" else row.get(key)
                             for key in fields})

    lines = ["# Pre-Fly Reconciliation Ledger", "",
             f"- Run: `{ledger['run_dir']}`",
             f"- Overall status: **{ledger['overall_status']}**",
             f"- Cloud write performed: `{ledger['cloud_write_performed']}`",
             f"- Conflict artifacts: {ledger['conflict_artifact_count']}",
             f"- Conflict rows: {ledger['conflict_row_count']}", "",
             "## PA measurement boundary", "",
             "Fantasy Eligible PA is represented by `dst_points_allowed` and its `pts_allow_*` tiers. Total PA is a separate team/scoreboard measure and is never substituted for Fantasy Eligible PA.", "",
             "## Lane status", "",
             "| Lane | Status | Headline |", "|---|---|---|"]
    lines.extend(f"| {r['lane']} | {r['status']} | {r['headline']} |" for r in ledger["lane_summary"])
    lines += ["", "## Conflict artifacts", "", "| Lane | Artifact | Class | Measure | Rows |", "|---|---|---|---|---|"]
    lines.extend(
        f"| {r['lane']} | `{r['artifact']}` | {r['conflict_class']} | {r['measure_family']} | {r['row_count']:,} |"
        for r in ledger["conflicts"]
    )
    (out / "pre-fly-reconciliation-ledger.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run_dir", type=Path)
    parser.add_argument("--output-dir", type=Path, default=Path("docs"))
    args = parser.parse_args()
    ledger = build_ledger(args.run_dir)
    write_outputs(ledger, args.output_dir)
    print(json.dumps({k: ledger[k] for k in ("overall_status", "conflict_artifact_count", "conflict_row_count")}, indent=2))


if __name__ == "__main__":
    main()
