"""Cross the supplied supertable column workbook with the gate and lake matrix."""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

import pandas as pd

from .source_column_matrix import canonical_alias, normalize_column_name

ROOT = Path(__file__).resolve().parents[3]
DEFAULT_XLSX = Path("D:\\sports-data\\nfl\\source\\supertable_columns.xlsx")

DEFAULT_GATE = ROOT / "docs" / "witness-column-master-matrix.json"
DEFAULT_SOURCE = ROOT / "docs" / "source-column-matrix.json"
DEFAULT_LOCAL_CROSSWALK = ROOT / "docs" / "witness-gate-crosswalk.json"
DEFAULT_OUT = ROOT / "docs"


def load_workbook(path: Path) -> dict[str, list[dict]]:
    sheets = {}
    for sheet in pd.ExcelFile(path).sheet_names:
        frame = pd.read_excel(path, sheet_name=sheet).fillna("")
        frame.columns = [str(c).strip() for c in frame.columns]
        sheets[sheet] = [
            {key: str(value).strip() if not isinstance(value, (int, float)) else value for key, value in row.items()}
            for row in frame.to_dict(orient="records") if str(row.get("Column", "")).strip()
        ]
    return sheets


def build_crosswalk(xlsx_path: Path = DEFAULT_XLSX, gate_path: Path = DEFAULT_GATE, source_path: Path = DEFAULT_SOURCE, local_crosswalk_path: Path = DEFAULT_LOCAL_CROSSWALK) -> dict:
    workbook = load_workbook(xlsx_path)
    gate = json.loads(Path(gate_path).read_text(encoding="utf-8"))
    source = json.loads(Path(source_path).read_text(encoding="utf-8"))
    local_crosswalk = json.loads(Path(local_crosswalk_path).read_text(encoding="utf-8")) if Path(local_crosswalk_path).exists() else {"rows": []}
    gate_tables = gate["tables"]
    source_by_canonical = {}
    registered_source_ids = {row["source_id"] for row in source.get("sources", [])}
    excluded_source_ids = {row["source_id"] for row in source.get("exclusions", []) if row.get("source_id")}
    observations_by_key = {}
    for observation in source["observations"]:
        source_by_canonical.setdefault(observation["canonical_column"], []).append(observation)
        observations_by_key[(observation["source_id"], observation["raw_column"])] = observation
    coverage_by_key = {}
    for row in source["coverage"]:
        coverage_by_key.setdefault((row["source_id"], row["raw_column"]), []).append(row)
    local_by_gate = {}
    for row in local_crosswalk.get("rows", []):
        if row.get("row_type") == "gate_column":
            local_by_gate.setdefault((row.get("gate_table"), row.get("gate_column")), []).append(row)

    sheet_map = {"Sheet1": "shared_identity_bio", "Sheet2": "weekly",
                 "Sheet3": "player_nfl_season", "Sheet4": "player_nfl_career"}
    # The workbook cites voting pages as one family; the registry splits them per award table.
    # A family citation resolves to "ambiguous" membership, never silently to one member.
    witness_ref_prefix_aliases = {"pfr_context:voting_pages": "pfr_context:voting_"}
    rows = []
    workbook_columns = set()
    for sheet, items in workbook.items():
        target = sheet_map.get(sheet, sheet)
        for item in items:
            column = item["Column"]
            canonical = canonical_alias(normalize_column_name(column))
            workbook_columns.add(canonical)
            gate_matches = []
            if target in gate_tables:
                gate_matches = [target] if column in gate_tables[target] else []
            elif target == "shared_identity_bio":
                gate_matches = [table for table, columns in gate_tables.items() if column in columns]
            observations = source_by_canonical.get(canonical, [])
            keys = sorted({(o["source_id"], o["raw_column"]) for o in observations})
            coverage = [r for key in keys for r in coverage_by_key.get(key, [])]
            density_summary = {}
            for density_name in ("all_row_density", "eligible_row_density", "signal_density", "year_coverage_density"):
                values = [r[density_name] for r in coverage if isinstance(r.get(density_name), (int, float))]
                density_summary[f"min_{density_name}"] = min(values) if values else None
                density_summary[f"max_{density_name}"] = max(values) if values else None
            years = [r["year"] for r in coverage if isinstance(r["year"], int)]
            source_status = "sourced" if observations else "no_source_testimony"
            gate_status = "in_gate" if gate_matches else "not_in_gate"
            local_matches = [row for table in gate_matches for row in local_by_gate.get((table, column), [])]
            local_statuses = {row.get("status") for row in local_matches}
            local_gate_status = ("derived_inputs_sourced" if "derived_inputs_sourced" in local_statuses
                                 else "sourced" if "sourced" in local_statuses
                                 else next(iter(local_statuses), "not_in_local_crosswalk"))
            local_gap = ";".join(sorted({row.get("gap_type") for row in local_matches if row.get("gap_type")})) or None
            local_missing_years = ";".join(sorted({year for row in local_matches for year in (row.get("missing_years") or "").split(";") if year})) or None
            witness_refs = _extract_witness_refs(item.get("Witnesses", ""))
            resolved_refs, excluded_refs, ambiguous_refs, unregistered_refs = [], [], [], []
            for ref in witness_refs:
                if ref.startswith("derived:") or ref in registered_source_ids:
                    resolved_refs.append(ref)
                elif ref in excluded_source_ids or ref.startswith("newspaper_"):
                    excluded_refs.append(ref)
                else:
                    aliases = [sid for sid in registered_source_ids if sid.startswith(ref + " ")]
                    if len(aliases) == 1:
                        resolved_refs.append(aliases[0])
                    elif len(aliases) > 1:
                        ambiguous_refs.extend(aliases)
                    elif ref in witness_ref_prefix_aliases:
                        family_matches = [sid for sid in registered_source_ids if sid.startswith(witness_ref_prefix_aliases[ref])]
                        if family_matches:
                            ambiguous_refs.extend(family_matches)
                    else:
                        unregistered_refs.append(ref)
            fully_crossed = bool(gate_matches) and local_gate_status in {"sourced", "derived_inputs_sourced"}
            rows.append({
                "row_type": "workbook_column", "workbook_sheet": sheet,
                "gate_table": ";".join(gate_matches),
                "column": column, "canonical_column": canonical, "family": item.get("Family"),
                "workbook_status": item.get("Status"), "workbook_witnesses": item.get("Witnesses"),
                "workbook_witness_source_refs": ";".join(witness_refs),
                "resolved_workbook_witness_refs": ";".join(sorted(set(resolved_refs))),
                "excluded_workbook_witness_refs": ";".join(sorted(set(excluded_refs))),
                "ambiguous_workbook_witness_refs": ";".join(sorted(set(ambiguous_refs))),
                "unregistered_workbook_witness_refs": ";".join(unregistered_refs),
                "workbook_witness_era": item.get("Witness era"), "workbook_populated": item.get("Populated"),
                "workbook_gap": item.get("Gap"), "gate_status": gate_status,
                "gate_verdict": ";".join(gate_tables[t][column].get("verdict", "") for t in gate_matches),
                "source_status": source_status, "source_count": len({o["source_id"] for o in observations}),
                "source_ids": ";".join(sorted({o["source_id"] for o in observations})),
                "observed_year_min": min(years, default=None), "observed_year_max": max(years, default=None),
                "measured_coverage_rows": sum(r["all_row_density"] is not None for r in coverage),
                **density_summary,
                "local_gate_witness_status": local_gate_status, "local_gate_gap_type": local_gap,
                "local_missing_years": local_missing_years,
                "crosswalk_status": "fully_crossed" if fully_crossed else "workbook_only_or_unwitnessed",
            })

    for table, columns in gate_tables.items():
        workbook_sheet = {"weekly": "Sheet2", "player_nfl_season": "Sheet3", "player_nfl_career": "Sheet4"}.get(table)
        if not workbook_sheet:
            continue
        workbook_names = {r["Column"] for r in workbook[workbook_sheet]}
        for column, detail in columns.items():
            if column not in workbook_names:
                rows.append({
                    "row_type": "gate_only_column", "workbook_sheet": workbook_sheet, "gate_table": table,
                    "column": column, "canonical_column": canonical_alias(normalize_column_name(column)),
                    "family": detail.get("family"), "workbook_status": None, "workbook_witnesses": None,
                    "workbook_witness_source_refs": None, "unregistered_workbook_witness_refs": None,
                    "resolved_workbook_witness_refs": None, "excluded_workbook_witness_refs": None,
                    "ambiguous_workbook_witness_refs": None,
                    "workbook_witness_era": None, "workbook_populated": None,
                    "workbook_gap": "missing_from_supplied_workbook",
                    "gate_status": "in_gate", "gate_verdict": detail.get("verdict"),
                    "source_status": "not_checked",
                    "source_count": None, "source_ids": None, "observed_year_min": None,
                    "observed_year_max": None,
                    "measured_coverage_rows": None, "crosswalk_status": "gate_only_column",
                    "min_all_row_density": None, "max_all_row_density": None,
                    "min_eligible_row_density": None, "max_eligible_row_density": None,
                    "min_signal_density": None, "max_signal_density": None,
                    "min_year_coverage_density": None, "max_year_coverage_density": None,
                    "local_gate_witness_status": local_by_gate.get((table, column), [{}])[0].get("status", "not_in_local_crosswalk"),
                    "local_gate_gap_type": local_by_gate.get((table, column), [{}])[0].get("gap_type"),
                    "local_missing_years": local_by_gate.get((table, column), [{}])[0].get("missing_years"),
                })

    return {
        "generated": "2026-07-21", "external_workbook": str(xlsx_path),
        "sheet_map": sheet_map, "rows": rows,
        "summary": {
            "workbook_column_count": sum(len(v) for v in workbook.values()),
            "workbook_unique_canonical_count": len(workbook_columns),
            "fully_crossed_count": sum(r["crosswalk_status"] == "fully_crossed" for r in rows),
            "workbook_only_or_unwitnessed_count": sum(r["crosswalk_status"] == "workbook_only_or_unwitnessed" for r in rows),
            "workbook_rows_without_direct_lake_column": sum(r["source_status"] == "no_source_testimony" for r in rows),
            "workbook_rows_with_year_gap": sum(r.get("local_gate_gap_type") == "missing_years_within_witness_span" for r in rows),
            "workbook_rows_with_derivation_gap": sum(r.get("local_gate_gap_type") == "missing_derivation_inputs" for r in rows),
            "workbook_rows_with_unregistered_witness_refs": sum(bool(r.get("unregistered_workbook_witness_refs")) for r in rows if r["row_type"] == "workbook_column"),
            "unregistered_witness_reference_count": len({ref for r in rows for ref in (r.get("unregistered_workbook_witness_refs") or "").split(";") if ref}),
            "workbook_rows_with_excluded_witness_refs": sum(bool(r.get("excluded_workbook_witness_refs")) for r in rows if r["row_type"] == "workbook_column"),
            "excluded_witness_reference_count": len({ref for r in rows for ref in (r.get("excluded_workbook_witness_refs") or "").split(";") if ref}),
            "workbook_rows_with_ambiguous_witness_refs": sum(bool(r.get("ambiguous_workbook_witness_refs")) for r in rows if r["row_type"] == "workbook_column"),
            "ambiguous_witness_reference_count": len({ref for r in rows for ref in (r.get("ambiguous_workbook_witness_refs") or "").split(";") if ref}),
            "workbook_canonical_without_direct_lake_column": len({r["canonical_column"] for r in rows if r["source_status"] == "no_source_testimony"}),
            "gate_only_column_count": sum(r["row_type"] == "gate_only_column" for r in rows),
            "workbook_gap_annotation_count": sum(bool(r.get("workbook_gap")) for r in rows if r["row_type"] == "workbook_column"),
        },
    }


def write_outputs(result: dict, out_dir: Path = DEFAULT_OUT) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "external-witness-crosswalk.json").write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    fields = sorted({key for row in result["rows"] for key in row})
    with (out_dir / "external-witness-crosswalk.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader(); writer.writerows(result["rows"])
    external_gaps = []
    priorities = {"missing_derivation_inputs": "P0", "missing_years_within_witness_span": "P1",
                  "no_source_column": "P2", "not_in_local_crosswalk": "P3"}
    for row in result["rows"]:
        if row["row_type"] == "workbook_column" and row["crosswalk_status"] != "fully_crossed":
            gap = dict(row)
            gap["priority"] = priorities.get(row.get("local_gate_gap_type"), "P3")
            external_gaps.append(gap)
        elif row["row_type"] == "gate_only_column":
            gap = dict(row); gap["priority"] = "P2"; external_gaps.append(gap)
    gap_fields = sorted({key for row in external_gaps for key in row})
    with (out_dir / "external-witness-gap-ledger.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=gap_fields); writer.writeheader(); writer.writerows(external_gaps)
    unregistered = {}
    ambiguous = {}
    for row in result["rows"]:
        for ref in filter(None, (row.get("unregistered_workbook_witness_refs") or "").split(";")):
            item = unregistered.setdefault(ref, {"witness_ref": ref, "row_count": 0, "columns": set(), "sheets": set()})
            item["row_count"] += 1; item["columns"].add(row["column"]); item["sheets"].add(row["workbook_sheet"])
        for ref in filter(None, (row.get("ambiguous_workbook_witness_refs") or "").split(";")):
            item = ambiguous.setdefault(ref, {"witness_ref": ref, "row_count": 0, "columns": set(), "sheets": set()})
            item["row_count"] += 1; item["columns"].add(row["column"]); item["sheets"].add(row["workbook_sheet"])
    unregistered_rows = [{"witness_ref": item["witness_ref"], "row_count": item["row_count"],
                          "columns": ";".join(sorted(item["columns"])), "sheets": ";".join(sorted(item["sheets"]))}
                         for item in unregistered.values()]
    with (out_dir / "external-witness-unregistered-sources.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["witness_ref", "row_count", "columns", "sheets"])
        writer.writeheader(); writer.writerows(sorted(unregistered_rows, key=lambda row: (-row["row_count"], row["witness_ref"])))
    ambiguous_rows = [{"witness_ref": item["witness_ref"], "row_count": item["row_count"],
                       "columns": ";".join(sorted(item["columns"])), "sheets": ";".join(sorted(item["sheets"]))}
                      for item in ambiguous.values()]
    with (out_dir / "external-witness-ambiguous-sources.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["witness_ref", "row_count", "columns", "sheets"])
        writer.writeheader(); writer.writerows(sorted(ambiguous_rows, key=lambda row: (-row["row_count"], row["witness_ref"])))
    s = result["summary"]
    lines = ["# External witness workbook crosswalk", "",
             f"Source workbook: `{result['external_workbook']}`", "",
             f"- Workbook columns: {s['workbook_column_count']} ({s['workbook_unique_canonical_count']} canonical)",
             f"- Fully crossed to current gate and lake testimony: {s['fully_crossed_count']}",
             f"- Workbook-only/unwitnessed: {s['workbook_only_or_unwitnessed_count']}",
             f"- Workbook rows without a direct lake column: {s['workbook_rows_without_direct_lake_column']} ({s['workbook_canonical_without_direct_lake_column']} canonical)",
             f"- Workbook rows with year gaps: {s['workbook_rows_with_year_gap']}; derivation gaps: {s['workbook_rows_with_derivation_gap']}",
             f"- Rows citing unregistered witness sources: {s['workbook_rows_with_unregistered_witness_refs']} ({s['unregistered_witness_reference_count']} unique references)",
             f"- Rows citing intentionally excluded witnesses: {s['workbook_rows_with_excluded_witness_refs']} ({s['excluded_witness_reference_count']} references)",
             f"- Rows citing ambiguous source-family witnesses: {s['workbook_rows_with_ambiguous_witness_refs']} ({s['ambiguous_witness_reference_count']} registered source members)",
             f"- Current-gate columns missing from workbook: {s['gate_only_column_count']}",
             f"- Workbook rows with explicit gap annotations: {s['workbook_gap_annotation_count']}", "",
             "Sheet mapping: Sheet1 = shared identity/bio; Sheet2 = weekly; Sheet3 = player season; Sheet4 = player career.", "",
             "The row-level CSV includes workbook status, workbook testimony text, gate verdict, source IDs, observed year endpoints, measured coverage row counts, and min/max density by coverage basis."]
    (out_dir / "external-witness-crosswalk.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _extract_witness_refs(value: object) -> list[str]:
    text = str(value or "")
    refs = []
    for token in text.split(","):
        token = token.strip()
        match = re.match(r"([A-Za-z0-9_]+:[A-Za-z0-9_.+-]+)", token)
        if match:
            refs.append(match.group(1))
    return sorted(set(refs))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--xlsx", type=Path, default=DEFAULT_XLSX)
    parser.add_argument("--gate", type=Path, default=DEFAULT_GATE)
    parser.add_argument("--source-matrix", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--local-crosswalk", type=Path, default=DEFAULT_LOCAL_CROSSWALK)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    result = build_crosswalk(args.xlsx, args.gate, args.source_matrix, args.local_crosswalk)
    write_outputs(result, args.out_dir)
    print(json.dumps(result["summary"], indent=2))


if __name__ == "__main__":
    main()
