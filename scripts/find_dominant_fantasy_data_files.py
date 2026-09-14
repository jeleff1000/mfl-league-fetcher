"""Find dominant files among similar fantasy-football data artifacts.

This is a second-pass audit for broad archive folders that may contain primary
data despite living under old cleanup names. It compares data files from the
old archive against active/organized roots and chooses a dominant copy per
similar family using transparent heuristics.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET


DEFAULT_OBSOLETE_ROOT = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized\legacy_primary_review\fantasy_football_data_downloads_4.6GB"
)
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ACTIVE_ROOT = REPO_ROOT
DEFAULT_ORGANIZED_ROOT = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized")
DEFAULT_DOCS_ROOT = Path(r"C:\Users\joeye\OneDrive\Documents\Docs_DELETE")

DATA_EXTS = {".csv", ".parquet", ".json", ".jsonl", ".xlsx", ".xls", ".duckdb", ".db", ".sqlite"}
SKIP_DIR_NAMES = {
    ".git",
    "node_modules",
    ".next",
    ".venv",
    "__pycache__",
    ".pytest_cache",
    ".ruff_cache",
    ".vercel",
    ".vs",
    ".idea",
}

XML_NS = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def is_under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def iter_files(root: Path) -> list[Path]:
    if not root.exists():
        return []
    files = []
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        parts_lower = {part.lower() for part in path.parts}
        if parts_lower & SKIP_DIR_NAMES:
            continue
        if path.suffix.lower() in DATA_EXTS:
            files.append(path)
    return files


def md5_text(value: str) -> str:
    return hashlib.md5(value.encode("utf-8", errors="replace")).hexdigest()


def norm_name(value: str) -> str:
    value = value.lower()
    value = re.sub(r"\.(csv|parquet|jsonl?|xlsx?|duckdb|db|sqlite)$", "", value)
    value = re.sub(r"20\d{2}|19\d{2}", "YEAR", value)
    value = re.sub(r"wk_?\d{1,2}|week_?\d{1,2}", "WEEK", value)
    value = re.sub(r"\d{8}(?:_\d{6})?|\d{4}-\d{2}-\d{2}", "DATE", value)
    value = re.sub(r"[_\-. ]+", "_", value)
    value = value.strip("_")
    return value


def semantic_family_from_name(path: Path, columns: list[str]) -> str:
    name = norm_name(path.name)
    parent = norm_name(path.parent.name)
    p = str(path).lower()
    if "pbp_scoring" in name:
        return "nflverse_scoring_events_rollup"
    if "nflverse_pbp" in name or ("nflverse" in p and "pbp" in name):
        return "nflverse_pbp_by_season"
    if "player_fantasy" in name:
        return "league_player_fantasy"
    if "player_keeper" in name:
        return "player_keeper"
    if "yahoo_nfl_merged" in name:
        return "yahoo_nfl_merged"
    if "nfl_stats_merged" in name or "player_stats_multi_year" in name:
        return "merged_nfl_player_stats"
    if "player_stats" in name and ("nfl" in p or "player_data" in p):
        return "player_stats"
    if "player_season" in name:
        return "player_season"
    if "players_by_year" in name:
        return "players_by_year"
    if "matchup" in name:
        return "matchup"
    if "draft" in name:
        return "draft"
    if "transaction" in name:
        return "transactions"
    if "schedule" in name:
        return "schedule"
    if "league_settings" in name or "settings" in name:
        return "league_settings"
    if "sleeper_players" in name:
        return "sleeper_players_cache"
    if "super" in name and "table" in name:
        return "super_table"
    if columns:
        return f"{parent}_{name}_{md5_text('|'.join(sorted(c.lower() for c in columns)))[:8]}"
    return f"{parent}_{name}"


def copy_family_key(path: Path, columns: list[str], row_count: int | None) -> str:
    name = path.name.lower()
    parent = path.parent.name.lower()
    schema = md5_text("|".join(c.lower() for c in columns))[:10] if columns else "noschema"
    # Keep year/week in the copy key; remove only archive scaffolding.
    stable = f"{parent}/{name}/{schema}"
    if row_count is not None:
        stable += f"/rows={row_count}"
    return stable


def root_bucket(path: Path, args: argparse.Namespace) -> str:
    if is_under(path, args.active_root):
        if "tmp" in [part.lower() for part in path.parts]:
            return "active_repo_tmp"
        return "active_repo"
    if is_under(path, args.obsolete_root):
        return "legacy_primary_review"
    if is_under(path, args.organized_root / "legacy_local_exports"):
        return "legacy_local_exports"
    if is_under(path, args.organized_root / "legacy_origin_archives"):
        return "legacy_origin_archives"
    if is_under(path, args.organized_root / "stathead_generated"):
        return "stathead_generated"
    if is_under(path, args.organized_root / "temp_review"):
        return "temp_review"
    if is_under(path, args.docs_root):
        return "docs_source_reference"
    return "other"


def root_priority(bucket: str) -> int:
    return {
        "active_repo": 1000,
        "docs_source_reference": 930,
        "stathead_generated": 900,
        "active_repo_tmp": 760,
        "legacy_local_exports": 720,
        "legacy_origin_archives": 680,
        "legacy_primary_review": 650,
        "temp_review": 200,
        "other": 100,
    }.get(bucket, 0)


def profile_csv(path: Path) -> tuple[list[str], int | None, str]:
    try:
        with path.open("r", newline="", encoding="utf-8-sig", errors="replace") as handle:
            reader = csv.reader(handle)
            header = next(reader, [])
        row_count = 0
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                row_count += chunk.count(b"\n")
        row_count = max(0, row_count - 1)
        return [h.strip() for h in header], row_count, ""
    except Exception as exc:
        return [], None, repr(exc)


def profile_parquet(path: Path) -> tuple[list[str], int | None, str]:
    try:
        import pyarrow.parquet as pq

        pf = pq.ParquetFile(path)
        return list(pf.schema_arrow.names), pf.metadata.num_rows, ""
    except Exception as exc:
        return [], None, repr(exc)


def profile_json(path: Path) -> tuple[list[str], int | None, str]:
    try:
        if path.suffix.lower() == ".jsonl":
            rows = 0
            keys = set()
            with path.open("r", encoding="utf-8", errors="replace") as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    rows += 1
                    if len(keys) < 200:
                        try:
                            obj = json.loads(line)
                            if isinstance(obj, dict):
                                keys.update(obj)
                        except json.JSONDecodeError:
                            pass
            return sorted(keys), rows, ""
        if path.stat().st_size > 50 * 1024 * 1024:
            return [], None, "json_too_large_skipped"
        obj = json.loads(path.read_text(encoding="utf-8", errors="replace"))
        if isinstance(obj, list):
            keys = set()
            for item in obj[:500]:
                if isinstance(item, dict):
                    keys.update(item)
            return sorted(keys), len(obj), ""
        if isinstance(obj, dict):
            return sorted(obj.keys()), 1, ""
        return [], 1, ""
    except Exception as exc:
        return [], None, repr(exc)


def col_index(cell_ref: str) -> int:
    letters = "".join(ch for ch in cell_ref if ch.isalpha())
    idx = 0
    for ch in letters:
        idx = idx * 26 + ord(ch.upper()) - 64
    return idx - 1


def profile_xlsx(path: Path) -> tuple[list[str], int | None, str]:
    try:
        with zipfile.ZipFile(path) as zf:
            names = set(zf.namelist())
            if "xl/sharedStrings.xml" not in names or "xl/worksheets/sheet1.xml" not in names:
                return [], None, "unsupported_xlsx_layout"
            shared = []
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", XML_NS):
                shared.append("".join((t.text or "") for t in si.findall(".//a:t", XML_NS)))
            sheet = ET.fromstring(zf.read("xl/worksheets/sheet1.xml"))
            rows = sheet.findall(".//a:sheetData/a:row", XML_NS)
            if not rows:
                return [], 0, ""
            values: list[str] = []
            for cell in rows[0].findall("a:c", XML_NS):
                idx = col_index(cell.attrib.get("r", "A1"))
                while len(values) <= idx:
                    values.append("")
                node = cell.find("a:v", XML_NS)
                if node is None:
                    value = ""
                elif cell.attrib.get("t") == "s":
                    value = shared[int(node.text or "0")]
                else:
                    value = node.text or ""
                values[idx] = value
            return [v.strip() for v in values], max(0, len(rows) - 1), ""
    except Exception as exc:
        return [], None, repr(exc)


def profile_file(path: Path, args: argparse.Namespace) -> dict[str, Any]:
    ext = path.suffix.lower()
    if ext == ".csv":
        columns, rows, err = profile_csv(path)
    elif ext == ".parquet":
        columns, rows, err = profile_parquet(path)
    elif ext in {".json", ".jsonl"}:
        columns, rows, err = profile_json(path)
    elif ext in {".xlsx", ".xls"}:
        columns, rows, err = profile_xlsx(path)
    else:
        columns, rows, err = [], None, ""

    bucket = root_bucket(path, args)
    stat = path.stat()
    semantic_family = semantic_family_from_name(path, columns)
    copy_key = copy_family_key(path, columns, rows)
    schema_hash = md5_text("|".join(c.lower() for c in columns))[:12] if columns else ""
    name = path.name.lower()
    bonus = 0
    if any(term in name for term in ["multi_year", "allweeks", "all_weeks", "merged", "all"]):
        bonus += 70
    if ext == ".parquet":
        bonus += 40
    if path.name == "stathead_pbp_1978_1998_raw.parquet":
        bonus += 100
    score = (
        root_priority(bucket)
        + bonus
        + min((rows or 0) / 10000, 80)
        + min(len(columns), 80)
        + min(stat.st_size / (1024 * 1024 * 100), 40)
    )
    return {
        "FullName": str(path),
        "Name": path.name,
        "Extension": ext,
        "Bucket": bucket,
        "Bytes": stat.st_size,
        "MB": round(stat.st_size / (1024 * 1024), 3),
        "LastWriteTime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "ColumnsCount": len(columns),
        "Rows": "" if rows is None else rows,
        "SchemaHash": schema_hash,
        "ColumnsPreview": "|".join(columns[:40]),
        "ProfileError": err,
        "SemanticFamily": semantic_family,
        "CopyFamilyKey": copy_key,
        "DominanceScore": round(score, 3),
    }


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def choose_dominant(group: list[dict[str, Any]]) -> dict[str, Any]:
    return sorted(
        group,
        key=lambda row: (
            float(row["DominanceScore"]),
            int(row["Rows"] or 0),
            int(row["Bytes"] or 0),
            row["LastWriteTime"],
        ),
        reverse=True,
    )[0]


def build_group_rows(profiles: list[dict[str, Any]], key_field: str, group_type: str) -> list[dict[str, Any]]:
    groups = defaultdict(list)
    for row in profiles:
        groups[row[key_field]].append(row)

    output = []
    for key, group in sorted(groups.items(), key=lambda item: (-len(item[1]), item[0])):
        if len(group) < 2:
            continue
        dominant = choose_dominant(group)
        buckets = Counter(row["Bucket"] for row in group)
        has_obsolete = any(row["Bucket"] == "legacy_primary_review" for row in group)
        has_active = any(row["Bucket"] == "active_repo" for row in group)
        if group_type == "copy" and not has_obsolete:
            continue
        if group_type == "semantic" and not has_obsolete:
            continue
        for row in sorted(group, key=lambda r: (-float(r["DominanceScore"]), r["FullName"])):
            output.append(
                {
                    "GroupType": group_type,
                    "GroupKey": key,
                    "GroupSize": len(group),
                    "DominantFullName": dominant["FullName"],
                    "IsDominant": str(row["FullName"] == dominant["FullName"]),
                    "DominantBucket": dominant["Bucket"],
                    "HasActiveRepoCopy": str(has_active),
                    "HasObsoleteArchiveCopy": str(has_obsolete),
                    "GroupBuckets": json.dumps(dict(buckets), sort_keys=True),
                    **row,
                }
            )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--obsolete-root", type=Path, default=DEFAULT_OBSOLETE_ROOT)
    parser.add_argument("--active-root", type=Path, default=DEFAULT_ACTIVE_ROOT)
    parser.add_argument("--organized-root", type=Path, default=DEFAULT_ORGANIZED_ROOT)
    parser.add_argument("--docs-root", type=Path, default=DEFAULT_DOCS_ROOT)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=DEFAULT_ORGANIZED_ROOT / "_catalog" / "dominant_file_analysis_20260507",
    )
    args = parser.parse_args()

    roots = [
        args.obsolete_root,
        args.active_root / "fantasy_football_data",
        args.active_root / "fantasy_football_data_scripts",
        args.active_root / "ops_data",
        args.active_root / "tmp",
        args.organized_root / "legacy_local_exports",
        args.organized_root / "legacy_origin_archives",
        args.organized_root / "stathead_generated",
        args.organized_root / "temp_review",
        args.docs_root / "Pipeline_Data",
        args.docs_root / "Super_Table_Work",
    ]
    files = []
    seen_paths = set()
    for root in roots:
        for path in iter_files(root):
            key = str(path.resolve()).lower()
            if key not in seen_paths:
                files.append(path)
                seen_paths.add(key)

    profiles = []
    for i, path in enumerate(files, start=1):
        if i % 500 == 0:
            print(f"profiled {i}/{len(files)}", flush=True)
        profiles.append(profile_file(path, args))

    copy_groups = build_group_rows(profiles, "CopyFamilyKey", "copy")
    semantic_groups = build_group_rows(profiles, "SemanticFamily", "semantic")

    write_csv(args.out_dir / "data_file_profiles.csv", profiles)
    write_csv(args.out_dir / "dominant_copy_families.csv", copy_groups)
    write_csv(args.out_dir / "dominant_semantic_families.csv", semantic_groups)

    # High-signal rows: legacy-primary-review files where the dominant copy is
    # also in that review bundle, meaning the old folder may contain the
    # best/only copy.
    primary_candidates = []
    for rows in [copy_groups, semantic_groups]:
        for row in rows:
            if row["Bucket"] == "legacy_primary_review" and row["IsDominant"] == "True":
                primary_candidates.append(row)
    write_csv(args.out_dir / "legacy_primary_review_dominant_candidates.csv", primary_candidates)

    summary_rows = []
    for rows, name in [(copy_groups, "copy"), (semantic_groups, "semantic")]:
        group_keys = {row["GroupKey"] for row in rows}
        dominant_keys = {
            (row["GroupKey"], row["DominantFullName"], row["DominantBucket"])
            for row in rows
            if row["IsDominant"] == "True"
        }
        summary_rows.append(
            {
                "GroupType": name,
                "Groups": len(group_keys),
                "Rows": len(rows),
                "DominantInLegacyPrimaryReview": sum(
                    1 for _, _, bucket in dominant_keys if bucket == "legacy_primary_review"
                ),
                "DominantInActiveRepo": sum(1 for _, _, bucket in dominant_keys if bucket == "active_repo"),
                "DominantInLegacyLocalOrOrigin": sum(
                    1 for _, _, bucket in dominant_keys if bucket in {"legacy_local_exports", "legacy_origin_archives"}
                ),
            }
        )
    write_csv(args.out_dir / "dominant_analysis_summary.csv", summary_rows)

    md = [
        "# Dominant Fantasy Data File Analysis",
        "",
        "This analysis re-audits the old `recommend_to_delete` archive as a legacy primary-review bundle.",
        "",
        "## Outputs",
        "",
        "- `data_file_profiles.csv`: profiled data files with rows, columns, schema hash, semantic family, and dominance score.",
        "- `dominant_copy_families.csv`: same dataset/version-style groups, keeping year identifiers.",
        "- `dominant_semantic_families.csv`: broader dataset-kind families, often across years/formats.",
        "- `legacy_primary_review_dominant_candidates.csv`: cases where the archive copy is currently the dominant candidate.",
        "- `dominant_analysis_summary.csv`: rollup counts.",
        "",
        "Dominance is heuristic: active repo/source roots are favored, but row count, schema richness, size, format, and merged/all-year naming also matter.",
    ]
    (args.out_dir / "README.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    manifest = {
        "files_profiled": len(profiles),
        "copy_group_rows": len(copy_groups),
        "semantic_group_rows": len(semantic_groups),
        "legacy_primary_review_dominant_candidates": len(primary_candidates),
        "out_dir": str(args.out_dir),
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
