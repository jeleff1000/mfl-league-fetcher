"""Classify the broad fantasy stats roundup into actionable cleanup buckets.

The source roundup was a machine-wide structured-data scan, so it contains a
mix of real football data, active project files, stale archives, and a lot of
false positives from application caches. This script writes audit manifests and
a conservative move plan; it does not delete anything.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


DEFAULT_ROUNDUP = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_stats_roundup_20260507_084417")
DEFAULT_OUT_NAME = "triage_outputs"
REPO_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT_NORM = str(REPO_ROOT).replace("/", "\\").lower()
REPO_TMP_NORM = REPO_ROOT_NORM + r"\tmp"

FOOTBALL_TERMS = {
    "fantasy",
    "football",
    "nfl",
    "nflverse",
    "sleeper",
    "yahoo",
    "espn",
    "stathead",
    "pfr",
    "pbp",
    "draft",
    "matchup",
    "league",
    "roster",
    "transaction",
    "keeper",
    "lamar",
    "player_fantasy",
}

NON_FOOTBALL_TERMS = {
    "daf",
    "torah",
    "movie",
    "movies",
    "recipe",
    "recipes",
    "gamingapp",
    "photos_",
    "visualstudio",
    "jetbrains",
    "pycharm",
    "chrome-cdp-research",
    "mcp-logs",
    "claude-cli-nodejs",
    "onedrive\\26.",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def norm(path: str) -> str:
    return path.replace("/", "\\").lower()


def has_any(path: str, terms: set[str]) -> bool:
    lowered = norm(path)
    return any(term in lowered for term in terms)


def path_bucket(full_name: str) -> str:
    p = norm(full_name)
    if p.startswith(REPO_TMP_NORM + r"\stathead_pbp_backfill_raw_1978_1998"):
        return "generated_stathead_backfill_raw"
    if p.startswith(REPO_TMP_NORM + r"\stathead"):
        return "generated_stathead_other"
    if p.startswith(REPO_ROOT_NORM):
        return "active_yahoo_oauth_repo"
    if p.startswith(r"c:\users\joeye\fantasy_football_data"):
        return "legacy_home_fantasy_football_data"
    if p.startswith(r"c:\users\joeye\onedrive\desktop\_cleanup\recommend_to_delete"):
        return "obsolete_recommend_to_delete_archive"
    if p.startswith(r"c:\users\joeye\onedrive\desktop\_cleanup\league_history_origins"):
        return "legacy_league_history_origins"
    if p.startswith(r"c:\users\joeye\onedrive\documents\docs_delete\pipeline_data"):
        return "source_pipeline_data_documents"
    if p.startswith(r"c:\users\joeye\onedrive\documents\docs_delete\super_table_work"):
        return "super_table_work_documents"
    if p.startswith(r"c:\temp") or p.startswith(r"c:\tmp"):
        return "loose_temp_data"
    if p.startswith(r"c:\users\joeye\appdata") or p.startswith(r"c:\users\joeye\.claude"):
        return "tool_or_app_cache"
    if p.startswith(r"c:\$windows.~bt") or p.startswith(r"c:\programdata"):
        return "system_or_windows_cache"
    if p.startswith(r"c:\users\joeye\onedrive\desktop"):
        return "desktop_other"
    if p.startswith(r"c:\users\joeye\onedrive\documents"):
        return "documents_other"
    return "other"


def classify(full_name: str, extension: str) -> tuple[str, str, str, str]:
    p = norm(full_name)
    bucket = path_bucket(full_name)
    ext = extension.lower()

    if bucket in {"system_or_windows_cache", "tool_or_app_cache"}:
        return bucket, "irrelevant", "ignore", "System/AppData/tool-cache structured file, not fantasy football data."
    if bucket == "desktop_other" and has_any(full_name, NON_FOOTBALL_TERMS):
        return bucket, "irrelevant", "ignore", "Desktop false positive from non-football project."
    if bucket == "documents_other" and has_any(full_name, NON_FOOTBALL_TERMS):
        return bucket, "irrelevant", "ignore", "Documents false positive from non-football data."
    if bucket == "active_yahoo_oauth_repo":
        if r"\tmp\chrome-cdp" in p:
            return bucket, "irrelevant", "delete_review", "Browser debug profile artifact under repo tmp."
        if r"\tmp\\" in p:
            return bucket, "relevant_generated", "organize_or_archive", "Generated repo tmp football artifact."
        return bucket, "relevant_active", "keep_in_place", "Active project or cache file; moving may break scripts."
    if bucket == "generated_stathead_backfill_raw":
        if ext == ".parquet" or full_name.endswith(
            ("README.md", "progress.csv", "season_summary.csv", "validation_summary.json", "combined_manifest.json")
        ):
            return bucket, "relevant_generated", "organize_keep", "Canonical Stathead PBP backfill output/audit file."
        return (
            bucket,
            "relevant_redundant_raw",
            "archive_raw_exploded",
            "Exploded per-query Stathead raw file; parquet is canonical.",
        )
    if bucket == "generated_stathead_other":
        return bucket, "relevant_generated", "organize_keep", "Generated Stathead historical scrape artifact."
    if bucket == "legacy_home_fantasy_football_data":
        return (
            bucket,
            "relevant_legacy",
            "centralize_archive",
            "Legacy local league export outside the active repo/Fly source of truth.",
        )
    if bucket == "obsolete_recommend_to_delete_archive":
        return (
            bucket,
            "obsolete",
            "archive_or_delete_review",
            "Already in recommend_to_delete; likely superseded by active repo/Fly/nflverse caches.",
        )
    if bucket == "legacy_league_history_origins":
        return (
            bucket,
            "relevant_legacy",
            "centralize_archive",
            "Historical league-origin material; keep as archive, not active data.",
        )
    if bucket in {"source_pipeline_data_documents", "super_table_work_documents"}:
        return (
            bucket,
            "relevant_source_or_reference",
            "catalog_keep_in_place",
            "Source/reference workbook or super-table work area.",
        )
    if bucket == "loose_temp_data":
        if has_any(full_name, FOOTBALL_TERMS) or ext in {".duckdb", ".parquet"}:
            return (
                bucket,
                "stale_or_temp_review",
                "centralize_temp_review",
                "Loose temp database/parquet; likely stale and not a live source of truth.",
            )
        return bucket, "irrelevant", "ignore", "Loose temp false positive."
    if has_any(full_name, NON_FOOTBALL_TERMS):
        return bucket, "irrelevant", "ignore", "Non-football project false positive."
    if has_any(full_name, FOOTBALL_TERMS):
        return bucket, "relevant_review", "manual_review", "Football-ish file outside known project roots."
    return bucket, "irrelevant", "ignore", "No fantasy/football signal beyond broad scanner match."


def duplicate_keep_score(path: str) -> tuple[int, int, int, str]:
    p = norm(path)
    if "stathead_pbp_backfill_raw_1978_1998" in p and "raw_sample" not in p:
        return (0, 0, 0, p)
    if p.startswith(REPO_ROOT_NORM) and r"\tmp\\" not in p:
        return (1, 0, 0, p)
    if p.startswith(REPO_ROOT_NORM):
        return (2, 0, 0, p)
    if "recommend_to_delete" in p:
        return (8, 0, 0, p)
    if p.startswith(r"c:\users\joeye\appdata") or p.startswith(r"c:\users\joeye\.claude"):
        return (9, 0, 0, p)
    return (5, 0, 0, p)


def build_duplicate_decisions(
    rows: list[dict[str, str]], classification_by_path: dict[str, dict[str, str]]
) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in rows:
        groups[row["GroupId"]].append(row)

    decisions = []
    for group_id, group in sorted(groups.items()):
        keep = sorted(group, key=lambda row: duplicate_keep_score(row["FullName"]))[0]
        bytes_each = int(float(keep.get("Bytes") or 0))
        relevance_counts = Counter(
            classification_by_path.get(row["FullName"], {}).get("relevance", "unknown") for row in group
        )
        duplicate_waste_bytes = max(0, len(group) - 1) * bytes_each
        for row in group:
            role = "keep_preferred_copy" if row["FullName"] == keep["FullName"] else "duplicate_copy"
            cls = classification_by_path.get(row["FullName"], {})
            decisions.append(
                {
                    "GroupId": group_id,
                    "SHA256": row.get("SHA256", ""),
                    "Count": row["Count"],
                    "Bytes": row["Bytes"],
                    "MB": row["MB"],
                    "Name": row["Name"],
                    "FullName": row["FullName"],
                    "DuplicateRole": role,
                    "PreferredCopy": keep["FullName"],
                    "DuplicateWasteBytesIfNonPreferredRemoved": duplicate_waste_bytes
                    if role == "duplicate_copy"
                    else 0,
                    "Bucket": cls.get("bucket", ""),
                    "Relevance": cls.get("relevance", ""),
                    "RecommendedAction": cls.get("recommended_action", ""),
                    "GroupRelevanceCounts": json.dumps(dict(relevance_counts), sort_keys=True),
                }
            )
    return decisions


def target_category(row: dict[str, str]) -> str:
    action = row["RecommendedAction"]
    bucket = row["Bucket"]
    if action == "organize_keep" and "stathead" in bucket:
        return "stathead_generated"
    if action == "archive_raw_exploded":
        return "stathead_generated/raw_exploded_audit"
    if action == "centralize_archive" and bucket == "legacy_home_fantasy_football_data":
        return "legacy_local_exports"
    if action == "centralize_archive":
        return "legacy_origin_archives"
    if action == "centralize_temp_review":
        return "temp_review"
    if action == "archive_or_delete_review":
        return "obsolete_delete_review"
    if action == "catalog_keep_in_place":
        return "source_references"
    if action == "keep_in_place":
        return "active_project_keep_in_place"
    if action == "ignore":
        return "irrelevant_ignore"
    return "manual_review"


def build_move_plan(classified: list[dict[str, str]], organized_root: Path) -> list[dict[str, str]]:
    """Conservative physical moves for directories/files that are safe to centralize."""
    candidates = [
        (
            Path(r"C:\Users\joeye\OneDrive\Desktop\_cleanup\recommend_to_delete\fantasy_football_data_downloads_4.6GB"),
            organized_root / "obsolete_delete_review" / "fantasy_football_data_downloads_4.6GB",
            "move_directory",
            "Old data-download archive already marked recommend_to_delete; centralize for final review.",
        ),
        (
            Path(r"C:\Users\joeye\OneDrive\Desktop\_cleanup\league_history_origins"),
            organized_root / "legacy_origin_archives" / "league_history_origins",
            "move_directory",
            "Historical league-origin material; archive centrally rather than leaving on Desktop cleanup island.",
        ),
        (
            REPO_ROOT / "tmp" / "stathead_pbp_backfill_raw_1978_1998",
            organized_root / "stathead_generated" / "pbp_backfill_1978_1998",
            "move_directory",
            "Completed generated Stathead PBP backfill raw+parquet package.",
        ),
        (
            REPO_ROOT / "tmp" / "stathead_pbp_backfill_plan",
            organized_root / "stathead_generated" / "pbp_backfill_1978_1998_plan",
            "move_directory",
            "Generated URL plan and schema bridge for Stathead PBP backfill.",
        ),
        (
            REPO_ROOT / "tmp" / "stathead_pbp_backfill_raw_sample",
            organized_root / "stathead_generated" / "pbp_backfill_1978_1998_sample",
            "move_directory",
            "Pilot pull for Bears/Cardinals; redundant but useful audit sample.",
        ),
        (
            REPO_ROOT / "tmp" / "stathead_pbp_backfill_logs",
            organized_root / "stathead_generated" / "pbp_backfill_1978_1998_logs",
            "move_directory",
            "Downloader stdout/stderr logs.",
        ),
        (
            REPO_ROOT / "tmp" / "stathead_gap_files",
            organized_root / "stathead_generated" / "player_game_gap_files",
            "move_directory",
            "Historical gap scrape CSVs.",
        ),
        (
            REPO_ROOT / "tmp" / "stathead_punting_raw",
            organized_root / "stathead_generated" / "punting_pre1999_raw_pages",
            "move_directory",
            "Raw pages from pre-1999 punting scrape.",
        ),
        (
            REPO_ROOT / "tmp" / "stathead_punting_all_through_1998.csv",
            organized_root / "stathead_generated" / "punting_pre1999" / "stathead_punting_all_through_1998.csv",
            "move_file",
            "Combined pre-1999 punting scrape.",
        ),
        (
            Path(r"C:\Users\joeye\fantasy_football_data"),
            organized_root / "legacy_local_exports" / "home_fantasy_football_data",
            "move_directory",
            "Legacy local league exports outside the active repo/Fly source of truth.",
        ),
        (
            Path(r"C:\Temp\___ops.duckdb"),
            organized_root / "temp_review" / "___ops.duckdb",
            "move_file",
            "Loose temp DuckDB; Fly is the current source of truth.",
        ),
        (
            Path(r"C:\tmp\pbp2024.parquet"),
            organized_root / "temp_review" / "pbp2024.parquet",
            "move_file",
            "Loose temp nflverse parquet duplicate/review file.",
        ),
        (
            Path(r"C:\tmp\playoff_test"),
            organized_root / "temp_review" / "playoff_test",
            "move_directory",
            "Loose playoff test database folder.",
        ),
    ]
    rows = []
    for source, target, action, reason in candidates:
        rows.append(
            {
                "Action": action,
                "Source": str(source),
                "Target": str(target),
                "SourceExists": str(source.exists()),
                "TargetExists": str(target.exists()),
                "Reason": reason,
            }
        )
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--roundup", type=Path, default=DEFAULT_ROUNDUP)
    parser.add_argument(
        "--organized-root",
        type=Path,
        default=Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"),
    )
    args = parser.parse_args()

    out_dir = args.roundup / DEFAULT_OUT_NAME
    out_dir.mkdir(parents=True, exist_ok=True)

    all_rows = read_csv(args.roundup / "all_structured_data_files.csv")
    likely_rows = read_csv(args.roundup / "likely_fantasy_stats_files.csv")
    dup_rows = read_csv(args.roundup / "duplicate_candidates_sha256_confirmed.csv")
    likely_set = {row["FullName"] for row in likely_rows}

    classified = []
    by_path: dict[str, dict[str, str]] = {}
    for row in all_rows:
        bucket, relevance, action, reason = classify(row["FullName"], row["Extension"])
        classified_row = {
            **row,
            "Bucket": bucket,
            "Relevance": relevance,
            "RecommendedAction": action,
            "TargetCategory": target_category(
                {
                    "Bucket": bucket,
                    "Relevance": relevance,
                    "RecommendedAction": action,
                }
            ),
            "Reason": reason,
            "InLikelyFantasyStatsManifest": str(row["FullName"] in likely_set),
        }
        classified.append(classified_row)
        by_path[row["FullName"]] = {
            "bucket": bucket,
            "relevance": relevance,
            "recommended_action": action,
        }

    dup_decisions = build_duplicate_decisions(dup_rows, by_path)
    move_plan = build_move_plan(classified, args.organized_root)

    write_csv(out_dir / "classified_all_structured_data_files.csv", classified)
    write_csv(out_dir / "exact_duplicate_decisions.csv", dup_decisions)
    write_csv(out_dir / "safe_physical_move_plan.csv", move_plan)

    bucket_summary = []
    grouped: dict[tuple[str, str, str], dict[str, Any]] = defaultdict(lambda: {"files": 0, "mb": 0.0})
    for row in classified:
        key = (row["Bucket"], row["Relevance"], row["RecommendedAction"])
        grouped[key]["files"] += 1
        grouped[key]["mb"] += float(row.get("MB") or 0)
    for (bucket, relevance, action), values in sorted(grouped.items(), key=lambda kv: kv[1]["mb"], reverse=True):
        bucket_summary.append(
            {
                "Bucket": bucket,
                "Relevance": relevance,
                "RecommendedAction": action,
                "Files": values["files"],
                "MB": round(values["mb"], 3),
            }
        )
    write_csv(out_dir / "bucket_summary.csv", bucket_summary)

    stale_or_irrelevant = [
        row
        for row in classified
        if row["Relevance"] in {"irrelevant", "obsolete", "stale_or_temp_review", "relevant_redundant_raw"}
    ]
    write_csv(out_dir / "irrelevant_obsolete_or_redundant_files.csv", stale_or_irrelevant)

    relevant_keep = [
        row
        for row in classified
        if row["Relevance"]
        in {
            "relevant_active",
            "relevant_generated",
            "relevant_legacy",
            "relevant_source_or_reference",
            "relevant_review",
        }
    ]
    write_csv(out_dir / "relevant_keep_or_review_files.csv", relevant_keep)

    summary = {
        "roundup": str(args.roundup),
        "organized_root": str(args.organized_root),
        "all_structured_files": len(all_rows),
        "likely_manifest_rows": len(likely_rows),
        "exact_duplicate_rows": len(dup_rows),
        "exact_duplicate_groups": len({row["GroupId"] for row in dup_rows}),
        "classified_outputs": str(out_dir),
        "bucket_summary": bucket_summary,
        "safe_move_plan_rows": len(move_plan),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    md = [
        "# Fantasy Stats Roundup Triage",
        "",
        f"Roundup folder: `{args.roundup}`",
        f"Organized root: `{args.organized_root}`",
        "",
        "## Main Outputs",
        "",
        "- `classified_all_structured_data_files.csv`: every scanned structured file with relevance/action.",
        "- `exact_duplicate_decisions.csv`: exact SHA256 duplicate rows with a preferred copy.",
        "- `irrelevant_obsolete_or_redundant_files.csv`: false positives, stale temp data, obsolete archives, and redundant exploded raw.",
        "- `relevant_keep_or_review_files.csv`: active/generated/legacy/source files worth keeping or reviewing.",
        "- `safe_physical_move_plan.csv`: conservative filesystem moves only; no deletion.",
        "- `bucket_summary.csv`: rollup by bucket/relevance/action.",
        "",
        "## Bucket Summary",
        "",
        "| bucket | relevance | action | files | MB |",
        "|---|---|---|---:|---:|",
    ]
    for row in bucket_summary:
        md.append(
            f"| `{row['Bucket']}` | `{row['Relevance']}` | `{row['RecommendedAction']}` | {row['Files']} | {row['MB']} |"
        )
    (out_dir / "README.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
