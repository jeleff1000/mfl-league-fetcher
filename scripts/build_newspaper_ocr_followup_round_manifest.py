#!/usr/bin/env python
"""Build an executable OCR manifest from the OCR follow-up prep lane.

The OCR prep station decides which documents need another OCR/visual pass. This
station converts that into the manifest format consumed by
`newspapers_ocr_round.py`, with new sidecar paths for follow-up OCR output.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_DB = DEFAULT_ROOT / "databases" / "newspaper_atoms.duckdb"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "ocr_followup_round_manifests"
DEFAULT_SIDECAR_ROOT = DEFAULT_ROOT / "ocr_followup_sidecars"
DEFAULT_OCR_TOOL = Path(r"D:\league-history-data\nfl\tools\newspaper_atom_conveyor\newspapers_ocr_round.py")

MANIFEST_FIELDS = [
    "candidate_id",
    "boxscore_id",
    "publication",
    "result_date",
    "page",
    "pdf_path",
    "ocr_sidecar_dir",
    "suggested_ocr_text_path",
    "suggested_ocr_json_path",
    "source_document_id",
    "ocr_followup_prep_run_id",
    "action_queue_run_id",
    "recommended_next_pass",
    "followup_types_json",
    "reason",
    "prior_region_text_paths_json",
    "prior_crop_image_paths_json",
]

RUN_FIELDS = [
    "ocr_followup_manifest_run_id",
    "ocr_followup_prep_run_id",
    "output_dir",
    "manifest_path",
    "document_count",
    "status",
    "created_at_utc",
    "summary_json_path",
]


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def stable_id(*parts: Any) -> str:
    raw = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def safe_path_part(value: Any, max_len: int = 80) -> str:
    text = re.sub(r"[^A-Za-z0-9_.-]+", "_", clean(value).strip())
    text = text.strip("._")
    if not text:
        text = "unknown"
    return text[:max_len]


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def query_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
    result = con.execute(sql, params or [])
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def latest_prep_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT ocr_followup_prep_run_id
        FROM newspaper_review.llm_ocr_followup_prep_run
        ORDER BY created_at_utc DESC, ocr_followup_prep_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def load_prep_documents(con: duckdb.DuckDBPyConnection, prep_run_id: str, limit: int) -> list[dict[str, Any]]:
    if not prep_run_id:
        return []
    sql = """
        SELECT *
        FROM newspaper_review.llm_ocr_followup_prep_document
        WHERE ocr_followup_prep_run_id = ?
        ORDER BY priority, recommended_next_pass, source_document_id
    """
    params: list[Any] = [prep_run_id]
    if limit > 0:
        sql += " LIMIT ?"
        params.append(limit)
    return query_dicts(con, sql, params)


def sidecar_paths(sidecar_root: Path, row: dict[str, Any], dpi: int, psm: int) -> tuple[Path, Path, Path]:
    boxscore_id = safe_path_part(row.get("boxscore_id"))
    source_document_id = safe_path_part(row.get("source_document_id"))
    next_pass = safe_path_part(row.get("recommended_next_pass"))
    key = stable_id(row.get("source_document_id"), row.get("recommended_next_pass"), dpi, psm)
    sidecar_dir = sidecar_root / boxscore_id / f"{source_document_id}_{next_pass}_{key}_{dpi}dpi_psm{psm}"
    return sidecar_dir, sidecar_dir / "ocr_text.txt", sidecar_dir / "ocr_layout.json"


def build_manifest_rows(
    rows: list[dict[str, Any]],
    sidecar_root: Path,
    dpi: int,
    psm: int,
) -> list[dict[str, Any]]:
    manifest_rows: list[dict[str, Any]] = []
    for row in rows:
        sidecar_dir, text_path, json_path = sidecar_paths(sidecar_root, row, dpi, psm)
        manifest_rows.append({
            "candidate_id": clean(row.get("source_document_id")),
            "boxscore_id": clean(row.get("boxscore_id")),
            "publication": clean(row.get("publication")),
            "result_date": clean(row.get("issue_date")),
            "page": clean(row.get("page")),
            "pdf_path": clean(row.get("asset_pdf_path")),
            "ocr_sidecar_dir": str(sidecar_dir),
            "suggested_ocr_text_path": str(text_path),
            "suggested_ocr_json_path": str(json_path),
            "source_document_id": clean(row.get("source_document_id")),
            "ocr_followup_prep_run_id": clean(row.get("ocr_followup_prep_run_id")),
            "action_queue_run_id": clean(row.get("action_queue_run_id")),
            "recommended_next_pass": clean(row.get("recommended_next_pass")),
            "followup_types_json": clean(row.get("followup_types_json")),
            "reason": clean(row.get("reasons_json")),
            "prior_region_text_paths_json": clean(row.get("region_text_paths_json")),
            "prior_crop_image_paths_json": clean(row.get("crop_image_paths_json")),
        })
    return manifest_rows


def create_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS newspaper_review")
    defs = ", ".join(f"{field} VARCHAR" for field in MANIFEST_FIELDS)
    con.execute(f"CREATE TABLE IF NOT EXISTS newspaper_review.llm_ocr_followup_round_manifest ({defs})")
    existing = {
        row[0] for row in con.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='newspaper_review'
              AND table_name='llm_ocr_followup_round_manifest'
            """
        ).fetchall()
    }
    for field in MANIFEST_FIELDS:
        if field not in existing:
            con.execute(f"ALTER TABLE newspaper_review.llm_ocr_followup_round_manifest ADD COLUMN IF NOT EXISTS {field} VARCHAR")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS newspaper_review.llm_ocr_followup_round_manifest_run (
          ocr_followup_manifest_run_id VARCHAR,
          ocr_followup_prep_run_id VARCHAR,
          output_dir VARCHAR,
          manifest_path VARCHAR,
          document_count INTEGER,
          status VARCHAR,
          created_at_utc VARCHAR,
          summary_json_path VARCHAR
        )
        """
    )


def insert_rows(con: duckdb.DuckDBPyConnection, table: str, rows: list[dict[str, Any]], fields: list[str]) -> None:
    if not rows:
        return
    placeholders = ",".join(["?"] * len(fields))
    con.executemany(
        f"INSERT INTO newspaper_review.{table} ({','.join(fields)}) VALUES ({placeholders})",
        [[clean(row.get(field)) for field in fields] for row in rows],
    )


def persist(
    db_path: Path,
    manifest_run_id: str,
    prep_run_id: str,
    out_dir: Path,
    manifest_path: Path,
    manifest_rows: list[dict[str, Any]],
    summary_path: Path,
) -> None:
    con = duckdb.connect(str(db_path))
    try:
        create_tables(con)
        con.execute(
            "DELETE FROM newspaper_review.llm_ocr_followup_round_manifest WHERE ocr_followup_prep_run_id = ?",
            [prep_run_id],
        )
        con.execute(
            "DELETE FROM newspaper_review.llm_ocr_followup_round_manifest_run WHERE ocr_followup_manifest_run_id = ?",
            [manifest_run_id],
        )
        insert_rows(con, "llm_ocr_followup_round_manifest", manifest_rows, MANIFEST_FIELDS)
        insert_rows(con, "llm_ocr_followup_round_manifest_run", [{
            "ocr_followup_manifest_run_id": manifest_run_id,
            "ocr_followup_prep_run_id": prep_run_id,
            "output_dir": str(out_dir),
            "manifest_path": str(manifest_path),
            "document_count": len(manifest_rows),
            "status": "complete",
            "created_at_utc": iso_now(),
            "summary_json_path": str(summary_path),
        }], RUN_FIELDS)
    finally:
        con.close()


def write_command_file(path: Path, ocr_tool: Path, manifest_path: Path, label: str, dpi: int, psm: int, timeout_seconds: int) -> None:
    lines = [
        "$ErrorActionPreference = \"Stop\"",
        f"py -3.10 \"{ocr_tool}\" --manifest \"{manifest_path}\" --label \"{label}\" --dpi {dpi} --psm {psm} --timeout-seconds {timeout_seconds} --overwrite",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB)
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--sidecar-root", type=Path, default=DEFAULT_SIDECAR_ROOT)
    parser.add_argument("--ocr-tool", type=Path, default=DEFAULT_OCR_TOOL)
    parser.add_argument("--ocr-followup-prep-run-id", default="")
    parser.add_argument("--label", default="newspaper_ocr_followup_round_manifest")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dpi", type=int, default=300)
    parser.add_argument("--psm", type=int, default=6)
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--no-db", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest_run_id = f"{stamp()}_{args.label}"
    created_at = iso_now()
    out_dir = args.out_root / manifest_run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(args.db_path), read_only=True)
    try:
        prep_run_id = args.ocr_followup_prep_run_id or latest_prep_run(con)
        prep_rows = load_prep_documents(con, prep_run_id, args.limit)
    finally:
        con.close()

    manifest_rows = build_manifest_rows(prep_rows, args.sidecar_root, args.dpi, args.psm)
    manifest_path = out_dir / "ocr_round_manifest.csv"
    command_path = out_dir / "run_ocr_followup_round.ps1"
    write_csv(manifest_path, manifest_rows, MANIFEST_FIELDS)
    write_command_file(
        command_path,
        args.ocr_tool,
        manifest_path,
        f"{args.label}_ocr",
        args.dpi,
        args.psm,
        args.timeout_seconds,
    )
    summary = {
        "created_at_utc": created_at,
        "ocr_followup_manifest_run_id": manifest_run_id,
        "ocr_followup_prep_run_id": prep_run_id,
        "output_dir": str(out_dir),
        "manifest_path": str(manifest_path),
        "run_command_path": str(command_path),
        "db_path": str(args.db_path),
        "document_count": len(manifest_rows),
        "recommended_next_pass_counts": dict(Counter(row["recommended_next_pass"] for row in manifest_rows)),
        "missing_pdf_count": sum(1 for row in manifest_rows if not Path(row["pdf_path"]).exists()),
        "persisted_to_duckdb": not args.no_db,
    }
    summary_path = out_dir / "summary.json"
    write_json(summary_path, summary)
    if not args.no_db:
        persist(args.db_path, manifest_run_id, prep_run_id, out_dir, manifest_path, manifest_rows, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
