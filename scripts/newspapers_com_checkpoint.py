#!/usr/bin/env python
"""Build checkpoint artifacts for Newspapers.com acquisition runs."""

from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
from pathlib import Path

try:
    from pypdf import PdfReader, PdfWriter

    PYPDF_IMPORT_ERROR = ""
except ModuleNotFoundError as exc:
    PdfReader = None
    PdfWriter = None
    PYPDF_IMPORT_ERROR = str(exc)


DEFAULT_BASE_ROOT = Path(
    r"D:\league-history-data\nfl\raw\newspaper_archives\source_horizon_game_candidates\game_completeness_download_pilots"
)


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def artifact_pdf(artifacts: list[dict]) -> tuple[dict, dict, dict]:
    official = next(
        (a for a in artifacts if a.get("classification") == "newspapers_com_fullpage_pdf_browser_download"),
        {},
    )
    fallback = next(
        (a for a in artifacts if a.get("classification") == "newspapers_com_browser_rendered_pdf_fallback"),
        {},
    )
    if fallback.get("ok") and fallback.get("outputPath"):
        return fallback, official, fallback
    if official.get("ok") and official.get("outputPath"):
        return official, official, fallback
    return {}, official, fallback


def selected_rows(manifest_paths: list[Path]) -> tuple[list[dict], list[Path], list[dict]]:
    rows: list[dict] = []
    pdf_paths: list[Path] = []
    game_rows: list[dict] = []
    seen_games: set[str] = set()

    for manifest_path in manifest_paths:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        for game in manifest["games"]:
            boxscore_id = game["boxscoreId"]
            if boxscore_id in seen_games:
                raise RuntimeError(f"duplicate game in checkpoint manifests: {boxscore_id}")
            seen_games.add(boxscore_id)

            selected = game.get("selectedCandidates") or []
            captured = {str(c.get("imageId")): c for c in (game.get("captured") or [])}
            game_row = game["game"]
            for rank, candidate in enumerate(selected, start=1):
                capture = captured.get(str(candidate.get("imageId")), {})
                artifacts = capture.get("artifacts") or []
                result_root = capture.get("resultRoot") or ""
                skipped = bool(capture.get("skipped"))
                if skipped and result_root:
                    artifacts = read_jsonl(Path(result_root) / "fetch_manifest.jsonl")

                pdf_artifact, official_pdf, fallback_pdf = artifact_pdf(artifacts)
                official_ok = bool(official_pdf.get("ok"))
                fallback_ok = bool(fallback_pdf.get("ok"))
                pdf_path = pdf_artifact.get("outputPath") or ""
                if pdf_path:
                    pdf_paths.append(Path(pdf_path))

                if fallback_ok:
                    status = "browser_rendered_pdf_fallback"
                elif official_ok:
                    status = "postgame_full"
                elif capture:
                    status = "postgame_partial_pdf_failed"
                else:
                    status = "selected_not_captured"

                rows.append(
                    {
                        "boxscore_id": boxscore_id,
                        "candidate_rank": rank,
                        "game_date": game_row.get("game_date", ""),
                        "year": game_row.get("year", ""),
                        "away_team": game_row.get("away_team", ""),
                        "home_team": game_row.get("home_team", ""),
                        "status": status,
                        "candidate_count": game.get("candidateCount", 0),
                        "selected_count": len(selected),
                        "captured_count": len(game.get("captured") or []),
                        "image_id": candidate.get("imageId", ""),
                        "publication": candidate.get("publication", ""),
                        "result_date": candidate.get("resultDate", ""),
                        "page": candidate.get("page", ""),
                        "score": candidate.get("score", ""),
                        "skipped_existing": skipped,
                        "official_pdf_ok": official_ok,
                        "fallback_pdf_ok": fallback_ok,
                        "pdf_path": pdf_path,
                        "pdf_bytes": pdf_artifact.get(
                            "bytes",
                            Path(pdf_path).stat().st_size if pdf_path and Path(pdf_path).exists() else "",
                        ),
                        "capture_error": any(
                            (a.get("classification") or "").endswith("capture_error") for a in artifacts
                        ),
                        "fallback_pdf_failed": bool(capture and not (official_ok or fallback_ok)),
                        "result_root": result_root,
                        "run_manifest": str(manifest_path),
                    }
                )

    for boxscore_id in sorted({r["boxscore_id"] for r in rows}):
        group = [r for r in rows if r["boxscore_id"] == boxscore_id]
        first = group[0]
        game_rows.append(
            {
                "boxscore_id": boxscore_id,
                "game_date": first["game_date"],
                "away_team": first["away_team"],
                "home_team": first["home_team"],
                "candidate_count": first["candidate_count"],
                "selected_count": len(group),
                "pdf_count": sum(1 for r in group if r["pdf_path"]),
                "fallback_pdf_count": sum(1 for r in group if r["fallback_pdf_ok"]),
                "publications": " | ".join(r["publication"] for r in group),
                "pages": " | ".join(str(r["page"]) for r in group),
                "pdf_paths": " | ".join(r["pdf_path"] for r in group if r["pdf_path"]),
            }
        )

    return rows, sorted(set(pdf_paths), key=lambda p: str(p)), game_rows


def write_csv(path: Path, rows: list[dict], fallback_fields: list[str]) -> None:
    fields = list(rows[0].keys()) if rows else fallback_fields
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def check_pdfs(pdf_paths: list[Path]) -> tuple[list[dict], list[dict]]:
    strip_rows: list[dict] = []
    structural_rows: list[dict] = []
    if PYPDF_IMPORT_ERROR:
        for pdf in pdf_paths:
            size = pdf.stat().st_size if pdf.exists() else 0
            header = pdf.read_bytes()[:5] if pdf.exists() else b""
            basic_ok = header == b"%PDF-" and size > 100_000
            error = ""
            if not basic_ok:
                error = (
                    "basic_pdf_check_failed_after_missing_pypdf: "
                    f"header={header!r}; bytes={size}; import_error={PYPDF_IMPORT_ERROR}"
                )
            else:
                error = f"pypdf_unavailable_annotation_strip_skipped: {PYPDF_IMPORT_ERROR}"
            strip_rows.append(
                {
                    "pdf_path": str(pdf),
                    "annotations_before": 0,
                    "annotations_after": 0,
                    "stripped": False,
                    "error": error,
                }
            )
            structural_rows.append(
                {
                    "pdf_path": str(pdf),
                    "ok": basic_ok,
                    "pages": "",
                    "encrypted": "",
                    "bytes": size,
                    "annotations_after_strip": "",
                    "error": "" if basic_ok else error,
                }
            )
        return strip_rows, structural_rows

    for pdf in pdf_paths:
        before_annots = 0
        after_annots = 0
        stripped = False
        error = ""
        try:
            reader = PdfReader(str(pdf))
            for page in reader.pages:
                annots = page.get("/Annots")
                if annots:
                    try:
                        before_annots += len(annots)
                    except TypeError:
                        before_annots += 1
            if before_annots:
                writer = PdfWriter()
                for page in reader.pages:
                    if "/Annots" in page:
                        del page["/Annots"]
                    writer.add_page(page)
                tmp = pdf.with_suffix(pdf.suffix + ".tmp")
                with tmp.open("wb") as handle:
                    writer.write(handle)
                shutil.move(str(tmp), str(pdf))
                stripped = True

            reread = PdfReader(str(pdf))
            for page in reread.pages:
                annots = page.get("/Annots")
                if annots:
                    try:
                        after_annots += len(annots)
                    except TypeError:
                        after_annots += 1
            structural_rows.append(
                {
                    "pdf_path": str(pdf),
                    "ok": True,
                    "pages": len(reread.pages),
                    "encrypted": reread.is_encrypted,
                    "bytes": pdf.stat().st_size,
                    "annotations_after_strip": after_annots,
                    "error": "",
                }
            )
        except Exception as exc:  # noqa: BLE001 - checkpoint should record every PDF failure.
            error = str(exc)
            structural_rows.append(
                {
                    "pdf_path": str(pdf),
                    "ok": False,
                    "pages": "",
                    "encrypted": "",
                    "bytes": pdf.stat().st_size if pdf.exists() else "",
                    "annotations_after_strip": after_annots,
                    "error": error,
                }
            )
        strip_rows.append(
            {
                "pdf_path": str(pdf),
                "annotations_before": before_annots,
                "annotations_after": after_annots,
                "stripped": stripped,
                "error": error,
            }
        )
    return strip_rows, structural_rows


def hygiene_scan(out_dir: Path, base_root: Path, boxscore_ids: set[str]) -> dict:
    patterns = [
        re.compile(pattern, re.I)
        for pattern in [
            r"X-Amz-Signature",
            r"Key-Pair-Id",
            r"Policy=",
            r"Signature=",
            r"AWSAccessKeyId",
            r"Expires=",
        ]
    ]
    roots = [out_dir]
    roots.extend(base_root / boxscore_id for boxscore_id in sorted(boxscore_ids) if (base_root / boxscore_id).exists())
    files: list[Path] = []
    for root in roots:
        files.extend(
            file
            for file in root.rglob("*")
            if file.is_file() and file.suffix.lower() in {".json", ".jsonl", ".csv", ".txt", ".pdf"}
        )

    findings = []
    for file in files:
        try:
            sample = file.read_bytes()[:750_000].decode("utf-8", errors="ignore")
        except OSError:
            continue
        hits = sorted({pattern.pattern for pattern in patterns if pattern.search(sample)})
        if hits:
            findings.append({"path": str(file), "patterns": hits})
    return {"files_scanned": len(files), "findings": findings}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--out-dir", required=True, type=Path)
    parser.add_argument("--manifest", required=True, action="append", type=Path)
    parser.add_argument("--base-root", default=DEFAULT_BASE_ROOT, type=Path)
    args = parser.parse_args()

    args.out_dir.mkdir(parents=True, exist_ok=True)
    rows, pdf_paths, game_rows = selected_rows(args.manifest)

    write_csv(args.out_dir / f"{args.label}_candidate_checkpoint.csv", rows, ["boxscore_id"])
    write_csv(args.out_dir / f"{args.label}_game_checkpoint.csv", game_rows, ["boxscore_id"])

    statuses = sorted(
        set(r["status"] for r in rows)
        | {
            "browser_rendered_pdf_fallback",
            "browser_rendered_pdf_error_page",
            "capture_error",
            "no_postgame_candidate",
            "not_run_or_no_manifest",
            "postgame_full",
            "postgame_partial_pdf_failed",
            "selected_not_captured",
        }
    )
    for status in statuses:
        ids = [f"{r['boxscore_id']}#{r['candidate_rank']}:{r['image_id']}" for r in rows if r["status"] == status]
        (args.out_dir / f"{args.label}_{status}_candidate_ids.txt").write_text(
            "\n".join(ids) + ("\n" if ids else ""),
            encoding="utf-8",
        )

    strip_rows, structural_rows = check_pdfs(pdf_paths)
    write_csv(args.out_dir / f"{args.label}_pdf_annotation_strip.csv", strip_rows, ["pdf_path"])
    write_csv(args.out_dir / f"{args.label}_pdf_structural_check.csv", structural_rows, ["pdf_path"])

    hygiene = hygiene_scan(args.out_dir, args.base_root, {r["boxscore_id"] for r in rows})
    (args.out_dir / f"{args.label}_hygiene_scan_scoped.json").write_text(
        json.dumps(hygiene, indent=2),
        encoding="utf-8",
    )

    summary = {
        "label": args.label,
        "checkpoint_dir": str(args.out_dir),
        "source_run_manifests": [str(path) for path in args.manifest],
        "games": len(game_rows),
        "selected_candidates": len(rows),
        "status_counts": {status: sum(1 for r in rows if r["status"] == status) for status in statuses},
        "pdf_count": len(pdf_paths),
        "structural_ok": sum(1 for r in structural_rows if r["ok"]),
        "annotations_before": sum(int(r["annotations_before"]) for r in strip_rows),
        "annotations_after": sum(int(r["annotations_after"]) for r in strip_rows),
        "hygiene_files": hygiene["files_scanned"],
        "hygiene_findings": len(hygiene["findings"]),
    }
    (args.out_dir / f"{args.label}_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
