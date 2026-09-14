#!/usr/bin/env python
"""Prepare the next unread newspaper LLM packet for review.

This avoids ad hoc D-drive inspection commands. It reads the latest read-state
queue, selects the next unread packet, and writes a compact handoff folder with
paths, copied packet markdown, and the expected review-output JSON location.
"""

from __future__ import annotations

import argparse
import csv
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


DEFAULT_ROOT = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms")
DEFAULT_READ_STATE_ROOT = DEFAULT_ROOT / "llm_review_read_states"
DEFAULT_OUT_ROOT = DEFAULT_ROOT / "packet_review_preps"


def truncate(text: Any, max_chars: int) -> str:
    value = "" if text is None else str(text)
    value = value.replace("\r\n", "\n").replace("\r", "\n").strip()
    if len(value) <= max_chars:
        return value
    return value[:max_chars].rstrip() + f"\n[TRUNCATED at {max_chars} of {len(value)} chars]"


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def latest_dir(root: Path) -> Path:
    candidates = sorted(path for path in root.iterdir() if path.is_dir())
    if not candidates:
        raise FileNotFoundError(f"No run directories found under {root}")
    return candidates[-1]


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def select_packet(rows: list[dict[str, str]], packet_id: str) -> dict[str, str]:
    if packet_id:
        for row in rows:
            if row.get("packet_id") == packet_id:
                return row
        raise ValueError(f"Packet {packet_id} not found in unread packet queue")
    if not rows:
        raise ValueError("No unread packets found")
    return rows[0]


def load_packet_documents(packet_json_path: Path) -> list[dict[str, Any]]:
    if not packet_json_path.exists():
        return []
    payload = read_json(packet_json_path)
    documents = payload.get("documents", [])
    return documents if isinstance(documents, list) else []


def build_review_brief(packet_id: str, documents: list[dict[str, Any]], max_region_chars: int) -> str:
    lines = [
        f"# Review Brief For {packet_id}",
        "",
        "Use this brief for quick LLM review. The copied full packet markdown/JSON are in the same handoff folder.",
        "",
    ]
    for doc in documents:
        meta = doc.get("candidate_metadata", {}) or {}
        lines.extend([
            f"## {doc.get('source_document_id', '')}",
            "",
            f"- Action: `{doc.get('document_next_action', '')}`",
            f"- Reason: `{doc.get('document_reason_code', '')}`",
            f"- Boxscore: `{meta.get('boxscore_id', '')}`",
            f"- Game date: `{meta.get('game_date', '')}`",
            f"- Teams: `{meta.get('team_1', '')}` vs `{meta.get('team_2', '')}`",
            f"- Publication: `{meta.get('publication', '')}` page `{meta.get('page', '')}`",
            f"- PDF: `{meta.get('asset_pdf_path', '')}`",
            "",
            "### Existing Atoms",
            "",
        ])
        atoms = doc.get("existing_atom_claims", []) or []
        if not atoms:
            lines.append("_None._")
        for atom in atoms:
            lines.extend([
                f"- `{atom.get('semantic_target_table', '')}` / `{atom.get('atom_type', '')}` confidence `{atom.get('confidence_score', '')}`",
                f"  - entity: {atom.get('entity_text', '')}",
                f"  - raw: {atom.get('raw_value', '')}",
                f"  - evidence: {truncate(atom.get('evidence_text', ''), 700)}",
            ])
        lines.extend(["", "### Domain Rows", ""])
        domain_rows = doc.get("domain_rows", {}) or {}
        any_domain = False
        for name, rows in domain_rows.items():
            if rows:
                any_domain = True
                lines.append(f"- `{name}`: `{len(rows)}`")
        if not any_domain:
            lines.append("_None._")
        lines.extend(["", "### OCR Region Excerpts", ""])
        for region in doc.get("source_regions", []) or []:
            lines.extend([
                f"#### {region.get('region_id', '')}",
                "",
                f"- Label: `{region.get('region_label', '')}`",
                f"- Text chars: `{region.get('region_text_chars', '')}`",
                f"- Crop: `{region.get('crop_image_path', '')}`",
                "",
                "```text",
                truncate(region.get("text_excerpt", ""), max_region_chars),
                "```",
                "",
            ])
    return "\n".join(lines).rstrip() + "\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--read-state-dir", type=Path, default=None)
    parser.add_argument("--packet-id", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="packet_review_prep")
    parser.add_argument("--max-region-chars", type=int, default=2500)
    parser.add_argument("--preview-lines", type=int, default=120)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    read_state_dir = args.read_state_dir or latest_dir(DEFAULT_READ_STATE_ROOT)
    read_state_summary = read_json(read_state_dir / "summary.json")
    unread_queue = read_state_dir / "next_unread_packet_queue.csv"
    unread_rows = read_csv(unread_queue)
    packet_row = select_packet(unread_rows, args.packet_id)

    packet_id = packet_row["packet_id"]
    packet_md_path = Path(packet_row["packet_md_path"])
    packet_json_path = Path(packet_row["packet_json_path"])
    packet_jsonl_path = Path(packet_row["packet_jsonl_path"])
    packet_run_dir = Path(read_state_summary["packet_run_dir"])
    review_output_dir = Path(read_state_summary["review_output_dir"])
    review_output_path = review_output_dir / f"{packet_id}_review.json"

    out_dir = args.out_root / f"{stamp()}_{args.label}_{packet_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    copied_packet_md = out_dir / f"{packet_id}.md"
    copied_packet_json = out_dir / f"{packet_id}.json"
    shutil.copyfile(packet_md_path, copied_packet_md)
    if packet_json_path.exists():
        shutil.copyfile(packet_json_path, copied_packet_json)

    documents = load_packet_documents(packet_json_path)
    review_brief = out_dir / "packet_review_brief.md"
    review_brief.write_text(
        build_review_brief(packet_id, documents, args.max_region_chars),
        encoding="utf-8",
    )

    template_path = packet_run_dir / "empty_review_output_template.json"
    copied_template = out_dir / "empty_review_output_template.json"
    if template_path.exists():
        shutil.copyfile(template_path, copied_template)

    work_order = out_dir / "packet_review_work_order.md"
    work_order.write_text(
        "\n".join([
            "# Newspaper Packet Review Work Order",
            "",
            f"Created: `{iso_now()}`",
            f"Packet: `{packet_id}`",
            "",
            "## Read",
            "",
            f"- Prepared packet markdown: `{copied_packet_md}`",
            f"- Prepared compact brief: `{review_brief}`",
            f"- Original packet markdown: `{packet_md_path}`",
            f"- Original packet JSONL: `{packet_jsonl_path}`",
            "",
            "## Write",
            "",
            f"- Save reviewed JSON to: `{review_output_path}`",
            f"- Output template: `{copied_template}`",
            "",
            "## Review Rules",
            "",
            "- Return JSON only.",
            "- Extract only facts directly supported by packet OCR/region text.",
            "- Use `target_fields` for row-shaped records such as scores, scoring events, play-by-play, player box scores, and lineups.",
            "- Use `row_group_key` to identify the row being claimed.",
            "- Use `field_evidence` when different columns in a row have different evidence.",
            "- Mark weak OCR or identity uncertainty as `review`, not `promote`.",
            "- Add `needs_followup` for better OCR, larger crop, identity resolution, team mapping, or boxscore reconciliation.",
            "",
            "After saving JSON, run:",
            "",
            "```powershell",
            r"powershell.exe -NoProfile -ExecutionPolicy Bypass -File D:\league-history-data\nfl\tools\newspaper_atom_conveyor\run_newspaper_conveyor_refresh.ps1",
            "```",
            "",
        ]),
        encoding="utf-8",
    )

    manifest = {
        "created_at_utc": iso_now(),
        "packet_id": packet_id,
        "read_state_dir": str(read_state_dir),
        "packet_run_dir": str(packet_run_dir),
        "review_output_dir": str(review_output_dir),
        "review_output_path": str(review_output_path),
        "prepared_dir": str(out_dir),
        "prepared_packet_md": str(copied_packet_md),
        "prepared_packet_json": str(copied_packet_json) if copied_packet_json.exists() else "",
        "review_brief_path": str(review_brief),
        "work_order_path": str(work_order),
        "unread_packet_count": len(unread_rows),
        "packet_row": packet_row,
    }
    write_json(out_dir / "packet_review_manifest.json", manifest)
    print(json.dumps(manifest, indent=2, sort_keys=True))
    print("\n--- packet_review_brief_preview ---")
    brief_lines = review_brief.read_text(encoding="utf-8").splitlines()
    for line in brief_lines[: args.preview_lines]:
        print(line)
    if len(brief_lines) > args.preview_lines:
        print(f"[TRUNCATED preview at {args.preview_lines} of {len(brief_lines)} lines]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
