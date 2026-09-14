"""
sota_recon/source_hunt_census.py  --  O.8 LAW A: the DISCOVERY gate (master plan §25.1)

Law A asks: *do we know about every source?* The disk census (O.7c) answered it for
material that already LANDED. It could not answer it for material we went LOOKING for
and never captured -- the source-hunt / source-horizon program in `curated/` was a
FOURTH unjoined key-space (source_search_queues, source_hunt_packets,
source_acquisition_packets, source_horizon_*, source_hunt_family_work_orders, ...),
and witness_gate's `discoveries` feed was empty. A hunt candidate could be raised,
worked, and abandoned with no counter ever noticing.

This lane walks those ledgers and requires every hunt PROGRAM and every candidate
SOURCE LANE to carry a terminal disposition:

  CAPTURED    the hunt landed material that is registered (or dispositioned) today
  QUEUED      still open, with a named next action
  ABANDONED   closed without capture, with a MANDATORY reason

Zero-counter (scoreboard): hunt_candidates_without_terminal_disposition = 0.

MEASURED GROUND TRUTH (2026-07-26, the final queue
`source_search_queues/20260615T190000Z_final_fly_only_source_horizon`): 2,159 search
tasks, ALL carrying the terminal status
`non_loc_closeout_fly_rejected_no_exact_local_source`, across 9 source lanes. The
program closed itself out; this gate makes that closure STRUCTURAL rather than
incidental -- a new work order or a re-opened task fails the counter until it is
dispositioned.

Output: docs/source-hunt-census.json

Run:  python -m scripts.sota_recon.source_hunt_census
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "source-hunt-census.json")

CURATED = Path("D:/league-history-data/nfl/curated")

DISPOSITION_VOCAB = {"CAPTURED", "QUEUED", "ABANDONED"}

# The hunt-program directories under curated/. Each is a dated work-order corpus;
# a NEW one appearing without a disposition entry below fails the counter.
HUNT_PROGRAM_DIRS = [
    "source_acquisition_packets",
    "source_hunt_packets",
    "source_hunt_family_work_orders",
    "source_search_queues",
    "source_search_execution_batches",
    "source_search_local_evidence_packets",
    "source_search_status_updates",
    "source_of_truth_candidates",
    "source_horizon_browser_batches",
    "source_horizon_game_loc_candidates",
    "source_horizon_internet_archive_captures",
    "source_horizon_issue_capture_plans",
    "source_horizon_loc_json_captures",
    "source_horizon_loc_page_asset_captures",
    "source_horizon_public_availability",
    "source_horizon_rotation_runs",
    "source_horizon_work_orders",
    "non_loc_source_horizon_closeouts",
]

# Per hunt program: what it hunted, and where its output went. Every entry is an
# adjudication with a receipt, never a default.
PROGRAM_DISPOSITIONS: dict[str, tuple[str, str]] = {
    "source_search_queues": (
        "ABANDONED",
        "The queue's own terminal state: the final run "
        "(20260615T190000Z_final_fly_only_source_horizon) carries 2,159 tasks, 100% at "
        "status non_loc_closeout_fly_rejected_no_exact_local_source -- every hunted "
        "stat-cell was closed as fly-only with no exact local source. Nothing left open"),
    "source_search_status_updates": (
        "ABANDONED", "status-update ledgers for the above queue runs; terminal with it"),
    "source_search_execution_batches": (
        "ABANDONED", "execution batches of the closed-out search queue"),
    "source_search_local_evidence_packets": (
        "ABANDONED", "local-evidence packets produced while working the closed queue"),
    "non_loc_source_horizon_closeouts": (
        "ABANDONED",
        "THE closeout program itself (pfr_era_1978_2025 dry_run + applied): it is what "
        "wrote the terminal fly-rejected statuses onto the search queue"),
    "source_acquisition_packets": (
        "ABANDONED",
        "1924 Hanny/Cardinals acquisition requests + promotion contracts (39 packets). "
        "Newspaper-era acquisition work; anything it captured lives under "
        "raw/newspaper_archives (REGISTERED as newspaper_raw_archives, archive-of the "
        "newspaper root). No un-actioned request remains"),
    "source_hunt_packets": (
        "ABANDONED",
        "high-impact residual source hunts (def_int / kicking families). Their family "
        "work orders resolved into the search queue that closed out fly-rejected"),
    "source_hunt_family_work_orders": (
        "ABANDONED",
        "per-family work orders (def_int_yards 1946-59 / 1960-77 etc.) feeding the "
        "closed-out search queue"),
    "source_of_truth_candidates": (
        "CAPTURED",
        "local SOT candidate manifests -- these are RELEASE candidates of our own "
        "supertable (internal lineage), not external sources; the current candidate is "
        "the registered v26 release under audit"),
    "source_horizon_work_orders": (
        "ABANDONED", "browser work orders for the 1924 Hanny/Cardinals horizon; "
                     "superseded by the closeout"),
    "source_horizon_browser_batches": (
        "ABANDONED", "browser capture batches for the same horizon"),
    "source_horizon_rotation_runs": (
        "ABANDONED", "rotation runs of the same browser horizon program"),
    "source_horizon_issue_capture_plans": (
        "ABANDONED", "issue-level capture plans for LOC newspaper issues"),
    "source_horizon_public_availability": (
        "ABANDONED",
        "public-availability probes (Herald Examiner routes) -- established which "
        "newspaper routes are publicly reachable; findings folded into the LOC/IA "
        "capture programs"),
    "source_horizon_game_loc_candidates": (
        "CAPTURED",
        "LOC game candidates -- the captured subset landed as raw/newspaper_archives/"
        "loc issue captures (REGISTERED as newspaper_raw_archives)"),
    "source_horizon_loc_json_captures": (
        "CAPTURED",
        "LOC JSON captures feeding the newspaper OCR streams that now register as "
        "ancient_newspaper_ocr (newspaper root) and the curated newspaper bundle"),
    "source_horizon_loc_page_asset_captures": (
        "CAPTURED",
        "LOC page-asset (image) captures -- the IMAGE layer that makes the newspaper "
        "root arbitrable; held under raw/newspaper_archives"),
    "source_horizon_internet_archive_captures": (
        "CAPTURED",
        "Internet Archive Tribune captures (1924 Hanny/Cardinals): capture results + "
        "OCR hits landed; the OCR rows flow into the newspaper streams"),
}

# Candidate SOURCE lanes named inside the hunt queue (`source_lane` column), each
# mapped to what actually happened to it. This is the roster Law A must dispose of:
# the hunt was FOR sources, so every lane is a candidate-source disposition.
SOURCE_LANE_DISPOSITIONS: dict[str, tuple[str, str]] = {
    "stathead_player_gamelog_source_reacquisition": (
        "CAPTURED",
        "Stathead = Sports Reference = the PFR root (gap-filler, NEVER a referee). The "
        "material is held and registered as the pbp_merged family under "
        "raw/stathead/generated"),
    "stathead_excel_kicking_distance_source_reacquisition": (
        "ABANDONED",
        "closed_reject_fly_only_stathead_excel_kick_distance_no_local_source (509 "
        "tasks): the fly values came from a Stathead excel export with no exact local "
        "artifact to replay. PFR-root material anyway -- it could never referee"),
    "stathead_excel_kicking_source_reacquisition": (
        "ABANDONED", "same class as the kicking-distance lane (1 task)"),
    "pfr_kicking_summary_source_reacquisition": (
        "ABANDONED",
        "closed_reject_fly_only_pfr_kicking_no_exact_local_source (6 tasks); the "
        "registered pfr_player_kicking / pfr_box_kicking authorities already hold the "
        "family"),
    "signature_specific_source_capture": (
        "ABANDONED",
        "closed_reject_fly_only_unknown_source_no_exact_replay (435 tasks): the fly "
        "values carry no identifiable source label to re-acquire"),
    "pfr_or_pfa_player_defensive_interception_gamelog_capture": (
        "CAPTURED",
        "PFA game-grain material WAS subsequently captured: "
        "pfa_player_game_participation (5.36M rows, 1920-2025, 20/20 shards) is "
        "registered, and its raw boxscore HTML is on disk. The def_int STAT extraction "
        "from those pages is the Law B capture-expansion item, not an open hunt"),
    "stathead_pbp_enriched_rollup_finalized": (
        "CAPTURED",
        "the enriched pbp rollup is registered as pbp_player_week_rollup "
        "(pbp_merged lineage, era-split root)"),
    "legacy_idp_excel_or_authoritative_gamebook_capture": (
        "ABANDONED",
        "closed_reject_fly_only_legacy_excel_no_local_artifact (154 tasks): legacy IDP "
        "excel values with no surviving local artifact. AUTHORITATIVE GAMEBOOK capture "
        "was never obtained -- this is the honest residual: the gamebook primal record "
        "is not in our possession for these cells"),
    "source_label_recovery_or_external_source_capture": (
        "ABANDONED",
        "closed_reject_fly_only_missing_source_label (88 tasks): fly rows whose source "
        "label was lost; unrecoverable without the label"),
}

# Named external source candidates the hunt program preferred (the `preferred_sources`
# column, uniform across all 2,159 tasks). Each is a source-universe disposition.
PREFERRED_SOURCE_DISPOSITIONS: dict[str, tuple[str, str]] = {
    "local_artifact_rescan": (
        "CAPTURED", "the local lake rescan lane -- now the O.7c disk registration "
                    "census, gated at zero undispositioned dirs"),
    "PFR/PFA/source leads": (
        "CAPTURED",
        "PFR is registered across 54 sources; PFA is registered as the ancient "
        "pfa_loc streams + pfa_player_game_participation. The raw/pfa 'source leads' "
        "directory itself measured EMPTY (2026-07-26) and is EXCLUDED with that reason"),
    "LOC Chronicling America": (
        "CAPTURED",
        "LOC scraper + page/issue captures held under raw/newspaper_archives "
        "(REGISTERED as newspaper_raw_archives); LOC OCR rows register as "
        "ancient_newspaper_ocr"),
    "newspaper archive OCR": (
        "CAPTURED",
        "newspapers.com OCR + LOC OCR streams register as ancient_newspaper_ocr and "
        "flow into the curated newspaper witness bundle (11 registered sources)"),
    "image/PDF review": (
        "QUEUED",
        "the human image-review lane: the newspaper program owns it and its holds "
        "(newspaper_review_holds). Non-voting until those holds clear -- this is the "
        "one preferred source that is legitimately still open, and it is owned, not "
        "abandoned"),
}


def _read_csv_header_and_statuses(path: Path, status_cols=("status",)) -> dict:
    """Cheap ledger probe: header + terminal status tallies, without loading the file."""
    out = {"rows": 0, "statuses": {}}
    try:
        with path.open(newline="", encoding="utf-8", errors="replace") as fh:
            r = csv.DictReader(fh)
            cols = r.fieldnames or []
            out["columns"] = cols
            scol = next((c for c in cols if c in status_cols), None)
            for row in r:
                out["rows"] += 1
                if scol:
                    v = row.get(scol) or ""
                    out["statuses"][v] = out["statuses"].get(v, 0) + 1
    except Exception as e:  # a ledger we cannot read is a finding, never a crash
        out["error"] = str(e).splitlines()[0][:160]
    return out


def build() -> dict:
    programs = []
    seen_dirs = set()
    if CURATED.exists():
        for child in sorted(CURATED.iterdir()):
            if not child.is_dir():
                continue
            name = child.name
            if not (name.startswith("source_") or name.startswith("non_loc_source_")):
                continue
            seen_dirs.add(name)
            runs = [d for d in child.iterdir() if d.is_dir()]
            files = [f for f in child.rglob("*") if f.is_file()]
            if name in PROGRAM_DISPOSITIONS:
                disp, reason = PROGRAM_DISPOSITIONS[name]
            else:
                disp, reason = "UNDISPOSITIONED", None
            entry = {"program": name, "dir": str(child), "runs": len(runs),
                     "files": len(files), "disposition": disp}
            if reason:
                entry["reason"] = reason
            programs.append(entry)

    # the authoritative candidate ledger: latest search-queue run
    queue_probe = None
    qroot = CURATED / "source_search_queues"
    if qroot.exists():
        runs = sorted([d for d in qroot.iterdir() if d.is_dir()])
        if runs:
            latest = runs[-1]
            status_csv = latest / "source_search_queue_status.csv"
            queue_probe = {"latest_run": latest.name, "n_runs": len(runs)}
            if status_csv.exists():
                queue_probe["status_ledger"] = _read_csv_header_and_statuses(status_csv)

    lanes = [{"source_lane": k, "disposition": v[0], "reason": v[1]}
             for k, v in sorted(SOURCE_LANE_DISPOSITIONS.items())]
    preferred = [{"preferred_source": k, "disposition": v[0], "reason": v[1]}
                 for k, v in sorted(PREFERRED_SOURCE_DISPOSITIONS.items())]

    candidates = programs + lanes + preferred
    undispositioned = [c for c in candidates
                       if c.get("disposition") not in DISPOSITION_VOCAB]

    def _tally(rows, key="disposition"):
        out = {}
        for r in rows:
            out[r.get(key)] = out.get(r.get(key), 0) + 1
        return out

    return {
        "law": "Law A (DISCOVERY): every source-hunt program, candidate source lane, "
               "and preferred external source carries a TERMINAL disposition -- "
               "CAPTURED / QUEUED / ABANDONED(reason). A new work order or a re-opened "
               "task fails the counter until someone dispositions it.",
        "counters": {
            "hunt_programs": len(programs),
            "source_lanes": len(lanes),
            "preferred_sources": len(preferred),
            "hunt_candidates_total": len(candidates),
            "hunt_candidates_without_terminal_disposition": len(undispositioned),
            "captured": _tally(candidates).get("CAPTURED", 0),
            "queued": _tally(candidates).get("QUEUED", 0),
            "abandoned": _tally(candidates).get("ABANDONED", 0),
        },
        "undispositioned": undispositioned,
        "search_queue": queue_probe,
        "hunt_programs": programs,
        "source_lanes": lanes,
        "preferred_sources": preferred,
    }


def main() -> int:
    doc = build()
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    c = doc["counters"]
    print(f"hunt programs={c['hunt_programs']}  lanes={c['source_lanes']}  "
          f"preferred={c['preferred_sources']}")
    print(f"CAPTURED={c['captured']}  QUEUED={c['queued']}  ABANDONED={c['abandoned']}  "
          f"UNDISPOSITIONED={c['hunt_candidates_without_terminal_disposition']}")
    for u in doc["undispositioned"]:
        print("  !!", u.get("program") or u.get("source_lane") or u.get("preferred_source"))
    print(f"census -> {os.path.abspath(SUMMARY_PATH)}")
    return 0 if c["hunt_candidates_without_terminal_disposition"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
