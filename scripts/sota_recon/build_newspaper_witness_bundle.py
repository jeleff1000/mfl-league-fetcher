"""
sota_recon/build_newspaper_witness_bundle.py -- freeze newspaper atoms as an immutable
sidecar-only WITNESS bundle.

Final architecture (Joe, 2026-07-17): newspaper extraction is a witness source like
NFL.com logs or PFR boxscores -- it never ships a supertable. The verified `_v26_local_only`
overlay release keeps a full 1,295-column nfl_player_stats_all.parquet beside the sidecars;
that overlay is REVIEW-ONLY/DEPRECATED for promotion. This builder copies ONLY the 11
newspaper_* sidecar parquets (plus the three hard-hold ledgers) into a write-once bundle:

    D:/league-history-data/nfl/curated/witnesses/newspaper/<release_stamp>_v1/

Guards (all fail-closed):
  * refuses any source file whose name is not newspaper_*.parquet
  * asserts nfl_player_stats_all.parquet is NEVER copied or referenced by the manifest
  * verifies expected row counts (from FINAL_PROMOTION_REVIEW_VALIDATION) before copy
  * sha256 source == sha256 destination for every file
  * refuses to overwrite an existing bundle version (immutability)
  * records sha256+size of the CURRENT latest_v26() subject before and after and
    asserts it is untouched and is NOT a *_local_only overlay

    python -m scripts.sota_recon.build_newspaper_witness_bundle            # build
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .sources import latest_v26

RELEASE = Path(r"D:\league-history-data\nfl\releases\nfl_local_release_newspaper_witness_overlay_20260717T065218Z_v26_local_only")
PACKET = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\supertable_witness_handoffs"
              r"\20260715T170438Z_newspaper_weekly_pbp_lineup_pilot_materialization_v1\PROMOTION_REVIEW_PACKET_V16")
BUNDLE_ROOT = Path(r"D:\league-history-data\nfl\curated\witnesses\newspaper")
BUNDLE_VERSION = "20260717T065218Z_v1"
FORBIDDEN = "nfl_player_stats_all.parquet"

# expected row counts from FINAL_PROMOTION_REVIEW_VALIDATION.json (checksum-verified packet)
EXPECTED_ROWS = {
    "newspaper_weekly_player_stat_cells": 1892,
    "newspaper_lineup_participation": 746,
    "newspaper_scoring_events": 1197,
    "newspaper_play_by_play_events": 678,
    "newspaper_player_game_notes": 67,
    "newspaper_team_game_stats": 922,
    "newspaper_team_game_stat_claims": 1889,
    "newspaper_game_context": 900,
    "newspaper_reviewer_source_document_notes": 4,
    "newspaper_general_reviewer_accepted_decisions": 286,
    "newspaper_general_reviewer_hold_decisions": 9,
}

# sidecar -> existing canonical table family it witnesses (the "soft landing spot")
CANONICAL_FAMILY = {
    "newspaper_play_by_play_events": {
        "family": "pbp_boxscore",
        "canonical": r"raw\pfr\boxscores\tables\pbp (per-boxscore_id) / raw\stathead\generated\pbp_merged_1978_2025 (1978+)",
        "join": "boxscore_id (+event_order); players via primary/secondary_NFL_player_id",
    },
    "newspaper_scoring_events": {
        "family": "scoring_events",
        "canonical": r"raw\pfr\boxscores\tables\scoring + derived\scoring_summary\scoring_summary.parquet",
        "join": "boxscore_id+scoring_team (+event_order); players via scoring/passer/receiver_NFL_player_id",
    },
    "newspaper_team_game_stats": {
        "family": "team_game_stats",
        "canonical": r"raw\pfr\boxscores\nfl_team_games_all.parquet + raw\pfr\boxscores\tables\team_stats",
        "join": "boxscore_id + team_1/team_2_nfl_team",
    },
    "newspaper_team_game_stat_claims": {
        "family": "team_game_stats",
        "canonical": r"raw\pfr\boxscores\nfl_team_games_all.parquet + raw\pfr\boxscores\tables\team_stats",
        "join": "boxscore_id+nfl_team (single-team unpivot of newspaper_team_game_stats via source_pair_key)",
    },
    "newspaper_weekly_player_stat_cells": {
        "family": "weekly_player_stats",
        "canonical": "v26 nfl_player_stats_all weekly grain (subject; witnessed cell-by-cell, never overwritten)",
        "join": "player_week / NFL_player_id + boxscore_id; stat_name -> canonical atom",
    },
    "newspaper_lineup_participation": {
        "family": "lineups_starters",
        "canonical": r"raw\pfr\boxscores\tables\home_starters + vis_starters",
        "join": "boxscore_id + NFL_player_id (+starter_position)",
    },
    "newspaper_game_context": {
        "family": "game_catalog",
        "canonical": r"raw\pfr\boxscores\nfl_team_games_all.parquet + tables\game_info",
        "join": "boxscore_id",
    },
    "newspaper_player_game_notes": {
        "family": "evidence_context",
        "canonical": "none (historical-record sidecar; injuries/playing-time notes)",
        "join": "player_week / NFL_player_id + boxscore_id",
    },
    "newspaper_reviewer_source_document_notes": {
        "family": "evidence_context",
        "canonical": "none (source-document evidence sidecar)",
        "join": "boxscore_id + source_document_id",
    },
    "newspaper_general_reviewer_accepted_decisions": {
        "family": "review_adjudication",
        "canonical": "none (adjudication audit ledger)",
        "join": "triage_id / target_entity_key",
    },
    "newspaper_general_reviewer_hold_decisions": {
        "family": "review_adjudication_holds",
        "canonical": "none (hold queue; never auto-promoted)",
        "join": "triage_id / target_entity_key",
    },
}

HOLD_LEDGERS = {
    "reviewer_holds": PACKET.parent / "reviewer_hold_hard_hold_audit_v1" / "reviewer_hold_hard_hold_audit.csv",
    "identity_holds": PACKET.parent / "remaining_identity_queue_hardhold_v16" / "remaining_identity_hardhold_queue.csv",
    "overlay_unmatched_holds": PACKET.parent / "remaining_overlay_unmatched_hardhold_v16" / "remaining_overlay_unmatched_hardhold.csv",
}


def _sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _subject_pin() -> dict:
    sub = Path(latest_v26())
    if "_local_only" in sub.parent.parent.name:
        raise SystemExit(f"GUARD: latest_v26() resolved to a _local_only overlay: {sub}")
    return {"path": str(sub), "bytes": sub.stat().st_size, "sha256": _sha256(sub)}


def run() -> dict:
    bundle = BUNDLE_ROOT / BUNDLE_VERSION
    if bundle.exists():
        raise SystemExit(f"GUARD: bundle version already exists (immutable): {bundle}")

    pin_before = _subject_pin()

    src_tables = sorted((RELEASE / "tables").glob("*.parquet"))
    sidecars = [p for p in src_tables if p.name.startswith("newspaper_")]
    excluded = [p.name for p in src_tables if not p.name.startswith("newspaper_")]
    if FORBIDDEN not in excluded:
        raise SystemExit(f"GUARD: expected to find+exclude {FORBIDDEN} in release; found excluded={excluded}")
    if sorted(p.stem for p in sidecars) != sorted(EXPECTED_ROWS):
        raise SystemExit(f"GUARD: sidecar set mismatch: {sorted(p.stem for p in sidecars)}")

    con = duckdb.connect()
    counts = {}
    for p in sidecars:
        n = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [p.as_posix()]).fetchone()[0]
        if n != EXPECTED_ROWS[p.stem]:
            raise SystemExit(f"GUARD: {p.stem} rows={n} expected={EXPECTED_ROWS[p.stem]}")
        counts[p.stem] = n
    con.close()

    (bundle / "tables").mkdir(parents=True)
    (bundle / "holds").mkdir()
    checks, tables_manifest = {}, {}
    for p in sidecars:
        if p.name == FORBIDDEN:
            raise SystemExit("GUARD: forbidden supertable parquet reached copy loop")
        src_sha = _sha256(p)
        dst = bundle / "tables" / p.name
        shutil.copy2(p, dst)
        dst_sha = _sha256(dst)
        if dst_sha != src_sha:
            raise SystemExit(f"GUARD: checksum mismatch after copy: {p.name}")
        checks[f"tables/{p.name}"] = dst_sha
        tables_manifest[p.stem] = {
            "file": f"tables/{p.name}", "rows": counts[p.stem], "sha256": dst_sha,
            **CANONICAL_FAMILY[p.stem],
        }

    holds_manifest = {}
    for key, src in HOLD_LEDGERS.items():
        dst = bundle / "holds" / src.name
        shutil.copy2(src, dst)
        sha = _sha256(dst)
        if sha != _sha256(src):
            raise SystemExit(f"GUARD: checksum mismatch after copy: {src.name}")
        with open(dst, encoding="utf-8", errors="replace") as f:
            n = max(sum(1 for _ in f) - 1, 0)
        checks[f"holds/{dst.name}"] = sha
        holds_manifest[key] = {"file": f"holds/{dst.name}", "rows": n, "sha256": sha}

    pin_after = _subject_pin()
    if pin_before != pin_after:
        raise SystemExit(f"GUARD: subject supertable changed during bundle build: {pin_before} -> {pin_after}")

    manifest = {
        "bundle": str(bundle),
        "bundle_version": BUNDLE_VERSION,
        "created_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "kind": "newspaper_sidecar_witness_bundle",
        "policy": {
            "witness_only": True,
            "supertable_writes_forbidden": True,
            "voting_status": "non_voting_until_identity_and_conflict_holds_clear",
            "wide_overlay_status": "deprecated_review_only",
            "forbidden_files": [FORBIDDEN],
            "allowed_table_pattern": "newspaper_*.parquet",
        },
        "provenance": {
            "source_release": str(RELEASE),
            "promotion_packet": str(PACKET),
            "excluded_from_source_release": excluded,
            "wide_overlay_note": "release also contains a 1295-col overlay supertable (203 newspaper_* cols, 789 rows touched); intentionally NOT bundled",
        },
        "subject_pin": pin_before,
        "tables": tables_manifest,
        "holds": holds_manifest,
        "hard_hold_totals": {"overlay_unmatched": 73, "identity": 1118, "reviewer": 9},
        "write_guarantee": "Created new immutable bundle dir only; no v26/live/Fly/supertable writes.",
    }
    with open(bundle / "BUNDLE_MANIFEST.json", "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)
    with open(bundle / "checksums_sha256.json", "w", encoding="utf-8") as f:
        json.dump(checks, f, indent=2)
    return manifest


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
