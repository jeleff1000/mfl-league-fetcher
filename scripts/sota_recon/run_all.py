"""
sota_recon/run_all.py  --  the single entry point

Runs every reconciliation lane against the current v26 release into ONE timestamped
run directory, then writes a machine manifest and a human RECON_SUMMARY.md so the
whole audit can be read from one place.

    python -m scripts.sota_recon.run_all
    python -m scripts.sota_recon.run_all --only oracle,internal   # subset

Lanes
  context_gates CONTEXT/COVERAGE/ENTITY absolute-count FAIL gates (self-play, player-team vs box
              witness, team targets>attempts, witnessed-but-empty columns, dup player-weeks).
              Added 2026-07-16 -- see runbook 10 for why the value-agreement lanes missed these.
  internal    physical impossibilities + cross-side identities (+ DEF backfill candidates)
  schedule    independent calendar anchor (existence, scores, dates, home/away backfill)
  team_total  vertical sum-of-players vs independent team totals (missing/dup players)
  oracle      second-source corroboration vs PBP player-week rollup (1978-2025)
  seam        year-over-year discontinuity detection (transform/units artifacts)

Overall verdict:
  fail   -> any lane failed (e.g. a physical impossibility exists)
  review -> no failures, but findings worth human triage
  pass   -> clean
"""

from __future__ import annotations

import argparse
import json
import os

from . import (recon_internal, recon_oracle, recon_schedule, recon_seam,
               recon_teamtotal, recon_scoring, recon_season_authority, golden_samples,
               recon_expectations, recon_player_season, recon_aggregate, recon_identity_splits,
               recon_identity_ids, recon_bounce, golden_grid, golden_points, recon_column_census,
               recon_context_gates, recon_bio_vs_pfr, recon_newspaper_sidecar,
               recon_pfr_scoring_vs_v26)
from .recon_common import new_run_dir, source_pin, utc_stamp, write_manifest
from .sources import registry


def _season_authority_lane(run_dir: str) -> dict:
    """v26 REG season totals vs the published record book (PFR player pages)."""
    r = recon_season_authority.run()
    counts = {f"{era}_{stat}": pct for era, stat, pct, _floor, _ok in r["results"]}
    counts["era_stat_checks_at_floor"] = f"{r['passed']}/{r['total']}"
    status = "pass" if r["passed"] == r["total"] else "review"
    return {"status": status, "counts": counts}


def _golden_lane(run_dir: str) -> dict:
    """Known-truth anchors (records, landmark seasons, era gates). A FAIL = real regression."""
    r = golden_samples.run()
    status = "pass" if r["failed"] == 0 else "fail"
    return {"status": status, "counts": {"passed": r["passed"], "total": r["total"],
                                         "failed": r["failed"]}}


def _expectations_lane(run_dir: str) -> dict:
    """Identity (one player=one id, one row/week), bounds, and position-aware coverage."""
    r = recon_expectations.run()
    n = r["total_discrepancies"]
    return {"status": "pass" if n == 0 else "review",
            "counts": {**r["by_check"], "total_discrepancies": n}}


def _column_census_lane(run_dir: str) -> dict:
    """Per-column sanity across ALL ~800 columns: ceiling-pair impossibilities (FAIL),
    string-typed-numeric columns (review), all-null/constant sentinels (review).

    2026-07-16: ALL_NULL/CONSTANT were 'informational' and therefore invisible -- `pick6` sat CONSTANT
    at 0 for the table's entire life and this lane counted it without ever raising. A dead stat column
    is now at least triageable here; the witnessed-era version of the check FAILS in
    recon_context_gates.COVERAGE.dead_column."""
    r = recon_column_census.run()
    nceil = len(r["ceiling_pair_violations"])
    ntype = r["flag_counts"].get("TYPE_STR_NUMERIC", 0)
    ndead = r["flag_counts"].get("ALL_NULL", 0) + r["flag_counts"].get("CONSTANT", 0)
    status = "fail" if nceil else ("review" if (ntype or ndead) else "pass")
    return {"status": status, "counts": {"columns": r["n_columns"],
            "ceiling_violations": nceil, **r["flag_counts"]}}


def _identity_splits_lane(run_dir: str) -> dict:
    """One human carrying >1 NFL_player_id. MATERIAL (merge) = pfr_id or DOB+name split where
    both ids carry stats, plausible age, no same-week-different-team conflict. Other verdicts
    (BIO_STUB / DOB_ERROR / DIFFERENT_PEOPLE) are not stat-affecting splits."""
    r = recon_identity_splits.run()
    status = "fail" if r["material_split_groups"] else "pass"
    return {"status": status, "counts": {"candidate_groups": r["candidate_groups"],
            "material": r["material_split_groups"], **r["by_verdict"],
            "name_collisions_excluded": r["name_collisions_distinct_dob"]}}


def _golden_grid_lane(run_dir: str) -> dict:
    """~1,150 frozen leader anchors (every stat x decade x game/season/career + team defense).
    FAIL = a leader drifted from the frozen master or exceeded an all-time ceiling."""
    r = golden_grid.run()
    if r["action"] in ("frozen", "refrozen"):
        return {"status": "pass", "counts": {"frozen_anchors": r["anchors"],
                "ceiling_violations": len(r["ceiling_violations"])}}
    status = "fail" if r["failed"] else "pass"
    return {"status": status, "counts": {"anchors": r["total"], "passed": r["passed"],
            "drifted": len(r["drift"]), "new_cells": r["new_cells"],
            "ceiling_violations": len(r["ceiling_violations"])}}


def _golden_points_lane(run_dir: str) -> dict:
    """POINTS/scoring golden matrix: recompute each scoring column (pts_def_std, pts_idp_std,
    pts_k_std + de-dup guards) from atoms and assert exact match -- the bug-catcher for the
    derived points layer (caught the IDP pick-six drop + stale K). Plus frozen points leaders at
    week/season/career. FAIL = a formula regression or a frozen leader drifted."""
    r = golden_points.run()
    if r["action"] == "frozen":
        return {"status": "pass" if r["formula_fail"] == 0 else "fail",
                "counts": {"frozen_leaders": r["leaders"], "formula_fail": r["formula_fail"]}}
    status = "pass" if r["passed_overall"] else "fail"
    return {"status": status, "counts": {"formula_fail": r["formula_fail"],
            "leaders": r["leaders"], "drift": len(r["drift"])}}


def _aggregate_lane(run_dir: str) -> dict:
    """Season/career aggregate tables: every column tied back to weekly (season=Sumweekly,
    career=Sumseason), games, dense/unique ranks, ppg, finite lamar. FAIL = real regression."""
    r = recon_aggregate.run()
    status = "pass" if r["failed"] == 0 else "fail"
    return {"status": status, "counts": {"passed": r["passed"], "total": r["total"],
                                         "failed": r["failed"]}}


def _player_season_lane(run_dir: str) -> dict:
    """EVERY player-season vs the published record book (catches per-player errors the
    league-year aggregate masks). Modern eras should be ~99%+; pre-1950 is era-limited."""
    r = recon_player_season.run()
    modern = [pct for era, _n, pct in r["by_era"] if era in ("1978-2009", "2010-2025")]
    status = "pass" if (modern and min(modern) >= 99.0) else "review"
    return {"status": status, "counts": {"player_seasons": r["player_seasons_compared"],
            "clean_pct": r["clean_player_seasons_pct"],
            "discrepant_cells": r["discrepant_cells"]}}


def _witness_gate_lane(run_dir: str) -> dict:
    """Run the universal typed gate; legacy vote output cannot authorize this lane."""
    from pathlib import Path

    from .witness_gate.cli import run_all as run_witness_gate
    from .witness_gate.models import GatePlane

    lake_root = Path(r"D:\league-history-data\nfl")
    contracts = Path(__file__).parent / "witness_gate" / "contracts"
    manifests = run_witness_gate(
        lake_root=lake_root,
        source_census_path=contracts / "source_census.v1.json",
        field_registry_path=contracts / "field_mappings.v1.json",
        output_dir=Path(run_dir) / "witness_gate",
        candidate_version="legacy-recon-candidate-v1",
        pbp_rollup_path=(
            lake_root
            / "raw/stathead/generated/pbp_supertable_audit_1978_2025/pbp_player_week_rollup.parquet"
        ),
        pbp_contract_path=contracts / "pbp_contracts.v1.json",
    )
    release = manifests[GatePlane.RELEASE]
    return {
        "status": release.status,
        "counts": {plane.value: len(manifest.findings) for plane, manifest in manifests.items()},
    }


def _context_gates_lane(run_dir: str) -> dict:
    """CONTEXT/COVERAGE/ENTITY gates (2026-07-16). Absolute-count FAIL gates -- never percentages --
    for the classes ~20 value-agreement lanes slept through (runbook 10): a team playing itself, a
    player filed under the wrong team vs the box witness (JAX 2001-02), team targets > team attempts,
    a witnessed column empty across its witnessed era (pick6), and same-player-same-game duplicates.
    Any nonzero count FAILS: a rate gate cannot see a localized cluster (16 bad games = 0.065%)."""
    r = recon_context_gates.run(csv_dir=os.path.join(run_dir, "context_gates"))
    return {"status": r["status"],
            "counts": {k: v["violations"] for k, v in r["gates"].items()}}


def _bio_vs_pfr_lane(run_dir):
    r = recon_bio_vs_pfr.run()  # standalone lane; writes its own fixed dir
    return {"status": r["status"],
            "counts": {"forward_gap_mintable": r["forward_gap_mintable"],
                       "relink": r["forward_gap_relink_existing_row"],
                       "twin_review": r["forward_gap_twin_review"],
                       "ghost": r["forward_gap_index_only_ghost"],
                       "reverse_drift": r["reverse_drift_bio_ids_not_in_pfr"]}}


def _newspaper_sidecar_lane(run_dir):
    r = recon_newspaper_sidecar.run()  # standalone lane; writes its own fixed dir
    return {"status": r["status"],
            "counts": {"structural": r["structural_violations"],
                       "adjudication_queue": r["adjudication_queue_total"]}}


def _pfr_scoring_lane(run_dir):
    r = recon_pfr_scoring_vs_v26.run()  # standalone lane; writes its own fixed dir
    return {"status": r["status"],
            "counts": {"pfr_higher": r["totals"]["pfr_higher"],
                       "v26_higher": r["totals"]["v26_higher"]}}


LANES = {
    "witness_gate":     _witness_gate_lane,
    "context_gates":    _context_gates_lane,
    "bio_vs_pfr":       _bio_vs_pfr_lane,
    "newspaper_sidecar": _newspaper_sidecar_lane,
    "pfr_scoring":      _pfr_scoring_lane,
    "internal":         recon_internal.run,
    "schedule":         recon_schedule.run,
    "team_total":       recon_teamtotal.run,
    "oracle":           recon_oracle.run,
    "seam":             recon_seam.run,
    "scoring":          recon_scoring.run,
    "season_authority": _season_authority_lane,
    "player_season":    _player_season_lane,
    "aggregate":        _aggregate_lane,
    "column_census":    _column_census_lane,
    "identity_splits":  _identity_splits_lane,
    "identity_ids":     recon_identity_ids.run,
    "bounce":           recon_bounce.run,
    "golden":           _golden_lane,
    "golden_grid":      _golden_grid_lane,
    "golden_points":    _golden_points_lane,
    "expectations":     _expectations_lane,
}

_RANK = {"fail": 2, "review": 1, "pass": 0}


def _overall(statuses: list[str]) -> str:
    worst = max((_RANK.get(s, 1) for s in statuses), default=0)
    return {2: "fail", 1: "review", 0: "pass"}[worst]


def _summary_md(run_dir: str, master: dict) -> str:
    reg = registry()
    lines = []
    a = lines.append
    a(f"# SOTA Reconciliation - {master['overall_status'].upper()}")
    a("")
    a(f"- generated: `{master['generated_at_utc']}`")
    a(f"- subject:   `{reg['v26_release'].path}`")
    a(f"- run dir:   `{run_dir}`")
    a("")
    a("## Verdict by lane")
    a("")
    a("| Lane | Status | Headline |")
    a("|------|--------|----------|")
    for lane, m in master["lanes"].items():
        a(f"| {lane} | **{m['status']}** | {m.get('headline','')} |")
    a("")
    a("## Sources (all D: data lake)")
    a("")
    a("| key | role | years | exists |")
    a("|-----|------|-------|--------|")
    for k, s in master["source_pin"]["sources"].items():
        a(f"| {k} | {s['role']} | {s['year_min']}-{s['year_max']} | {'yes' if s['exists'] else 'NO'} |")
    a("")
    a("## Findings to triage")
    a("")
    for lane, m in master["lanes"].items():
        c = m.get("counts", {})
        nonzero = {k: v for k, v in c.items() if isinstance(v, (int, float)) and v}
        if nonzero:
            a(f"**{lane}**")
            for k, v in nonzero.items():
                a(f"- {k}: {v:,}" if isinstance(v, int) else f"- {k}: {v}")
            a("")
    a("## Notes")
    a("- `review` is expected on first pass: it flags items for human triage, not breakage.")
    a("- Coverage gaps (e.g. DEF INT/sack NULL) are reported as *backfill candidates*, "
      "separately from true mismatches, so real bugs are not buried.")
    a("- Tolerances and expected era-gaps live in `scripts/sota_recon/asymmetry_registry.py`.")
    return "\n".join(lines) + "\n"


def _headline(lane: str, m: dict) -> str:
    c = m.get("counts", {})
    if lane == "witness_gate":
        return (
            f"global {c.get('global_research_health', 0)}, source {c.get('source_health', 0)}, "
            f"candidate {c.get('candidate_promotion', 0)}, release {c.get('release', 0)} findings"
        )
    if lane == "context_gates":
        return (f"self_play {c.get('CONTEXT.self_play',0)}, team_vs_box {c.get('CONTEXT.team_vs_box',0)}, "
                f"targets>att {c.get('CONTEXT.targets_gt_att',0)}, dead_col {c.get('COVERAGE.dead_column',0)}, "
                f"dup_week {c.get('IDENTITY.dup_player_week',0)}")
    if lane == "bio_vs_pfr":
        return (f"{c.get('forward_gap_mintable',0)} mintable, {c.get('relink',0)} relink, "
                f"{c.get('twin_review',0)} twin-review, {c.get('ghost',0)} ghost | "
                f"{c.get('reverse_drift',0)} reverse-drift")
    if lane == "newspaper_sidecar":
        return (f"{c.get('structural',0)} structural, "
                f"{c.get('adjudication_queue',0)} adjudication-queue")
    if lane == "pfr_scoring":
        return (f"{c.get('pfr_higher',0)} v26-undercounts, {c.get('v26_higher',0)} v26-overcounts "
                f"vs PFR scorer attribution (report-only)")
    if lane == "oracle":
        return (f"{c.get('overall_agree_pct','?')}% agree across "
                f"{c.get('cells_compared',0):,} cells; {c.get('unmatched_player_weeks',0):,} unmatched")
    if lane == "schedule":
        return (f"{c.get('match_pct_normalized','?')}% matched; "
                f"{c.get('score_mismatches',0)} score mismatches; "
                f"{c.get('calendar_year_violations',0)} calendar violations")
    if lane == "team_total":
        return (f"{c.get('missing_player_suspects',0):,} missing / "
                f"{c.get('dup_player_suspects',0):,} dup suspects")
    if lane == "internal":
        return (f"{c.get('hard_violations',0)} hard, {c.get('cross_flags',0):,} cross, "
                f"{c.get('def_backfill_candidates',0):,} DEF backfill")
    if lane == "seam":
        return f"{c.get('seam_flags',0)} discontinuity flags"
    if lane == "scoring":
        return (f"{c.get('exact_pct','?')}% reconcile to scoreboard "
                f"({c.get('exact_pct_2015plus','?')}% in 2015+); {c.get('mismatches',0):,} mismatches")
    if lane == "season_authority":
        return f"{c.get('era_stat_checks_at_floor','?')} era-stat checks at/above record-book floor"
    if lane == "player_season":
        return (f"{c.get('clean_pct','?')}% of {c.get('player_seasons',0):,} player-seasons "
                f"clean vs record book ({c.get('discrepant_cells',0):,} cells)")
    if lane == "aggregate":
        return f"{c.get('passed',0)}/{c.get('total',0)} season/career aggregate checks pass"
    if lane == "column_census":
        return (f"{c.get('columns',0)} cols | {c.get('ceiling_violations',0)} ceiling-impossibilities, "
                f"{c.get('TYPE_STR_NUMERIC',0)} str-numeric, {c.get('ALL_NULL',0)} all-null, {c.get('CONSTANT',0)} constant")
    if lane == "identity_splits":
        return (f"{c.get('material',0)} material splits to merge | "
                f"{c.get('candidate_groups',0)} candidates "
                f"(BIO_STUB {c.get('BIO_STUB',0)}, DOB_ERROR {c.get('DOB_ERROR',0)}, "
                f"DIFFERENT_PEOPLE {c.get('DIFFERENT_PEOPLE',0)})")
    if lane == "golden_grid":
        return (f"{c.get('passed', c.get('frozen_anchors',0))}/{c.get('anchors', c.get('frozen_anchors',0))} "
                f"frozen leader anchors stable; {c.get('drifted',0)} drift, {c.get('ceiling_violations',0)} ceiling")
    if lane == "bounce":
        return f"{c.get('min_match_pct',0)}% min allowed-vs-gained match ({c.get('games',0):,} modern team-games)"
    if lane == "identity_ids":
        return (f"{c.get('orphan_stats',0)} stat-ids w/o bio, {c.get('dup_bio',0)} dup-bio, "
                f"{c.get('reverse_split_total',0)} reverse-id-splits ({c.get('bio_no_stats',0):,} bio w/o stats, info)")
    if lane == "golden_points":
        return (f"{c.get('formula_fail',0)} scoring-formula regressions | "
                f"{c.get('frozen_leaders', c.get('leaders',0))} frozen points leaders (wk/season/career), "
                f"{c.get('drift',0)} drift")
    if lane == "golden":
        return f"{c.get('passed',0)}/{c.get('total',0)} known-truth anchors pass"
    if lane == "expectations":
        return (f"{c.get('total_discrepancies',0):,} candidates "
                f"({c.get('identity.dup_person',0)} dup-person, "
                f"{c.get('identity.dup_player_week',0)} dup-week, "
                f"{c.get('bounds.exceeds_ceiling',0)} bounds)")
    return ""


def main() -> None:
    ap = argparse.ArgumentParser(description="Run the SOTA reconciliation harness vs v26")
    ap.add_argument("--only", help="comma list of lanes to run (default: all)")
    args = ap.parse_args()

    want = [x.strip() for x in args.only.split(",")] if args.only else list(LANES)
    bad = [x for x in want if x not in LANES]
    if bad:
        raise SystemExit(f"unknown lane(s): {bad}. valid: {list(LANES)}")

    stamp = utc_stamp()
    run_dir = new_run_dir(stamp)
    print(f"run dir: {run_dir}\n")

    lanes_out: dict[str, dict] = {}
    for lane in want:
        print(f"  >> {lane} ...", flush=True)
        m = LANES[lane](run_dir)
        m["headline"] = _headline(lane, m)
        lanes_out[lane] = m
        print(f"     [{m['status']}] {m['headline']}")

    overall = _overall([m["status"] for m in lanes_out.values()])
    master = {
        "generated_at_utc": stamp,
        "overall_status": overall,
        "subject_release": registry()["v26_release"].path,
        "lanes": {
            lane: {"status": m["status"], "counts": m.get("counts", {}),
                   "headline": m.get("headline", "")}
            for lane, m in lanes_out.items()
        },
        "source_pin": source_pin(),
    }
    write_manifest(os.path.join(run_dir, "recon_master_manifest.json"), master)

    md = _summary_md(run_dir, master)
    with open(os.path.join(run_dir, "RECON_SUMMARY.md"), "w", encoding="utf-8") as f:
        f.write(md)

    print(f"\nOVERALL: {overall.upper()}")
    print(f"summary: {os.path.join(run_dir, 'RECON_SUMMARY.md')}")
    print(f"manifest: {os.path.join(run_dir, 'recon_master_manifest.json')}")


if __name__ == "__main__":
    main()
