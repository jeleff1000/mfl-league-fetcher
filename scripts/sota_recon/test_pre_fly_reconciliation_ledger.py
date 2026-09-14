import json

from scripts.sota_recon.build_pre_fly_reconciliation_ledger import build_ledger


def test_build_ledger_preserves_lane_status_and_conflict_artifact(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "recon_master_manifest.json").write_text(json.dumps({
        "generated_at_utc": "20260722T000000Z",
        "overall_status": "fail",
        "lanes": {"scoring": {"status": "review", "counts": {"mismatches": 2},
                              "headline": "2 mismatches"}},
    }))
    scoring = run_dir / "scoring_reconciliation"
    scoring.mkdir()
    (scoring / "scoring_recon_mismatches.csv").write_text(
        "year,wk,actual_pts,computed_pts,resid\n2020,1,20,18,2\n2021,2,14,16,-2\n"
    )
    ledger = build_ledger(run_dir)
    assert ledger["overall_status"] == "fail"
    assert ledger["lane_summary"][0]["lane"] == "scoring"
    assert ledger["conflicts"][0]["row_count"] == 2
    assert ledger["conflicts"][0]["conflict_class"] == "source_disagreement_or_reconstruction_residual"


def test_build_ledger_labels_fantasy_eligible_pa_separately(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "recon_master_manifest.json").write_text(json.dumps({
        "generated_at_utc": "20260722T000000Z",
        "overall_status": "fail",
        "lanes": {},
    }))
    scoring = run_dir / "golden_points"
    scoring.mkdir()
    (scoring / "pa_tier.csv").write_text("pts_allow_0,pts_allow_1_6\n1,0\n")
    ledger = build_ledger(run_dir)
    assert ledger["conflicts"][0]["measure_family"] == "fantasy_eligible_points_allowed"
