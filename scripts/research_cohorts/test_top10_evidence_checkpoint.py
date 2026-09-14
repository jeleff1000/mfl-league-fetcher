from __future__ import annotations

from top10_evidence_checkpoint import CandidateCheckpoint
from top10_stability_runner import CandidateResult


def _result(n: int) -> CandidateResult:
    return CandidateResult(
        n=n,
        total_trials=500,
        valid_trials=500,
        invalid_trials=0,
        median_rho=0.95,
        p10_rho=0.90,
        pass_share=0.97,
        pass_share_lower_95=0.95,
        passes=True,
    )


def test_checkpoint_resumes_without_recomputing_finished_candidates(tmp_path) -> None:
    path = tmp_path / "candidate_evidence.csv"
    calls: list[int] = []
    identity = {"dataset": "matchup", "metric": "clutch", "year": 2025}

    first = CandidateCheckpoint(path, identity)
    assert first.measure(100, lambda n: calls.append(n) or _result(n)) == _result(100)

    resumed = CandidateCheckpoint(path, identity)
    assert resumed.measure(100, lambda n: calls.append(n) or _result(n)) == _result(100)
    assert resumed.measure(150, lambda n: calls.append(n) or _result(n)) == _result(150)

    assert calls == [100, 150]
    assert [result.n for result in resumed.results()] == [100, 150]


def test_checkpoint_rejects_identity_drift(tmp_path) -> None:
    path = tmp_path / "candidate_evidence.csv"
    CandidateCheckpoint(path, {"dataset": "matchup", "year": 2025}).measure(
        100, _result
    )

    try:
        CandidateCheckpoint(path, {"dataset": "matchup", "year": 2024})
    except ValueError as exc:
        assert "identity" in str(exc)
    else:
        raise AssertionError("identity drift should fail closed")
