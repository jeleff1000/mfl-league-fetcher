"""The current UI/manual attempt must still own the lease at publication."""

import pytest

from multi_league.core.league_update_publish_claim import renew_claim_for_publication


def test_unclaimed_local_rehearsal_needs_no_writer(monkeypatch):
    for key in ("LEAGUE_UPDATE_REQUIRE_CLAIM", "LEAGUE_UPDATE_TOKEN", "LEAGUE_UPDATE_ATTEMPT_ID"):
        monkeypatch.delenv(key, raising=False)
    assert renew_claim_for_publication(object(), database_name="afi_data", platform="espn") is False


def test_required_claim_cannot_publish_without_exact_attempt(monkeypatch):
    monkeypatch.setenv("LEAGUE_UPDATE_REQUIRE_CLAIM", "1")
    monkeypatch.delenv("LEAGUE_UPDATE_TOKEN", raising=False)
    with pytest.raises(RuntimeError, match="missing exact attempt"):
        renew_claim_for_publication(object(), database_name="afi_data", platform="espn")


@pytest.mark.parametrize("accepted", (True, False))
def test_claim_is_renewed_or_stale_writer_is_rejected(monkeypatch, accepted):
    import multi_league.core.league_update_publish_claim as claim_mod

    monkeypatch.setenv("LEAGUE_UPDATE_REQUIRE_CLAIM", "1")
    monkeypatch.setenv("LEAGUE_UPDATE_TOKEN", "owned-token")
    monkeypatch.setenv("LEAGUE_UPDATE_ATTEMPT_ID", "owned-attempt")
    monkeypatch.setenv("LEAGUE_UPDATE_CLAIM_VERSION", "4")
    monkeypatch.setenv("GITHUB_RUN_ID", "12345")
    seen = []
    monkeypatch.setattr(
        claim_mod,
        "assert_league_update_entitled",
        lambda reader, *, database_name: seen.append(("entitled", database_name)),
    )
    monkeypatch.setattr(
        claim_mod,
        "record_league_update_status",
        lambda writer, **fields: seen.append(("record", fields)) or accepted,
    )
    writer = object()
    if accepted:
        assert renew_claim_for_publication(
            object(), database_name="afi_data", platform="espn", writer=writer
        ) is True
    else:
        with pytest.raises(RuntimeError, match="no longer owns"):
            renew_claim_for_publication(
                object(), database_name="afi_data", platform="espn", writer=writer
            )
    assert seen[0] == ("entitled", "afi_data")
    assert seen[1][1] == {
        "database_name": "afi_data",
        "platform": "espn",
        "status": "running",
        "dispatch_token": "owned-token",
        "attempt_id": "owned-attempt",
        "claim_version": 4,
        "workflow_run_id": "12345",
    }


def test_expired_entitlement_prevents_lease_renewal(monkeypatch):
    import multi_league.core.league_update_publish_claim as claim_mod

    monkeypatch.setenv("LEAGUE_UPDATE_REQUIRE_CLAIM", "1")
    monkeypatch.setenv("LEAGUE_UPDATE_TOKEN", "owned-token")
    monkeypatch.setenv("LEAGUE_UPDATE_ATTEMPT_ID", "owned-attempt")
    monkeypatch.setenv("LEAGUE_UPDATE_CLAIM_VERSION", "4")
    monkeypatch.setattr(
        claim_mod,
        "assert_league_update_entitled",
        lambda reader, *, database_name: (_ for _ in ()).throw(PermissionError("expired")),
    )
    with pytest.raises(PermissionError, match="expired"):
        renew_claim_for_publication(
            object(), database_name="afi_data", platform="espn", writer=object()
        )
