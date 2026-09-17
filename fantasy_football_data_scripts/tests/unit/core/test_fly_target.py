"""Unit tests for FlyTarget publish helpers."""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, call, patch

import pytest
import requests

from multi_league.core.targets.fly_target import FlyTarget


@pytest.fixture
def fly_env(monkeypatch):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-token")


def test_constructor_rejects_corpus_mode_even_with_credentials(fly_env, monkeypatch):
    monkeypatch.setenv("CORPUS_MODE", "yes")

    with pytest.raises(RuntimeError, match="corpus mode"):
        FlyTarget()


def test_mark_league_imported_finalizes_inventory_row(fly_env):
    target = FlyTarget()
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = []

    with patch("multi_league.core.fly_writer.requests.post", return_value=mock_resp) as mock_post:
        target.mark_league_imported("no_fun_league_xl", import_mode="full", platform="yahoo")

    args, kwargs = mock_post.call_args
    assert args[0] == "https://fly.test/query-rw"
    assert kwargs["headers"]["Authorization"] == "Bearer admin-token"
    assert kwargs["json"]["database"] == "___ops"

    sql = kwargs["json"]["sql"]
    assert "UPDATE accounts.league_inventory" in sql
    assert "INSERT INTO accounts.league_inventory" in sql
    assert "last_import_at = current_timestamp" in sql
    assert "in_centralized = TRUE" in sql
    assert "import_status = 'complete'" in sql
    assert "last_import_mode = COALESCE('full', last_import_mode)" in sql
    assert "database_name = 'no_fun_league_xl'" in sql


def test_mark_league_imported_escapes_sql_literals(fly_env):
    target = FlyTarget()
    mock_resp = Mock(status_code=200)
    mock_resp.json.return_value = []

    with patch("multi_league.core.fly_writer.requests.post", return_value=mock_resp) as mock_post:
        target.mark_league_imported("o'hare", import_mode="quick", platform="yahoo")

    sql = mock_post.call_args.kwargs["json"]["sql"]
    assert "o''hare" in sql


def _resp(status_code, payload=None, text=""):
    response = Mock()
    response.status_code = status_code
    response.text = text
    response.headers = {}
    response.json.return_value = payload if payload is not None else {"ok": True}
    return response


def test_merge_league_retries_ssl_eof_then_succeeds(fly_env, tmp_path):
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()
    sequence = [
        requests.exceptions.SSLError("EOF occurred in violation of protocol"),
        _resp(200),
    ]

    with (
        patch("multi_league.core.targets.fly_target.requests.post", side_effect=sequence) as mock_post,
        patch("multi_league.core.targets.fly_target.time.sleep") as mock_sleep,
    ):
        result = target.merge_league("td_s_beer", db_path)

    assert result["ok"] is True
    assert mock_post.call_count == 2
    mock_sleep.assert_called_once_with(10)


def test_rename_league_posts_only_identity_metadata(fly_env):
    target = FlyTarget()
    with patch(
        "multi_league.core.targets.fly_target.requests.post",
        return_value=_resp(
            200,
            {
                "status": "COMMITTED",
                "source_db": "agustafantasyleague",
                "target_db": "agusta_fantasy_league",
            },
        ),
    ) as post:
        result = target.rename_league(
            source_db="agustafantasyleague",
            target_db="agusta_fantasy_league",
            display_name="Agusta Fantasy League",
            operation_id="rename-agusta",
        )

    assert result["status"] == "COMMITTED"
    args, kwargs = post.call_args
    assert args[0] == "https://fly.test/rename-league"
    assert kwargs["headers"] == {"Authorization": "Bearer admin-token"}
    assert kwargs["json"] == {
        "source_db": "agustafantasyleague",
        "target_db": "agusta_fantasy_league",
        "display_name": "Agusta Fantasy League",
        "operation_id": "rename-agusta",
    }
    assert "credential" not in str(kwargs["json"]).lower()


def test_merge_fleet_partition_posts_a_scoped_bundle(fly_env, tmp_path):
    bundle_path = tmp_path / "draft-2026.tar.gz"
    bundle_path.write_bytes(b"fleet-bundle")
    target = FlyTarget()

    with patch(
        "multi_league.core.targets.fly_target.requests.post",
        return_value=_resp(200, {"status": "COMMITTED", "tables": {"draft": 168}}),
    ) as mock_post:
        result = target.merge_fleet_partition(
            bundle_path,
            bundle_id="fleet-2026-draft",
            bundle_hash="bundle-hash",
        )

    assert result["status"] == "COMMITTED"
    args, kwargs = mock_post.call_args
    assert args[0] == "https://fly.test/merge-fleet-partition"
    assert kwargs["headers"]["X-Db-Name"] == "___fleet"
    assert kwargs["headers"]["X-Bundle-Id"] == "fleet-2026-draft"
    assert kwargs["headers"]["X-Bundle-Hash"] == "bundle-hash"


def test_fleet_publish_reconciles_a_post_commit_http_failure(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"fleet")
    target = FlyTarget()
    target.max_upload_retries = 1
    with (
        patch("multi_league.core.targets.fly_target.requests.post", return_value=_resp(500, {"detail": "response failed"}, text="response failed")),
        patch.object(target, "get_delta_merge_status", return_value={"status": "COMMITTED", "bundle_id": "fleet-hash", "bundle_hash": "hash"}) as status,
    ):
        result = target.merge_fleet_partition(bundle, bundle_id="fleet-hash", bundle_hash="hash")
    assert result["status"] == "COMMITTED"
    assert result["recovered_after_ambiguous_failure"] is True
    status.assert_called_once_with("___fleet", "fleet-hash")


def test_fleet_publish_reconciles_a_commit_followed_by_retry_conflict(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"fleet")
    target = FlyTarget()
    target.max_upload_retries = 1
    with (
        patch("multi_league.core.targets.fly_target.requests.post", return_value=_resp(409, {"detail": "retry collided"}, text="retry collided")),
        patch.object(target, "get_delta_merge_status", return_value={"status": "COMMITTED", "bundle_id": "fleet-hash", "bundle_hash": "hash"}) as status,
    ):
        result = target.merge_fleet_partition(bundle, bundle_id="fleet-hash", bundle_hash="hash")
    assert result["status"] == "COMMITTED"
    assert result["recovered_after_ambiguous_failure"] is True
    status.assert_called_once_with("___fleet", "fleet-hash")


def test_fleet_publish_rejects_an_ambiguous_receipt_for_another_content_hash(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"fleet")
    target = FlyTarget()
    target.max_upload_retries = 1
    with (
        patch("multi_league.core.targets.fly_target.requests.post", return_value=_resp(500, {"detail": "response failed"}, text="response failed")),
        patch.object(target, "get_delta_merge_status", return_value={"status": "COMMITTED", "bundle_id": "fleet-hash", "bundle_hash": "other"}),
    ):
        with pytest.raises(RuntimeError, match="bundle hash mismatch"):
            target.merge_fleet_partition(bundle, bundle_id="fleet-hash", bundle_hash="hash")


def test_fleet_publish_retries_the_same_bundle_only_when_no_commit_was_recorded(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"fleet")
    target = FlyTarget()
    target.max_upload_retries = 1
    with (
        patch("multi_league.core.targets.fly_target.requests.post", side_effect=[
            _resp(500, {"detail": "response failed"}, text="response failed"),
            _resp(200, {"status": "COMMITTED", "bundle_id": "fleet-hash", "bundle_hash": "hash"}),
        ]) as post,
        patch.object(target, "get_delta_merge_status", return_value={"status": "UNKNOWN"}) as status,
    ):
        with pytest.raises(RuntimeError, match="Fleet partition merge failed"):
            target.merge_fleet_partition(bundle, bundle_id="fleet-hash", bundle_hash="hash")
        result = target.merge_fleet_partition(bundle, bundle_id="fleet-hash", bundle_hash="hash")
    assert result["status"] == "COMMITTED"
    assert post.call_count == 2
    assert [call.kwargs["headers"]["X-Bundle-Id"] for call in post.call_args_list] == ["fleet-hash", "fleet-hash"]
    status.assert_called_once_with("___fleet", "fleet-hash")


def test_merge_league_targets_primary_only(fly_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FLY_PRIMARY_MACHINE_ID", "machine-primary")
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()

    with patch(
        "multi_league.core.targets.fly_target.requests.post", return_value=_resp(200, {"merged": True})
    ) as mock_post:
        result = target.merge_league("td_s_beer", db_path)

    assert result["merged"] is True
    assert mock_post.call_count == 1
    forced_ids = [call.kwargs["headers"]["fly-force-instance-id"] for call in mock_post.call_args_list]
    assert forced_ids == ["machine-primary"]


def test_merge_league_retries_transient_5xx_then_succeeds(fly_env, tmp_path):
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()
    sequence = [_resp(502, text="bad gateway"), _resp(200, {"merged": True})]

    with (
        patch("multi_league.core.targets.fly_target.requests.post", side_effect=sequence) as mock_post,
        patch("multi_league.core.targets.fly_target.time.sleep") as mock_sleep,
    ):
        result = target.merge_league("td_s_beer", db_path)

    assert result["merged"] is True
    assert mock_post.call_count == 2
    mock_sleep.assert_called_once_with(10)


def test_merge_league_retry_delay_can_be_tuned_with_env(fly_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FLY_UPLOAD_RETRY_BASE_DELAY", "3")
    monkeypatch.setenv("FLY_UPLOAD_RETRY_MAX_DELAY", "4")
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()
    sequence = [_resp(502, text="bad gateway"), _resp(502, text="bad gateway"), _resp(200, {"merged": True})]

    with (
        patch("multi_league.core.targets.fly_target.requests.post", side_effect=sequence) as mock_post,
        patch("multi_league.core.targets.fly_target.time.sleep") as mock_sleep,
    ):
        result = target.merge_league("td_s_beer", db_path)

    assert result["merged"] is True
    assert mock_post.call_count == 3
    assert mock_sleep.call_args_list == [call(3.0), call(4.0)]


def test_merge_league_retry_delay_uses_jitter_to_spread_parallel_workers(fly_env, monkeypatch, tmp_path):
    monkeypatch.setenv("FLY_UPLOAD_RETRY_BASE_DELAY", "3")
    monkeypatch.setenv("FLY_UPLOAD_RETRY_MAX_DELAY", "10")
    monkeypatch.setenv("FLY_UPLOAD_RETRY_JITTER_SECONDS", "2")
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()
    sequence = [_resp(502, text="bad gateway"), _resp(200, {"merged": True})]

    with (
        patch("multi_league.core.targets.fly_target.requests.post", side_effect=sequence),
        patch("multi_league.core.targets.fly_target.random.uniform", return_value=1.25),
        patch("multi_league.core.targets.fly_target.time.sleep") as mock_sleep,
    ):
        result = target.merge_league("td_s_beer", db_path)

    assert result["merged"] is True
    mock_sleep.assert_called_once_with(4.25)


def test_retry_after_delay_keeps_jitter_inside_configured_cap(fly_env, monkeypatch):
    monkeypatch.setenv("FLY_UPLOAD_RETRY_MAX_DELAY", "10")
    monkeypatch.setenv("FLY_UPLOAD_RETRY_JITTER_SECONDS", "2")
    target = FlyTarget()
    resp = _resp(429)
    resp.headers = {"Retry-After": "9.5"}

    with patch("multi_league.core.targets.fly_target.random.uniform", return_value=1.5):
        assert target._retry_delay(0, resp) == 10


def test_merge_league_survives_long_transient_upload_burst(fly_env, tmp_path):
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()
    sequence = [_resp(502, text="bad gateway") for _ in range(FlyTarget.MAX_UPLOAD_RETRIES - 1)]
    sequence.append(_resp(200, {"merged": True}))

    with (
        patch("multi_league.core.targets.fly_target.requests.post", side_effect=sequence) as mock_post,
        patch("multi_league.core.targets.fly_target.time.sleep") as mock_sleep,
    ):
        result = target.merge_league("td_s_beer", db_path)

    assert result["merged"] is True
    assert mock_post.call_count == FlyTarget.MAX_UPLOAD_RETRIES
    assert mock_sleep.call_count == FlyTarget.MAX_UPLOAD_RETRIES - 1


def test_merge_league_handles_thirty_parallel_client_uploads(fly_env, tmp_path):
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()

    with patch(
        "multi_league.core.targets.fly_target.requests.post", return_value=_resp(200, {"merged": True})
    ) as mock_post:
        with ThreadPoolExecutor(max_workers=30) as pool:
            results = list(pool.map(lambda i: target.merge_league(f"league_{i}", db_path), range(30)))

    assert len(results) == 30
    assert all(result["merged"] is True for result in results)
    assert mock_post.call_count == 30


def test_merge_league_does_not_retry_client_errors(fly_env, tmp_path):
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()

    with (
        patch("multi_league.core.targets.fly_target.requests.post", return_value=_resp(401, text="nope")) as mock_post,
        patch("multi_league.core.targets.fly_target.time.sleep") as mock_sleep,
    ):
        with pytest.raises(RuntimeError, match=r"401"):
            target.merge_league("td_s_beer", db_path)

    assert mock_post.call_count == 1
    mock_sleep.assert_not_called()


def test_merge_league_does_not_retry_corrupt_database_response(fly_env, tmp_path):
    db_path = tmp_path / "league.duckdb"
    db_path.write_bytes(b"duckdb")
    target = FlyTarget()
    response = _resp(
        500,
        text=(
            '{"detail":"IO Error: Corrupt database file: computed checksum '
            'does not match stored checksum"}'
        ),
    )

    with (
        patch("multi_league.core.targets.fly_target.requests.post", return_value=response) as mock_post,
        patch("multi_league.core.targets.fly_target.time.sleep") as mock_sleep,
    ):
        with pytest.raises(RuntimeError, match="500"):
            target.merge_league("td_s_beer", db_path)

    assert mock_post.call_count == 1
    mock_sleep.assert_not_called()


def test_merge_league_delta_recovers_committed_status_after_transport_failure(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"delta")
    target = FlyTarget()
    target.max_upload_retries = 1

    with (
        patch(
            "multi_league.core.targets.fly_target.requests.post",
            side_effect=requests.exceptions.ConnectionError("lost response"),
        ),
        patch("multi_league.core.targets.fly_target.time.sleep"),
        patch.object(
            target,
            "get_delta_merge_status",
            return_value={"status": "COMMITTED", "bundle_id": "bundle-1", "bundle_hash": "hash-1"},
        ) as mock_status,
    ):
        result = target.merge_league_delta("td_s_beer", bundle, bundle_id="bundle-1", bundle_hash="hash-1")

    assert result["status"] == "COMMITTED"
    assert result["recovered_after_ambiguous_failure"] is True
    mock_status.assert_called_once_with("td_s_beer", "bundle-1")


def test_merge_league_delta_rejects_conflict(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"delta")
    target = FlyTarget()

    with patch(
        "multi_league.core.targets.fly_target.requests.post",
        return_value=_resp(409, {"status": "CONFLICT"}, text="conflict"),
    ), patch.object(target, "get_delta_merge_status", return_value={"status": "CONFLICT"}):
        with pytest.raises(RuntimeError, match="conflict"):
            target.merge_league_delta("td_s_beer", bundle, bundle_id="bundle-1", bundle_hash="hash-1")


def test_merge_league_delta_skips_stale_bundle_conflict(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"delta")
    target = FlyTarget()

    with patch(
        "multi_league.core.targets.fly_target.requests.post",
        return_value=_resp(
            409,
            {"detail": "Older bundle cannot commit over newer committed state"},
            text='{"detail":"Older bundle cannot commit over newer committed state"}',
        ),
    ), patch.object(target, "get_delta_merge_status", return_value={"status": "CONFLICT"}):
        result = target.merge_league_delta("td_s_beer", bundle, bundle_id="bundle-1", bundle_hash="hash-1")

    assert result["status"] == "STALE_SKIPPED"
    assert result["db_name"] == "td_s_beer"
    assert result["bundle_id"] == "bundle-1"
    assert result["http_status"] == 409


def test_merge_league_delta_fails_closed_on_stale_source_generation(fly_env, tmp_path):
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"delta")
    target = FlyTarget()
    detail = "Snapshot generation 2 for td_s_beer is stale; current generation is 3"

    with patch(
        "multi_league.core.targets.fly_target.requests.post",
        return_value=_resp(409, {"detail": detail}, text=detail),
    ), patch.object(target, "get_delta_merge_status", return_value={"status": "CONFLICT"}):
        with pytest.raises(RuntimeError, match="Snapshot generation 2"):
            target.merge_league_delta("td_s_beer", bundle, bundle_id="bundle-1", bundle_hash="hash-1")
