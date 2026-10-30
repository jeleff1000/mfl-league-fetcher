"""Publication control checks use their reserve, without widening data reads."""
from unittest.mock import Mock
import pytest
import requests
from multi_league.core.readers.fly_reader import FlyReader, FlyReaderNetworkError
from multi_league.core.league_update_publish_claim import renew_claim_for_publication

@pytest.fixture
def control_env(monkeypatch):
    monkeypatch.setenv('DATABASE_SERVER_URL', 'https://fly.test')
    monkeypatch.setenv('DATABASE_READ_TOKEN', 'read-token')
    monkeypatch.setenv('LEAGUE_UPDATE_DEADLINE_EPOCH', '1120')
    monkeypatch.setenv('LEAGUE_UPDATE_READ_RECOVERY_SECONDS', '60')
    monkeypatch.setenv('LEAGUE_UPDATE_REQUIRE_CLAIM', '1')
    monkeypatch.setenv('LEAGUE_UPDATE_TOKEN', 'owned-token')
    monkeypatch.setenv('LEAGUE_UPDATE_ATTEMPT_ID', 'owned-attempt')
    monkeypatch.setenv('LEAGUE_UPDATE_CLAIM_VERSION', '4')
    clock = [1091.0]
    monkeypatch.setattr('multi_league.core.readers.fly_reader.time.time', lambda: clock[0])
    monkeypatch.setattr('multi_league.core.readers.fly_reader.time.sleep', lambda seconds: clock.__setitem__(0, clock[0] + seconds))
    response = Mock(status_code=200, text='', headers={})
    response.json.return_value = [{'eligible': 1}]
    post = Mock(return_value=response)
    monkeypatch.setattr('multi_league.core.readers.fly_reader.requests.post', post)
    record = Mock(return_value=True)
    monkeypatch.setattr('multi_league.core.league_update_publish_claim.record_league_update_status', record)
    return clock, response, post, record

@pytest.mark.parametrize('platform', ['yahoo', 'espn', 'sleeper'])
def test_late_claim_uses_publication_reserve_not_data_budget(control_env, platform):
    clock, response, post, record = control_env
    reader = FlyReader()
    with pytest.raises(FlyReaderNetworkError, match='budget exhausted'):
        reader.query_scalar('SELECT 1', database='___ops')
    post.assert_not_called()
    assert renew_claim_for_publication(reader, database_name='wahoo', platform=platform, writer=object())
    assert record.call_args.kwargs['platform'] == platform
    assert record.call_args.kwargs['dispatch_token'] == 'owned-token'
    assert record.call_args.kwargs['claim_version'] == 4
    assert 0 < post.call_args.kwargs['timeout'] <= 10
    assert reader.REFRESH_FINALIZE_SECONDS == 30
    with pytest.raises(FlyReaderNetworkError, match='budget exhausted'):
        reader.query_scalar('SELECT 1', database='___ops')
    assert post.call_count == 1

@pytest.mark.parametrize('platform', ['yahoo', 'espn', 'sleeper'])
@pytest.mark.parametrize('eligible,owned', [(0, True), (1, False)])
def test_late_claim_still_rejects_unpaid_or_stale_writer(control_env, platform, eligible, owned):
    clock, response, post, record = control_env
    response.json.return_value = [{'eligible': eligible}]
    record.return_value = owned
    with pytest.raises(PermissionError if not eligible else RuntimeError, match='not entitled' if not eligible else 'no longer owns'):
        renew_claim_for_publication(FlyReader(), database_name='wahoo', platform=platform, writer=object())
    assert post.call_count == 1
    assert record.call_count == int(bool(eligible))

@pytest.mark.parametrize('now', [1105.0, 1120.0, 1121.0])
def test_control_read_cannot_consume_merge_reserve_or_exceed_hard_deadline(control_env, now):
    clock, response, post, record = control_env
    clock[0] = now
    with pytest.raises(FlyReaderNetworkError, match='budget exhausted'):
        renew_claim_for_publication(FlyReader(), database_name='wahoo', platform='yahoo', writer=object())
    post.assert_not_called()
    record.assert_not_called()

def test_control_read_recovers_transient_timeout_inside_same_deadline(control_env):
    clock, response, post, record = control_env
    clock[0] = 1099.0
    post.side_effect = [requests.exceptions.ReadTimeout('temporary'), response]
    assert renew_claim_for_publication(FlyReader(), database_name='wahoo', platform='yahoo', writer=object())
    assert post.call_count == 2
    assert [call.kwargs['timeout'] for call in post.call_args_list] == [6, 5]
    assert clock[0] == 1100.0
