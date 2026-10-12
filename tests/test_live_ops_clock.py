from datetime import datetime, timezone
from pathlib import Path

from scripts.live_ops_clock import Clock, instant, latest_due, next_due, payload_for


def test_next_three_am_crosses_spring_dst():
    due = next_due(instant('2026-03-07T09:00:00Z'))
    assert due.astimezone(timezone.utc) == instant('2026-03-08T07:00:00Z')


def test_next_three_am_crosses_fall_dst():
    due = next_due(instant('2026-10-31T08:00:00Z'))
    assert due.astimezone(timezone.utc) == instant('2026-11-01T08:00:00Z')


def test_catchup_keeps_original_game_date():
    due = latest_due(instant('2026-10-12T19:00:00Z'))
    payload = payload_for(due)
    assert payload['client_payload']['game_date'] == '2026-10-11'
    assert payload['client_payload']['scheduled_for'] == '2026-10-12T03:00:00-04:00'


class DispatchAPI:
    def __init__(self):
        self.calls = []
        self.accepted_keys = set()
        self.lose_response = False

    def request(self, method, suffix, payload):
        self.calls.append(payload)
        self.accepted_keys.add(payload['client_payload']['dispatch_key'])
        if self.lose_response:
            raise TimeoutError('response lost after accepting request')

    def accepted(self, key, due):
        return key in self.accepted_keys


def test_successful_dispatch_is_not_repeated_after_restart(tmp_path: Path):
    api = DispatchAPI()
    due = latest_due(instant('2026-10-12T19:00:00Z'))
    state = tmp_path / 'clock.json'
    assert Clock(state, api).dispatch(due) is True
    assert Clock(state, api).dispatch(due) is False
    assert len(api.calls) == 1


def test_lost_dispatch_response_is_recovered_without_resending(tmp_path: Path):
    import pytest
    api = DispatchAPI()
    api.lose_response = True
    state = tmp_path / 'clock.json'
    due = latest_due(instant('2026-10-12T19:00:00Z'))
    with pytest.raises(TimeoutError):
        Clock(state, api).dispatch(due)
    assert Clock(state, api).dispatch(due) is False
    assert len(api.calls) == 1


def test_next_day_dispatches_once(tmp_path: Path):
    api = DispatchAPI()
    clock = Clock(tmp_path / 'clock.json', api)
    assert clock.dispatch(latest_due(instant('2026-10-12T19:00:00Z'))) is True
    assert clock.dispatch(latest_due(instant('2026-10-13T19:00:00Z'))) is True
    assert len(api.calls) == 2


def test_probe_event_cannot_trigger_data_worker():
    payload = payload_for(instant('2026-10-12T19:00:00Z'), probe=True)
    assert payload['event_type'] == 'live-nfl-ops-clock-probe'


def test_pending_dispatch_survives_the_next_three_am(tmp_path):
    import pytest
    api = DispatchAPI()
    api.lose_response = True
    due = latest_due(instant('2026-10-12T19:00:00Z'))
    state = tmp_path / 'clock.json'
    with pytest.raises(TimeoutError):
        Clock(state, api).dispatch(due)
    assert Clock(state, api).pending_due(instant('2026-10-13T12:00:00Z')) == due


def test_probe_does_not_erase_production_acknowledgment(tmp_path):
    api = DispatchAPI()
    state = tmp_path / 'clock.json'
    due = latest_due(instant('2026-10-12T19:00:00Z'))
    clock = Clock(state, api)
    clock.dispatch(due)
    clock.dispatch(due, probe=True)
    assert Clock(state, api).dispatch(due) is False
    assert len(api.calls) == 2


def test_clock_catches_up_each_missed_day(tmp_path):
    api = DispatchAPI()
    clock = Clock(tmp_path / 'clock.json', api)
    clock.dispatch(latest_due(instant('2026-10-12T19:00:00Z')))
    now = instant('2026-10-15T12:00:00Z')
    assert clock.pending_due(now).date().isoformat() == '2026-10-13'
    clock.dispatch(clock.pending_due(now))
    assert clock.pending_due(now).date().isoformat() == '2026-10-14'


def test_acknowledgment_search_includes_later_pages():
    from scripts.live_ops_clock import GitHub

    class Pages(GitHub):
        def request(self, method, suffix, payload=None):
            if suffix.endswith('page=1'):
                return {'workflow_runs': [{'display_title': 'unrelated'}] * 100}
            return {'workflow_runs': [{'display_title': 'Refresh Live NFL Ops ops-2026-10-12'}]}

    assert Pages('unused').accepted('ops-2026-10-12', instant('2026-10-12T07:00:00Z')) is True
