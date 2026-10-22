"""Exercise exact candidate fetchers against requests/rauth and real Yahoo refresh."""
import json
import time
from email.utils import formatdate
from types import SimpleNamespace

import pandas as pd
import pytest
from requests import Response
from requests.adapters import BaseAdapter
from rauth import OAuth2Service
from yahoo_oauth import OAuth2

from multi_league.data_fetchers.yahoo import yahoo_draft as draft
from multi_league.data_fetchers.yahoo import yahoo_transactions as transactions
from multi_league.data_fetchers.yahoo import yahoo_rosters as rosters
from multi_league.data_fetchers.yahoo import yahoo_recovery as recovery
from multi_league.data_fetchers.yahoo.yahoo_recovery_api import RecoveryDeferredError

URL = 'https://fantasysports.yahooapis.com/fantasy/v2/league/79.l.1/teams'
TEAMS = ('<fantasy_content><league><teams><team><team_key>79.l.1.t.1</team_key>'
         '<name>One</name><managers><manager><guid>owner</guid><nickname>One</nickname>'
         '</manager></managers></team></teams></league></fantasy_content>')


class YahooHTTP(BaseAdapter):
    """The old bearer is denied; only a refreshed bearer can read actual data."""
    def __init__(self, *, denial=200, permanent=False, responses=None):
        self.denial = denial
        self.permanent = permanent
        self.responses = list(responses or [])
        self.calls = []
        self.refreshes = 0

    def send(self, request, **kwargs):
        self.calls.append(request.url)
        response = Response()
        response.request = request
        response.url = request.url
        response.status_code = 200
        if request.url == 'https://auth.test/token':
            self.refreshes += 1
            body = json.dumps({'access_token': 'fresh-test-token', 'refresh_token': 'test-refresh', 'token_type': 'bearer'})
        elif self.responses:
            response.status_code, body, headers = self.responses.pop(0)
            response.headers.update(headers)
        elif self.permanent:
            response.status_code, body = 403, '<error>Permission denied: user is not in this league</error>'
        elif request.headers.get('Authorization') != 'Bearer fresh-test-token':
            response.status_code, body = self.denial, 'Request denied\r\n'
        else:
            body = TEAMS
        response._content = body.encode()
        response.encoding = 'utf-8'
        return response

    def close(self):
        pass


def oauth_for(http, *, expired=False, fresh=False):
    class Service(OAuth2Service):
        def get_session(self, token=None):
            session = super().get_session(token)
            session.mount('https://', http)
            return session
    # Avoid interactive constructor/file IO; refresh_access_token and rauth's
    # bearer application are real, including the library's stale-session bug.
    oauth = OAuth2.__new__(OAuth2)
    oauth.consumer_key = 'test-client'
    oauth.consumer_secret = 'test-secret'
    oauth.refresh_token = 'test-refresh'
    oauth.callback_uri = 'test-callback'
    oauth.token_time = time.time() - (4000 if expired else 0)
    oauth.access_token = 'fresh-test-token' if fresh else 'old-test-token'
    oauth.oauth = Service(name='yahoo-test', client_id='test-client', client_secret='test-secret',
                          access_token_url='https://auth.test/token')
    oauth.session = oauth.oauth.get_session(oauth.access_token)
    return oauth


@pytest.fixture(autouse=True)
def no_wait(monkeypatch):
    waits = []
    clock = [time.time()]
    def sleep(seconds):
        waits.append(seconds)
        clock[0] += seconds
    monkeypatch.setattr(time, 'sleep', sleep)
    monkeypatch.setattr(time, 'time', lambda: clock[0])
    return waits


def fetch(kind, oauth, tmp_path):
    if kind == 'roster':
        fetcher = rosters.YahooRosterFetcher(None, '79.l.1', rate_limit=0,
                                             oauth_session=oauth, output_dir=tmp_path)
        return fetcher._fetch_url_xml(URL)
    return (draft if kind == 'draft' else transactions).fetch_url(URL, oauth)


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
def test_expired_bearer_is_repaired_before_reading_history(kind, tmp_path):
    http = YahooHTTP()
    result = fetch(kind, oauth_for(http, expired=True), tmp_path)
    assert result.findtext('.//team_key') == '79.l.1.t.1'
    assert http.refreshes == 1
    assert http.calls.count(URL) == 1


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
def test_external_refresh_does_not_leave_requests_using_old_bearer(kind, tmp_path):
    http = YahooHTTP()
    oauth = oauth_for(http)
    oauth.refresh_access_token()  # Same operation as ensure_session_healthy.
    result = fetch(kind, oauth, tmp_path)
    assert result.findtext('.//team_key') == '79.l.1.t.1'
    assert http.refreshes == 1
    assert http.calls.count(URL) == 1


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
@pytest.mark.parametrize('status', [200, 401])
def test_denied_bearer_gets_one_effective_refresh_then_returns_data(kind, status, tmp_path):
    http = YahooHTTP(denial=status)
    result = fetch(kind, oauth_for(http), tmp_path)
    assert result.findtext('.//team_key') == '79.l.1.t.1'
    assert http.refreshes == 1
    assert http.calls.count(URL) == 2


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
def test_permanent_permission_denial_does_not_refresh_or_repeat(kind, tmp_path):
    http = YahooHTTP(permanent=True)
    with pytest.raises(Exception, match='denied|403|permission'):
        fetch(kind, oauth_for(http, fresh=True), tmp_path)
    assert http.refreshes == 0
    assert http.calls == [URL]


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
@pytest.mark.parametrize('header', ['3', 'date'])
def test_throttle_honors_retry_after_without_refresh_or_stacked_backoff(kind, header, tmp_path, no_wait):
    now = time.time()
    retry_after = '3' if header == '3' else formatdate(now + 3, usegmt=True)
    http = YahooHTTP(responses=[(429, 'Too many requests', {'Retry-After': retry_after}), (200, TEAMS, {})])
    result = fetch(kind, oauth_for(http, fresh=True), tmp_path)
    assert result.findtext('.//team_key') == '79.l.1.t.1'
    assert http.refreshes == 0
    assert http.calls == [URL, URL]
    assert len(no_wait) == 1 and 1.9 < no_wait[0] <= 3


def test_recovery_uses_injected_session_without_constructing_another(tmp_path, monkeypatch):
    oauth = oauth_for(YahooHTTP(), fresh=True)
    ctx = SimpleNamespace(league_ids={'2003': '79.l.1'})
    monkeypatch.setattr(rosters.YahooRosterFetcher, '_initialize_oauth',
                        lambda *a: pytest.fail('Recovery must use its supplied live session'))
    fetcher = recovery._get_recovery_roster_fetcher(ctx, 2003, {}, oauth)
    assert fetcher is not None and fetcher.oauth is oauth


@pytest.mark.parametrize('discover_teams', [False, True])
def test_transient_completed_week_is_never_hard_missing(tmp_path, monkeypatch, discover_teams):
    http = YahooHTTP(responses=[(429, 'Too many requests', {})] * 10)
    oauth = oauth_for(http, fresh=True)
    ctx = SimpleNamespace(league_ids={'2003': '79.l.1'})
    fetcher = rosters.YahooRosterFetcher(None, '79.l.1', rate_limit=0,
                                        oauth_session=oauth, output_dir=tmp_path)
    teams = {2003: {'79.l.1.t.1': {'manager_name': 'One', 'manager_guid': 'owner'}}}
    if discover_teams:
        teams = {}
    gap = recovery.Gap('roster', 2003, 'wk17', 'manifest')
    monkeypatch.setattr(recovery, '_get_recovery_roster_fetcher', lambda *a, **k: fetcher)
    with pytest.raises(RecoveryDeferredError):
        recovery._resolve_roster_gap(gap, ctx, tmp_path, oauth, {}, teams, local_db=None)
    assert gap.hard_missing is False


def test_successful_team_snapshot_is_cached_by_full_league_key(tmp_path):
    http = YahooHTTP()
    oauth = oauth_for(http, fresh=True)
    first = transactions.fetch_team_mappings(oauth, '79.l.1')
    second = transactions.fetch_team_mappings(oauth, '79.l.1', {'One': 'Alias'})
    assert first[0] == {'79.l.1.t.1': 'One'}
    assert second[0] == {'79.l.1.t.1': 'Alias'}
    assert http.calls == [URL]


def test_failed_team_snapshot_is_not_cached(tmp_path):
    http = YahooHTTP(permanent=True)
    oauth = oauth_for(http, fresh=True)
    with pytest.raises(RuntimeError):
        transactions.fetch_team_mappings(oauth, '79.l.1')
    http.permanent = False
    assert transactions.fetch_team_mappings(oauth, '79.l.1')[0] == {'79.l.1.t.1': 'One'}


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
@pytest.mark.parametrize('remaining,header', [(None, '3600'), (2, '3')])
def test_retry_after_outside_remaining_budget_defers_without_sleep(kind, remaining, header, tmp_path, no_wait):
    http = YahooHTTP(responses=[(429, 'Too many requests', {'Retry-After': header})] * 6)
    oauth = oauth_for(http, fresh=True)
    if remaining is not None:
        oauth._yahoo_recovery_deadline = time.time() + remaining
    with pytest.raises(Exception, match='rate limit'):
        fetch(kind, oauth, tmp_path)
    assert no_wait == []
    assert http.calls == [URL] and http.refreshes == 0


class ReadOnlyOAuth:
    def __init__(self, oauth):
        object.__setattr__(self, '_wrapped', oauth)

    def __getattr__(self, name):
        return getattr(self._wrapped, name)

    def __setattr__(self, name, value):
        raise AttributeError('read-only OAuth wrapper')


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
def test_oauth_without_optional_setattr_support_still_returns_all_data(kind, tmp_path):
    http = YahooHTTP()
    result = fetch(kind, ReadOnlyOAuth(oauth_for(http, fresh=True)), tmp_path)
    assert result.findtext('.//team_key') == '79.l.1.t.1'
    assert http.calls == [URL]


@pytest.mark.parametrize('kind', ['draft', 'transaction', 'roster'])
@pytest.mark.parametrize('name', ['Too Many Requests', 'Access Denied', 'Permission denied', 'Unauthorized'])
def test_legitimate_xml_team_names_are_not_transport_errors(kind, name, tmp_path):
    http = YahooHTTP(responses=[(200, TEAMS.replace('<name>One</name>', f'<name>{name}</name>'), {})])
    result = fetch(kind, oauth_for(http, fresh=True), tmp_path)
    assert result.findtext('.//team/name') == name
    assert http.calls == [URL] and http.refreshes == 0


@pytest.mark.parametrize('body', [
    '<error><description>Forbidden access</description></error>',
    '<fantasy_content><error><description>Unexpected provider error</description></error></fantasy_content>',
])
def test_error_envelope_never_becomes_successful_empty_transactions(body):
    http = YahooHTTP(responses=[(200, body, {})] * 6)
    oauth = oauth_for(http, fresh=True)
    with pytest.raises((transactions.RecoverableAPIError, transactions.APITimeoutError)):
        transactions.fetch_transactions_for_year(oauth, '79.l.1', 2003, {}, {}, {}, pd.DataFrame())


def test_concurrent_denials_of_same_bearer_refresh_only_once(tmp_path):
    import threading
    from concurrent.futures import ThreadPoolExecutor

    barrier = threading.Barrier(2)
    class SimultaneousDenials(YahooHTTP):
        def send(self, request, **kwargs):
            if request.url == URL and request.headers.get('Authorization') == 'Bearer old-test-token':
                barrier.wait(timeout=3)
            return super().send(request, **kwargs)
    http = SimultaneousDenials(denial=401)
    oauth = oauth_for(http)
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: fetch('draft', oauth, tmp_path), range(2)))
    assert all(root.findtext('.//team_key') == '79.l.1.t.1' for root in results)
    assert http.refreshes == 1
    assert http.calls.count(URL) == 4
