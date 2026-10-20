"""Future fixtures are simulation inputs, not missing historical results."""
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from multi_league.core.local_db import LocalLeagueDB
from multi_league.data_fetchers.yahoo import yahoo_recovery as recovery


@pytest.fixture(autouse=True)
def audit_clock(monkeypatch):
    from multi_league.core import date_utils
    class Clock(datetime):
        @classmethod
        def now(cls,tz=None):
            return datetime(2026,10,20,12,tzinfo=UTC)
    monkeypatch.setattr(date_utils,'datetime',Clock)
    monkeypatch.setattr(date_utils,'_get_nfl_state_from_api',lambda: pytest.fail('Audit must use fetched state, not request NFL state'))


def seed(db, *, year=2026, played=range(1, 7), scheduled=range(1, 18), end=17):
    for table in ('league_settings', 'matchup', 'schedule', 'player_fantasy', 'transactions', 'draft'):
        db.ensure_table(table)
    conn = db.connect()
    conn.execute('INSERT INTO public.league_settings '
        '(db_name,year,num_teams,start_week,end_week,playoff_start_week) VALUES (?, ?, 2, 1, ?, 15)',
        [db.league_name, year, end])
    for week in played:
        for manager, opponent in (('A', 'B'), ('B', 'A')):
            identity = f'{manager}_{year}_{week}'
            conn.execute('INSERT INTO public.matchup '
                '(db_name,year,week,manager,opponent,manager_guid,franchise_id,manager_week,team_key,team_points,opponent_points) '
                'VALUES (?,?,?,?,?,?,?,?,?,100,90)',
                [db.league_name,year,week,manager,opponent,manager,manager,identity,manager])
            conn.execute('INSERT INTO public.player_fantasy '
                '(db_name,year,week,manager,manager_guid,franchise_id,manager_week,yahoo_player_id) '
                'VALUES (?,?,?,?,?,?,?,?)', [db.league_name,year,week,manager,manager,manager,identity,manager])
    for week in scheduled:
        for manager, opponent in (('A', 'B'), ('B', 'A')):
            conn.execute('INSERT INTO public.schedule '
                '(db_name,year,week,manager,opponent,franchise_id,opponent_franchise_id,manager_week) '
                'VALUES (?,?,?,?,?,?,?,?)',
                [db.league_name,year,week,manager,opponent,manager,opponent,f'{manager}_{year}_{week}'])


def settings(*, current=7, finished=False, end=17):
    return {2026: {'metadata': {'current_week': current, 'is_finished': str(int(finished)),
        'end_week':end, 'end_date':'2027-01-04'}}}


def test_completed_results_plus_future_fixtures_do_not_trigger_recovery(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db)
        assert recovery._detect_gaps_from_local_db(db, settings()) == []
        assert db.connect().execute('SELECT MAX(week),COUNT(*) FROM public.schedule').fetchone() == (17,34)


@pytest.mark.parametrize('played,metadata', [([],settings(current=1)),
    (range(1,17),settings(current=17)), (range(1,7),{2026:{}})])
def test_preseason_live_final_and_unknown_provider_state_do_not_require_future_results(tmp_path, played, metadata):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db,played=played)
        assert recovery._detect_gaps_from_local_db(db, metadata) == []


@pytest.mark.parametrize('missing_week', [3,6])
def test_missing_completed_week_still_requires_recovery(tmp_path, missing_week):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db,played=[week for week in range(1,7) if week != missing_week])
        gaps = recovery._detect_gaps_from_local_db(db,settings())
        assert ('matchup',2026,'full_year') in {gap.key for gap in gaps}


def test_finished_league_still_requires_its_last_playoff_result(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db,played=range(1,17))
        assert ('matchup',2026,'full_year') in {
            gap.key for gap in recovery._detect_gaps_from_local_db(db,settings(current=17,finished=True))}


def test_empty_transaction_season_is_not_a_fetch_failure(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db,year=2025,played=range(1,18))
        seed(db)
        db.connect().execute("INSERT INTO public.transactions "
            "(db_name,year,week,transaction_id,manager,transaction_type) VALUES ('qa',2026,1,'real','A','add')")
        gaps = recovery._detect_gaps_from_local_db(db,{2025:{},**settings()})
        assert not [gap for gap in gaps if gap.fetcher=='transaction']


def test_future_matchup_and_roster_manifests_do_not_enter_recovery_queue(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db)
        recovery.write_manifest(tmp_path,'matchups',2026,failed_weeks=[7,14])
        recovery.write_manifest(tmp_path,'rosters',2026,failed_weeks=[7,14])
        _, _, queue = recovery._collect_retryable_gaps(tmp_path,db,settings())
        assert queue == []


def test_missing_completed_week_manifest_is_not_discarded(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db,played=range(1,6))
        recovery.write_manifest(tmp_path,'matchups',2026,failed_weeks=[6,7,14])
        _, _, queue = recovery._collect_retryable_gaps(tmp_path,db,settings())
        assert {gap.key for gap in queue} == {('matchup',2026,'full_year')}


def test_future_manifests_finish_without_recovery_or_hard_missing(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db)
        recovery.write_manifest(tmp_path,'matchups',2026,failed_weeks=[7,14])
        recovery.write_manifest(tmp_path,'rosters',2026,failed_weeks=[7,14])
        result = recovery.run_yahoo_recovery(SimpleNamespace(data_directory=tmp_path),settings(),local_db=db)
        assert result.success and result.rounds == 0
        assert result.unresolved == result.hard_missing == result.resolved == []
        assert not (tmp_path/recovery.MANIFEST_DIR_NAME).exists()


def test_real_transaction_failure_is_not_discarded_as_future_result(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db)
        recovery.write_manifest(tmp_path,'transactions',2026,failed_weeks=[7])
        _, _, queue = recovery._collect_retryable_gaps(tmp_path,db,settings())
        assert {gap.key for gap in queue} == {('transaction',2026,'full_year')}


def test_unknown_state_preserves_explicit_failed_later_week(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db,played=range(1,6))
        recovery.write_manifest(tmp_path,'matchups',2026,failed_weeks=[6])
        recovery.write_manifest(tmp_path,'rosters',2026,failed_weeks=[6])
        _, _, queue = recovery._collect_retryable_gaps(tmp_path,db,{2026:{}})
        assert {gap.key for gap in queue} == {('matchup',2026,'full_year'),('roster',2026,'wk6')}


def test_full_year_roster_failure_does_not_expand_into_live_final_week(tmp_path):
    with LocalLeagueDB(tmp_path,'qa') as db:
        seed(db,played=range(1,18))
        recovery.write_manifest(tmp_path,'rosters',2026,failed_year=True)
        _, _, queue = recovery._collect_retryable_gaps(tmp_path,db,settings(current=17))
        assert {gap.key for gap in queue} == {('roster',2026,f'wk{week}') for week in range(1,17)}


@pytest.mark.parametrize('year,clock,metadata,observed,want', [
    (2025,'2026-10-20',{},6,17),
    (2027,'2026-10-20',{},0,0),
    (2026,'2026-08-20',{'current_week':1},0,0),
    (2026,'2026-10-20',{'current_week':7},5,6),
    (2026,'2026-10-20',{'current_week':1},0,0),
    (2026,'2026-10-20',{'current_week':0},0,0),
    (2026,'2027-01-02',{'current_week':17,'is_finished':'0'},16,16),
    (2026,'2027-01-05',{'current_week':17,'is_finished':'1'},16,17),
    (2026,'2027-01-05',{'current_week':17,'end_date':'2027-01-04'},16,17),
    (2026,'2026-10-20',{'current_week':None},6,6),
    (2026,'2026-10-20',{'current_week':'unknown'},6,6),
])
def test_completed_boundary_uses_fetched_state_without_guessing_future_results(year,clock,metadata,observed,want):
    from multi_league.core.date_utils import completed_matchup_week
    assert completed_matchup_week(year=year,end_week=17,metadata=metadata,observed_week=observed,
        reference_date=datetime.fromisoformat(clock).replace(tzinfo=UTC)) == want
