from __future__ import annotations

from types import SimpleNamespace


class _FakeCtx:
    playoff_start_week = None
    espn_s2 = None
    swid = None

    def get_league_id_for_year(self, year: int) -> int:
        assert year == 2024
        return 1784135

    def get_manager_name(self, team_id: int, team_name: str | None = None, year: int | None = None) -> str:
        return f"Manager {team_id}"

    def get_manager_guid(self, team_id: int, year: int | None = None) -> str:
        return f"guid_{team_id}"

    def get_franchise_id(self, team_id: int, year: int | None = None) -> str:
        return f"fid_{team_id}"


class _FakeLegacyCtx:
    playoff_start_week = 1
    espn_s2 = None
    swid = None

    def get_league_id_for_year(self, year: int) -> int:
        assert year == 2018
        return 159117

    def get_manager_name(self, team_id: int, team_name: str | None = None, year: int | None = None) -> str:
        return f"Manager {team_id}"

    def get_manager_guid(self, team_id: int, year: int | None = None) -> str:
        return f"guid_{team_id}"

    def get_franchise_id(self, team_id: int, year: int | None = None) -> str:
        return f"fid_{team_id}"


def test_fetch_espn_matchups_modern_preserves_commissioner_adjustment(monkeypatch):
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=3, team_name="Garnier Fructis")
    away_team = SimpleNamespace(team_id=9, team_name="Opponent")
    fake_box_score = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=176.5,
        away_score=120.0,
        is_playoff=False,
        matchup_type="NONE",
    )
    fake_league = SimpleNamespace(box_scores=lambda week: [fake_box_score] if week == 1 else [])

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            assert year == 2024
            return fake_league

        def get_raw_schedule(self, year: int, week: int):
            if year != 2024 or week != 1:
                return []
            return [
                {
                    "home": {"teamId": 3, "adjustment": 12.44, "tiebreak": 0.0},
                    "away": {"teamId": 9, "adjustment": 0.0, "tiebreak": 0.0},
                }
            ]

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {"end_week": 1, "playoff_matchup_period_length": 1},
    )

    df = espn_matchups.fetch_espn_matchups_modern(_FakeCtx(), 2024)

    assert df is not None
    home_row = df.loc[df["team_key"] == "3"].iloc[0]
    away_row = df.loc[df["team_key"] == "9"].iloc[0]

    assert home_row["team_points"] == 176.5
    assert home_row["adjustment"] == 12.44
    assert home_row["tiebreak"] == 0.0
    assert away_row["adjustment"] == 0.0
    assert away_row["tiebreak"] == 0.0


def test_fetch_espn_matchups_modern_reuses_prefetched_provider_payloads(monkeypatch):
    """Weekly refresh must not fetch the same box score or schedule twice."""
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=3, team_name="Home")
    away_team = SimpleNamespace(team_id=9, team_name="Away")
    box_score = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=101.5,
        away_score=99.0,
        is_playoff=False,
        matchup_type="NONE",
    )

    class _NoFetchLeague:
        _uses_league_history = False

        def box_scores(self, _week):
            raise AssertionError("prefetched box scores must be reused")

    class _NoFetchClient:
        def get_league(self, _year):
            raise AssertionError("the supplied league must be reused")

        def get_raw_schedule(self, _year, _week):
            raise AssertionError("prefetched raw schedules must be reused")

        def get_raw_team_playoff_seed_map(self, _year):
            return {}

    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {"end_week": 1, "playoff_matchup_period_length": 1},
    )

    frame = espn_matchups.fetch_espn_matchups_modern(
        _FakeCtx(),
        2024,
        weeks=[1],
        client=_NoFetchClient(),
        league=_NoFetchLeague(),
        box_scores_by_week={1: [box_score]},
        raw_schedules_by_week={
            1: [{
                "matchupPeriodId": 1,
                "winner": "HOME",
                "home": {"teamId": 3, "totalPoints": 101.5},
                "away": {"teamId": 9, "totalPoints": 99.0},
            }],
        },
    )

    assert frame is not None
    assert frame.loc[frame["team_key"] == "3", "team_points"].iloc[0] == 101.5
    assert frame.loc[frame["team_key"] == "9", "team_points"].iloc[0] == 99.0


def test_fetch_espn_matchups_modern_prefers_final_raw_totals_over_zero_box_totals(monkeypatch):
    """Active ESPN can lag BoxScore totals after its raw period is complete."""
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=3, team_name="Home")
    away_team = SimpleNamespace(team_id=9, team_name="Away")
    box_score = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=0,
        away_score=0,
        is_playoff=False,
        matchup_type="NONE",
    )
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {"end_week": 1, "playoff_matchup_period_length": 1},
    )

    frame = espn_matchups.fetch_espn_matchups_modern(
        _FakeCtx(),
        2024,
        weeks=[1],
        client=SimpleNamespace(get_raw_team_playoff_seed_map=lambda _year: {}),
        league=SimpleNamespace(_uses_league_history=False),
        box_scores_by_week={1: [box_score]},
        raw_schedules_by_week={
            1: [{
                "matchupPeriodId": 1,
                "winner": "HOME",
                "home": {
                    "teamId": 3,
                    "totalPoints": 0,
                    "pointsByScoringPeriod": {"1": 112.5},
                },
                "away": {
                    "teamId": 9,
                    "totalPoints": 0,
                    "pointsByScoringPeriod": {"1": 99.25},
                },
            }],
        },
    )

    assert frame is not None
    assert frame.loc[frame["team_key"] == "3", "team_points"].iloc[0] == 112.5
    assert frame.loc[frame["team_key"] == "9", "team_points"].iloc[0] == 99.25


def test_fetch_espn_matchups_modern_joins_string_library_ids_to_numeric_raw_ids(monkeypatch):
    """espn_api can expose team IDs as strings while raw JSON uses numbers."""
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id="3", team_name="Home")
    away_team = SimpleNamespace(team_id="9", team_name="Away")
    box_score = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=0,
        away_score=0,
        is_playoff=False,
        matchup_type="NONE",
    )
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {"end_week": 1, "playoff_matchup_period_length": 1},
    )

    frame = espn_matchups.fetch_espn_matchups_modern(
        _FakeCtx(),
        2024,
        weeks=[1],
        client=SimpleNamespace(get_raw_team_playoff_seed_map=lambda _year: {}),
        league=SimpleNamespace(_uses_league_history=False),
        box_scores_by_week={1: [box_score]},
        raw_schedules_by_week={
            1: [{
                "matchupPeriodId": 1,
                "winner": "HOME",
                "home": {"teamId": 3, "pointsByScoringPeriod": {"1": 112.5}},
                "away": {"teamId": 9, "pointsByScoringPeriod": {"1": 99.25}},
            }],
        },
    )

    assert frame is not None
    assert frame.loc[frame["team_key"] == "3", "team_points"].iloc[0] == 112.5
    assert frame.loc[frame["team_key"] == "9", "team_points"].iloc[0] == 99.25


def test_fetch_espn_matchups_legacy_uses_raw_winner_and_playoff_bonus_for_away_tie(monkeypatch):
    """Pre-2019 ESPN scoreboards can flatten a playoff bonus game as a tie.

    Regression for the_league_de1e 2018: raw ESPN schedule says the away side
    won, the league has a 3-point playoff home bonus, and the away side's
    period score did not already include that bonus. The legacy fetcher should
    preserve the API result and emit the adjusted winner score.
    """
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=7, team_name="KAMARAHAMEHA V3")
    away_team = SimpleNamespace(team_id=1, team_name="Cooking Up Another Kowski")
    fake_matchup = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=135.0,
        away_score=135.0,
        is_playoff=True,
        matchup_type="WINNERS_BRACKET",
    )
    fake_league = SimpleNamespace(scoreboard=lambda week: [fake_matchup] if week == 1 else [])

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            assert year == 2018
            return fake_league

        def get_raw_schedule(self, year: int, week: int):
            if year != 2018 or week != 1:
                return []
            return [
                {
                    "winner": "AWAY",
                    "playoffTierType": "WINNERS_BRACKET",
                    "home": {
                        "teamId": 7,
                        "totalPoints": 135.0,
                        "pointsByScoringPeriod": {"1": 132.0},
                        "adjustment": -3.0,
                        "tiebreak": 0.0,
                    },
                    "away": {
                        "teamId": 1,
                        "totalPoints": 135.0,
                        "pointsByScoringPeriod": {"1": 135.0},
                        "adjustment": 0.0,
                        "tiebreak": 0.0,
                    },
                }
            ]

        def get_raw_team_playoff_seed_map(self, year: int):
            return {7: 1, 1: 2}

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {
            "end_week": 1,
            "playoff_start_week": 1,
            "playoff_home_team_bonus": 3,
        },
    )

    df = espn_matchups.fetch_espn_matchups_legacy(_FakeLegacyCtx(), 2018)

    assert df is not None
    home_row = df.loc[df["team_key"] == "7"].iloc[0]
    away_row = df.loc[df["team_key"] == "1"].iloc[0]

    assert home_row["team_points"] == 135.0
    assert home_row["opponent_points"] == 138.0
    assert home_row["win"] == 0
    assert home_row["loss"] == 1
    assert home_row["tie"] == 0
    assert home_row["adjustment"] == -3.0

    assert away_row["team_points"] == 138.0
    assert away_row["opponent_points"] == 135.0
    assert away_row["margin"] == 3.0
    assert away_row["win"] == 1
    assert away_row["loss"] == 0
    assert away_row["tie"] == 0
    assert away_row["final_playoff_seed"] == 2


def test_legacy_away_winner_bonus_repair_does_not_double_apply_bonus():
    from multi_league.data_fetchers.espn.espn_matchups import _apply_legacy_away_winner_bonus_repair

    raw_meta = {
        "winner": "AWAY",
        "home_period_points": 132.0,
        "away_period_points": 135.0,
    }

    home_score, away_score = _apply_legacy_away_winner_bonus_repair(
        135.0,
        138.0,
        raw_meta,
        is_playoff=True,
        playoff_home_team_bonus=3.0,
    )

    assert home_score == 135.0
    assert away_score == 138.0


def test_fetch_espn_matchups_modern_preserves_raw_playoff_seed(monkeypatch):
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=3, team_name="Total Points Seed")
    away_team = SimpleNamespace(team_id=9, team_name="Record Seed")
    fake_box_score = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=176.5,
        away_score=120.0,
        is_playoff=True,
        matchup_type="WINNERS_BRACKET",
    )
    reg_box_score = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=100.0,
        away_score=90.0,
        is_playoff=False,
        matchup_type="NONE",
    )
    fake_league = SimpleNamespace(
        box_scores=lambda week: [reg_box_score] if 1 <= week <= 13 else [fake_box_score] if week == 14 else []
    )

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            assert year == 2024
            return fake_league

        def get_raw_schedule(self, year: int, week: int):
            return []

        def get_raw_team_playoff_seed_map(self, year: int):
            assert year == 2024
            return {3: 1, 9: 4}

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {
            "end_week": 14,
            "playoff_matchup_period_length": 1,
            "playoff_start_week": 14,
        },
    )

    df = espn_matchups.fetch_espn_matchups_modern(_FakeCtx(), 2024)

    assert df is not None
    assert df.loc[df["team_key"] == "3", "final_playoff_seed"].iloc[0] == 1
    assert df.loc[df["team_key"] == "9", "final_playoff_seed"].iloc[0] == 4


def test_fetch_espn_matchups_modern_skips_all_zero_score_week(monkeypatch):
    """When ESPN returns box_scores for a week with all home_score=0 and
    away_score=0 (e.g. preseason snapshot, abandoned-league renewal,
    pre-game-completion import), don't emit phantom matchup rows for that
    week. The previous guard exempted week 1 from the all-zero skip, which
    allowed a 10-row phantom snapshot to persist for `national_ca_az_tx_ffb_league`
    2024 + 2025 even though nobody played those years.

    The fetcher should bail after MAX_CONSECUTIVE_EMPTY weeks of all-zero
    scores, so a league with no completed games gets no matchup rows.
    """
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=1, team_name="Abe")
    away_team = SimpleNamespace(team_id=2, team_name="Ian")
    zero_box = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=0,
        away_score=0,
        is_playoff=False,
        matchup_type="NONE",
    )

    fake_league = SimpleNamespace(box_scores=lambda week: [zero_box] if 1 <= week <= 5 else [])

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            return fake_league

        def get_raw_schedule(self, year: int, week: int):
            return []

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {
            "end_week": 14,
            "playoff_matchup_period_length": 1,
            "playoff_start_week": 14,
        },
    )

    df = espn_matchups.fetch_espn_matchups_modern(_FakeCtx(), 2024)

    assert df is None, f"All-zero-score weeks must yield no matchup rows, got: {df}"


def test_fetch_espn_matchups_modern_rejects_stale_period_payload(monkeypatch):
    """Do not stamp one ESPN scoring period onto every requested week.

    ESPN can return the same current-period schedule when a historical or
    unplayed period is not available.  The roster fetcher already removes this
    stale repetition; matchup rows must enforce the same provider-period
    boundary or standings become 17-0/0-17.
    """
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=1, team_name="Abe")
    away_team = SimpleNamespace(team_id=2, team_name="Ian")
    box = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=110.0,
        away_score=95.0,
        is_playoff=False,
        matchup_type="NONE",
    )

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            return SimpleNamespace(box_scores=lambda _week: [box])

        def get_raw_schedule(self, year: int, week: int):
            # The provider ignored the requested period and returned period 1.
            return [{
                "matchupPeriodId": 1,
                "home": {"teamId": 1},
                "away": {"teamId": 2},
            }]

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {"end_week": 3, "playoff_matchup_period_length": 1},
    )

    df = espn_matchups.fetch_espn_matchups_modern(_FakeCtx(), 2024)

    assert df is not None
    assert sorted(df["week"].unique().tolist()) == [1]


def test_fetch_espn_matchups_modern_rejects_repeated_box_score_snapshot(monkeypatch):
    """Reject a repeated snapshot when ESPN omits period markers entirely."""
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=1, team_name="Abe")
    away_team = SimpleNamespace(team_id=2, team_name="Ian")
    box = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=110.0,
        away_score=95.0,
        is_playoff=False,
        matchup_type="NONE",
    )

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            return SimpleNamespace(box_scores=lambda _week: [box])

        def get_raw_schedule(self, year: int, week: int):
            return []

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {"end_week": 3, "playoff_matchup_period_length": 1},
    )

    df = espn_matchups.fetch_espn_matchups_modern(_FakeCtx(), 2024)

    assert df is not None
    assert sorted(df["week"].unique().tolist()) == [1]


def _build_fake_box_scores_by_week(playoff_w14: SimpleNamespace, playoff_w15: SimpleNamespace):
    """Return a callable that supplies regular-season filler boxes for weeks
    1-13 and the supplied playoff boxes for weeks 14-15. Filler is needed
    because the fetcher exits early after 3 consecutive empty weeks."""
    reg_home = SimpleNamespace(team_id=2, team_name="RegHome")
    reg_away = SimpleNamespace(team_id=3, team_name="RegAway")
    reg_box = SimpleNamespace(
        home_team=reg_home,
        away_team=reg_away,
        home_score=110.0,
        away_score=95.0,
        is_playoff=False,
        matchup_type="NONE",
    )
    by_week = {w: [reg_box] for w in range(1, 14)}
    by_week[14] = [playoff_w14]
    by_week[15] = [playoff_w15]
    return lambda week: by_week.get(week, [])


def test_fetch_espn_matchups_modern_skips_zero_delta_2week_continuation(monkeypatch):
    """When ESPN reports has_multiweek_championship=True but the league only
    actually plays the first week of each round, the continuation week's
    cumulative scores match week 1 exactly (delta = 0). Don't write phantom
    0-score rows for those weeks — they poison bracket_tracer, power_rating,
    and luck calculations downstream.

    Regression for my_2025_league 2025: weeks 15 and 17 had all-zero deltas
    while ESPN's settings claimed playoff_matchup_period_length=2.
    """
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=1, team_name="Gabe")
    away_team = SimpleNamespace(team_id=4, team_name="Raanan")

    # Week 14: real semifinal scores
    box_w14 = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=305.62,
        away_score=293.68,
        is_playoff=True,
        matchup_type="WINNERS_BRACKET",
    )
    # Week 15: ESPN reports cumulative SAME as week 14 → per-week delta is (0, 0).
    box_w15 = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=305.62,
        away_score=293.68,
        is_playoff=True,
        matchup_type="WINNERS_BRACKET",
    )

    fake_league = SimpleNamespace(box_scores=_build_fake_box_scores_by_week(box_w14, box_w15))

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            return fake_league

        def get_raw_schedule(self, year: int, week: int):
            return []

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {
            "end_week": 15,
            "playoff_matchup_period_length": 2,
            "playoff_start_week": 14,
        },
    )

    df = espn_matchups.fetch_espn_matchups_modern(_FakeCtx(), 2024)

    assert df is not None
    playoff_weeks = sorted(df.loc[df["week"] >= 14, "week"].unique().tolist())
    assert playoff_weeks == [14], f"Expected only week 14 in playoffs, got {playoff_weeks}"
    w14_rows = df.loc[df["week"] == 14]
    assert len(w14_rows) == 2, f"Expected 2 rows (home + away) for week 14, got {len(w14_rows)}"


def test_fetch_espn_matchups_modern_keeps_real_2week_continuation_deltas(monkeypatch):
    """A genuine 2-week aggregate playoff with non-zero deltas in week 2 should
    still produce per-week rows (zero-delta skip must not eat real data)."""
    from multi_league.data_fetchers.espn import espn_matchups

    home_team = SimpleNamespace(team_id=1, team_name="Gabe")
    away_team = SimpleNamespace(team_id=4, team_name="Raanan")

    box_w14 = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=120.0,
        away_score=100.0,
        is_playoff=True,
        matchup_type="WINNERS_BRACKET",
    )
    # Week 15 cumulative = (250, 180) → per-week delta = (130, 80)
    box_w15 = SimpleNamespace(
        home_team=home_team,
        away_team=away_team,
        home_score=250.0,
        away_score=180.0,
        is_playoff=True,
        matchup_type="WINNERS_BRACKET",
    )

    fake_league = SimpleNamespace(box_scores=_build_fake_box_scores_by_week(box_w14, box_w15))

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year: int):
            return fake_league

        def get_raw_schedule(self, year: int, week: int):
            return []

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)
    monkeypatch.setattr(
        "multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings",
        lambda _ctx, _year: {
            "end_week": 15,
            "playoff_matchup_period_length": 2,
            "playoff_start_week": 14,
        },
    )

    df = espn_matchups.fetch_espn_matchups_modern(_FakeCtx(), 2024)

    assert df is not None
    playoff_weeks = sorted(df.loc[df["week"] >= 14, "week"].unique().tolist())
    assert playoff_weeks == [14, 15], f"Expected weeks [14, 15] in playoffs, got {playoff_weeks}"
    w15_home = df.loc[(df["week"] == 15) & (df["team_key"] == "1")].iloc[0]
    w15_away = df.loc[(df["week"] == 15) & (df["team_key"] == "4")].iloc[0]
    assert w15_home["team_points"] == 130.0
    assert w15_away["team_points"] == 80.0
